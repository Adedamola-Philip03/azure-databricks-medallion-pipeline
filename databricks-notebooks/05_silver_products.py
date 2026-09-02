# Databricks notebook source
# ---------------------------------------------------------
# Silver Layer — Products
# Uses shared configuration from 00_config for environment-aware
# catalog/schema names. Incremental processing via bookmark table.
# ---------------------------------------------------------

# COMMAND ----------

%run /Workspace/Users/adedeji2503@gmail.com/00_config

# COMMAND ----------

import logging
from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, when, lit, trim, initcap, lower, row_number, desc,
    count, countDistinct, expr
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("silver_products")

# COMMAND ----------

def get_last_processed_timestamp(table_name: str) -> str:
    """Reads the bookmark for a given Silver table."""
    result = spark.sql(f"""
        SELECT MAX(last_processed_ingested_at) AS last_ts
        FROM {CATALOG}.{SCHEMA}.processing_log
        WHERE table_name = '{table_name}'
    """).collect()[0]["last_ts"]
    return result if result else "1900-01-01T00:00:00"

# COMMAND ----------

def update_processing_log(table_name: str, latest_ingested_at: str) -> None:
    """Records the newest ingested_at actually processed in this run."""
    processed_at = datetime.now(timezone.utc).isoformat()
    spark.sql(f"""
        INSERT INTO {CATALOG}.{SCHEMA}.processing_log
        VALUES ('{table_name}', '{latest_ingested_at}', '{processed_at}')
    """)

# COMMAND ----------

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_products"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_products"
REJECTED_TABLE = f"{CATALOG}.{SCHEMA}.rejected_products"
BUSINESS_KEY = "ProductID"

MIN_VALID_PRICE = 0.0

# COMMAND ----------

def profile_bronze_table(df: DataFrame, name: str) -> None:
    """Inspects a Bronze table for data quality signals before any
    cleaning/validation logic is written against it."""
    logger.info(f"Profiling {name}: {df.count()} rows")

    null_exprs = [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
    logger.info("True null counts per column:")
    df.select(null_exprs).show(truncate=False)

    logger.info("Price column raw distinct values (checking for non-numeric contamination):")
    df.select("Price").distinct().show(truncate=False)

# COMMAND ----------

def deduplicate_latest(df: DataFrame, business_key: str, order_col: str = "ingested_at") -> DataFrame:
    """Keep only the most recently ingested record per business key."""
    window_spec = Window.partitionBy(business_key).orderBy(desc(order_col))
    return (
        df.withColumn("_rn", row_number().over(window_spec))
          .filter(col("_rn") == 1)
          .drop("_rn")
    )

# COMMAND ----------

def standardize_product_fields(df: DataFrame) -> DataFrame:
    """Trim whitespace, normalize casing, and cast Price to a real
    numeric type via try_cast."""
    return (
        df.withColumn("ProductName", initcap(trim(col("ProductName"))))
          .withColumn("Category", initcap(trim(col("Category"))))
          .withColumn("Price", expr("try_cast(Price AS DOUBLE)"))
    )

# COMMAND ----------

def validate_products(df: DataFrame) -> DataFrame:
    """Price is a hard rejection rule."""
    return (
        df.withColumn("is_price_valid", col("Price").isNotNull() & (col("Price") > MIN_VALID_PRICE))
          .withColumn(
              "rejection_reason",
              when(~col("is_price_valid"), lit("invalid_price")).otherwise(lit(None))
          )
          .drop("is_price_valid")
    )

# COMMAND ----------

def run_silver_products() -> dict:
    """Executes the full Silver processing run with incremental
    processing via the bookmark table."""
    run_started_at = datetime.now(timezone.utc).isoformat()
    logger.info("Silver Products run started")

    last_processed = get_last_processed_timestamp("products")
    logger.info(f"Last processed timestamp: {last_processed}")

    df_bronze = spark.read.table(BRONZE_TABLE).filter(col("ingested_at") > last_processed)
    rows_read = df_bronze.count()

    if rows_read == 0:
        logger.info("No new rows to process — skipping run.")
        return {
            "run_started_at": run_started_at,
            "run_completed_at": datetime.now(timezone.utc).isoformat(),
            "rows_read": 0,
            "rows_good": 0,
            "rows_rejected": 0,
            "status": "skipped_no_new_data",
        }

    profile_bronze_table(df_bronze, "bronze_products")

    df_deduped = deduplicate_latest(df_bronze, BUSINESS_KEY)
    df_cleaned = standardize_product_fields(df_deduped)
    df_validated = validate_products(df_cleaned)

    df_good = df_validated.filter(col("rejection_reason").isNull()).drop("rejection_reason")
    df_rejected = df_validated.filter(col("rejection_reason").isNotNull()).select(
        BUSINESS_KEY, "ProductName", "Category", "Price",
        "batch_id", "source_file", "ingested_at", "rejection_reason"
    )

    rows_good = df_good.count()
    rows_rejected = df_rejected.count()

    df_good.createOrReplaceTempView("silver_products_staged")

    if spark.catalog.tableExists(SILVER_TABLE):
        spark.sql(f"""
            MERGE WITH SCHEMA EVOLUTION INTO {SILVER_TABLE} AS target
            USING silver_products_staged AS source
            ON target.{BUSINESS_KEY} = source.{BUSINESS_KEY}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    else:
        df_good.write.saveAsTable(SILVER_TABLE)

    if rows_rejected > 0:
        df_rejected.write.mode("append").saveAsTable(REJECTED_TABLE)

    latest_ingested_at = df_bronze.agg({"ingested_at": "max"}).collect()[0][0]
    update_processing_log("products", latest_ingested_at)

    metrics = {
        "run_started_at": run_started_at,
        "run_completed_at": datetime.now(timezone.utc).isoformat(),
        "rows_read": rows_read,
        "rows_deduplicated": df_deduped.count(),
        "rows_good": rows_good,
        "rows_rejected": rows_rejected,
        "status": "success",
    }

    logger.info(f"Silver Products run complete: {metrics}")
    return metrics

# COMMAND ----------

metrics = run_silver_products()
metrics