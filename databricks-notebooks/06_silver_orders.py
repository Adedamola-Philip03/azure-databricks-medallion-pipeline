# Databricks notebook source
# ---------------------------------------------------------
# Silver Layer — Orders
# Deduplicates, standardizes, and validates Bronze order records.
# Unlike Customers/Products, validity here is relational: an order
# is only valid if it references a real, known CustomerID and ProductID
# in their respective Silver tables. This is the only Silver script
# that depends on other Silver tables rather than just its own Bronze
# source.
# ---------------------------------------------------------

# COMMAND ----------

import logging
from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, when, lit, row_number, desc, count, expr, coalesce
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("silver_orders")

# COMMAND ----------

BRONZE_TABLE = "dataengineering.cloud_pipeline.bronze_orders"
SILVER_TABLE = "dataengineering.cloud_pipeline.silver_orders"
REJECTED_TABLE = "dataengineering.cloud_pipeline.rejected_orders"
BUSINESS_KEY = "OrderID"

SILVER_CUSTOMERS_TABLE = "dataengineering.cloud_pipeline.silver_customers"
SILVER_PRODUCTS_TABLE = "dataengineering.cloud_pipeline.silver_products"

MIN_VALID_QUANTITY = 0

# COMMAND ----------

def profile_bronze_table(df: DataFrame, name: str) -> None:
    """Inspects a Bronze table for data quality signals before any
    cleaning/validation logic is written against it."""
    logger.info(f"Profiling {name}: {df.count()} rows")

    null_exprs = [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
    logger.info("True null counts per column:")
    df.select(null_exprs).show(truncate=False)

    logger.info("Quantity column raw distinct values (checking type/contamination):")
    df.select("Quantity").distinct().show(truncate=False)

    logger.info("OrderDate column raw distinct values (checking for mixed formats):")
    df.select("OrderDate").distinct().show(50, truncate=False)

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

def standardize_order_fields(df: DataFrame) -> DataFrame:
    """Casts Quantity to a real integer (confirmed string-typed via
    profiling, despite clean-looking values) and resolves OrderDate's
    mixed M/D/YYYY and D/M/YYYY formats, plus the literal 'null' string
    bug, using try_to_date with a fallback pattern."""
    return (
        df.withColumn("Quantity", expr("try_cast(Quantity AS INT)"))
          .withColumn(
              "OrderDate",
              coalesce(
                  expr("try_to_date(OrderDate, 'M/d/yyyy')"),
                  expr("try_to_date(OrderDate, 'd/M/yyyy')")
              )
          )
    )

# COMMAND ----------

def validate_orders(df: DataFrame, df_customers: DataFrame, df_products: DataFrame) -> DataFrame:
    """Rejects orders that reference a non-existent CustomerID or
    ProductID (referential integrity), have a non-positive Quantity,
    or have a missing/unparseable OrderDate."""

    valid_customer_ids = df_customers.select("CustomerID").distinct().withColumnRenamed("CustomerID", "valid_cust_id")
    valid_product_ids = df_products.select("ProductID").distinct().withColumnRenamed("ProductID", "valid_prod_id")

    df_joined = (
        df
        .join(valid_customer_ids, df["CustomerID"] == col("valid_cust_id"), "left")
        .join(valid_product_ids, df["ProductID"] == col("valid_prod_id"), "left")
    )

    return (
        df_joined
        .withColumn("is_customer_valid", col("valid_cust_id").isNotNull())
        .withColumn("is_product_valid", col("valid_prod_id").isNotNull())
        .withColumn("is_quantity_valid", col("Quantity").isNotNull() & (col("Quantity") > MIN_VALID_QUANTITY))
        .withColumn("is_date_valid", col("OrderDate").isNotNull())
        .withColumn(
            "rejection_reason",
            when(~col("is_customer_valid"), lit("unknown_customer"))
            .when(~col("is_product_valid"), lit("unknown_product"))
            .when(~col("is_quantity_valid"), lit("invalid_quantity"))
            .when(~col("is_date_valid"), lit("missing_order_date"))
            .otherwise(lit(None))
        )
        .drop("valid_cust_id", "valid_prod_id", "is_customer_valid", "is_product_valid", "is_quantity_valid", "is_date_valid")
    )

# COMMAND ----------

def run_silver_orders() -> dict:
    """Executes the full Silver processing run: profile → dedup → clean
    → validate (including referential integrity against Silver Customers
    and Silver Products) → merge/reject."""
    run_started_at = datetime.now(timezone.utc).isoformat()
    logger.info("Silver Orders run started")

    df_bronze = spark.read.table(BRONZE_TABLE)
    profile_bronze_table(df_bronze, "bronze_orders")

    df_silver_customers = spark.read.table(SILVER_CUSTOMERS_TABLE)
    df_silver_products = spark.read.table(SILVER_PRODUCTS_TABLE)

    rows_read = df_bronze.count()

    df_deduped = deduplicate_latest(df_bronze, BUSINESS_KEY)
    df_cleaned = standardize_order_fields(df_deduped)
    df_validated = validate_orders(df_cleaned, df_silver_customers, df_silver_products)

    df_good = df_validated.filter(col("rejection_reason").isNull()).drop("rejection_reason")
    df_rejected = df_validated.filter(col("rejection_reason").isNotNull()).select(
        BUSINESS_KEY, "CustomerID", "ProductID", "Quantity", "OrderDate",
        "batch_id", "source_file", "ingested_at", "rejection_reason"
    )

    rows_good = df_good.count()
    rows_rejected = df_rejected.count()

    df_good.createOrReplaceTempView("silver_orders_staged")

    if spark.catalog.tableExists(SILVER_TABLE):
        spark.sql(f"""
            MERGE WITH SCHEMA EVOLUTION INTO {SILVER_TABLE} AS target
            USING silver_orders_staged AS source
            ON target.{BUSINESS_KEY} = source.{BUSINESS_KEY}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    else:
        df_good.write.saveAsTable(SILVER_TABLE)

    if rows_rejected > 0:
        df_rejected.write.mode("append").saveAsTable(REJECTED_TABLE)

    metrics = {
        "run_started_at": run_started_at,
        "run_completed_at": datetime.now(timezone.utc).isoformat(),
        "rows_read": rows_read,
        "rows_deduplicated": df_deduped.count(),
        "rows_good": rows_good,
        "rows_rejected": rows_rejected,
        "status": "success",
    }

    logger.info(f"Silver Orders run complete: {metrics}")
    return metrics

# COMMAND ----------

metrics = run_silver_orders()
metrics