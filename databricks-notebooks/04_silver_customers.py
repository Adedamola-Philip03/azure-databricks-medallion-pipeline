# Databricks notebook source
# ---------------------------------------------------------
# Silver Layer — Customers
# Deduplicates, standardizes, and validates Bronze customer records.
# Business rules are externalized as configuration rather than hardcoded
# inline, so rule changes don't require touching pipeline logic.
# ---------------------------------------------------------

# COMMAND ----------

import logging
from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, when, lit, trim, initcap, lower, row_number, desc,
    count, countDistinct
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("silver_customers")

# COMMAND ----------

BRONZE_TABLE = "dataengineering.cloud_pipeline.bronze_customers"
SILVER_TABLE = "dataengineering.cloud_pipeline.silver_customers"
REJECTED_TABLE = "dataengineering.cloud_pipeline.rejected_customers"
BUSINESS_KEY = "CustomerID"

VALID_COUNTRIES = ["Nigeria", "Ghana", "Kenya", "Togo"]
EMAIL_PATTERN = r"^[A-Za-z0-9._-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

# COMMAND ----------

def profile_bronze_table(df: DataFrame, name: str) -> None:
    """Inspects a Bronze table for data quality signals before any
    cleaning/validation logic is written against it. This is how an
    engineer approaches an unfamiliar dataset — profile first, assume
    nothing."""
    logger.info(f"Profiling {name}: {df.count()} rows")

    null_exprs = [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
    logger.info("True null counts per column:")
    df.select(null_exprs).show(truncate=False)

    fake_null_count = df.filter(lower(trim(col("Email"))) == "null").count()
    logger.info(f"Rows with literal 'null' string in Email: {fake_null_count}")

    distinct_countries = df.select("Country").distinct().count()
    logger.info(f"Distinct Country values (before cleaning): {distinct_countries}")
    df.select("Country").distinct().show(truncate=False)

# COMMAND ----------

def deduplicate_latest(df: DataFrame, business_key: str, order_col: str = "ingested_at") -> DataFrame:
    """Keep only the most recently ingested record per business key.
    Bronze retains full history across all batches; Silver represents
    current state only."""
    window_spec = Window.partitionBy(business_key).orderBy(desc(order_col))
    return (
        df.withColumn("_rn", row_number().over(window_spec))
          .filter(col("_rn") == 1)
          .drop("_rn")
    )

# COMMAND ----------

def standardize_customer_fields(df: DataFrame) -> DataFrame:
    """Trim whitespace, normalize casing, and resolve known 'fake null'
    representations (the literal string 'null', confirmed present via
    profiling) to true nulls."""
    return (
        df.withColumn("CustomerName", initcap(trim(col("CustomerName"))))
          .withColumn("Country", initcap(trim(col("Country"))))
          .withColumn("Email", trim(col("Email")))
          .withColumn("Email", when(lower(col("Email")) == "null", None).otherwise(col("Email")))
          .withColumn("Email", when(col("Email").isNull(), None).otherwise(lower(col("Email"))))
    )

# COMMAND ----------

def validate_customers(df: DataFrame) -> DataFrame:
    """Apply business rules discovered/confirmed via profiling.
    Country is a hard rejection rule — an unrecognized country breaks
    referential and reporting integrity. Email is a soft quality flag —
    a bad email doesn't disqualify a customer from being counted in
    revenue or order reporting; it flags a contactability gap instead."""
    return (
        df.withColumn("is_country_valid", col("Country").isin(VALID_COUNTRIES))
          .withColumn(
              "email_quality_flag",
              when(col("Email").isNull(), "missing")
              .when(~col("Email").rlike(EMAIL_PATTERN), "invalid_format")
              .otherwise("valid")
          )
          .withColumn(
              "rejection_reason",
              when(~col("is_country_valid"), lit("invalid_country")).otherwise(lit(None))
          )
          .drop("is_country_valid")
    )

# COMMAND ----------

def run_silver_customers() -> dict:
    """Executes the full Silver processing run: profile → dedup → clean
    → validate → merge/reject. Returns a metrics dict suitable for
    logging to an audit table or monitoring dashboard."""
    run_started_at = datetime.now(timezone.utc).isoformat()
    logger.info("Silver Customers run started")

    df_bronze = spark.read.table(BRONZE_TABLE)
    profile_bronze_table(df_bronze, "bronze_customers")

    rows_read = df_bronze.count()

    df_deduped = deduplicate_latest(df_bronze, BUSINESS_KEY)
    df_cleaned = standardize_customer_fields(df_deduped)
    df_validated = validate_customers(df_cleaned)

    df_good = df_validated.filter(col("rejection_reason").isNull()).drop("rejection_reason")
    df_rejected = df_validated.filter(col("rejection_reason").isNotNull()).select(
        BUSINESS_KEY, "CustomerName", "Country", "Email",
        "batch_id", "source_file", "ingested_at", "rejection_reason"
    )

    rows_good = df_good.count()
    rows_rejected = df_rejected.count()

    df_good.createOrReplaceTempView("silver_customers_staged")

    if spark.catalog.tableExists(SILVER_TABLE):
        spark.sql(f"""
            MERGE WITH SCHEMA EVOLUTION INTO {SILVER_TABLE} AS target
            USING silver_customers_staged AS source
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

    logger.info(f"Silver Customers run complete: {metrics}")
    return metrics

# COMMAND ----------

metrics = run_silver_customers()
metrics