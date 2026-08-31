# Databricks notebook source
# ---------------------------------------------------------
# Gold Layer — Business Aggregates
# Built exclusively on validated Silver tables. Any row excluded during
# Silver validation (bad prices, unknown references, invalid data) is
# correctly absent from these numbers by construction, not by luck.
# ---------------------------------------------------------

# COMMAND ----------

import logging
from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, round as spark_round

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("gold_aggregates")

# COMMAND ----------

SILVER_CUSTOMERS_TABLE = "dataengineering.cloud_pipeline.silver_customers"
SILVER_PRODUCTS_TABLE = "dataengineering.cloud_pipeline.silver_products"
SILVER_ORDERS_TABLE = "dataengineering.cloud_pipeline.silver_orders"

GOLD_CUSTOMER_SUMMARY_TABLE = "dataengineering.cloud_pipeline.gold_customer_country_summary"
GOLD_REVENUE_TABLE = "dataengineering.cloud_pipeline.gold_revenue_by_category"

# COMMAND ----------

def build_customer_country_summary(df_customers: DataFrame) -> DataFrame:
    """Aggregates customer count and email reachability by country."""
    return (
        df_customers
        .groupBy("Country")
        .agg(
            count("*").alias("total_customers"),
            spark_sum(when(col("email_quality_flag") == "valid", 1).otherwise(0)).alias("valid_email_count"),
            spark_sum(when(col("email_quality_flag") == "missing", 1).otherwise(0)).alias("missing_email_count"),
            spark_sum(when(col("email_quality_flag") == "invalid_format", 1).otherwise(0)).alias("invalid_email_count")
        )
        .withColumn(
            "valid_email_rate_pct",
            spark_round((col("valid_email_count") / col("total_customers")) * 100, 1)
        )
        .orderBy(col("total_customers").desc())
    )

# COMMAND ----------

def build_revenue_by_category(df_orders: DataFrame, df_products: DataFrame) -> DataFrame:
    """Joins validated orders to validated products to compute revenue
    and units sold per category."""
    return (
        df_orders
        .join(df_products, on="ProductID", how="inner")
        .withColumn("line_total", col("Quantity") * col("Price"))
        .groupBy("Category")
        .agg(
            spark_sum("line_total").alias("total_revenue"),
            spark_sum("Quantity").alias("total_units_sold")
        )
        .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
        .orderBy(col("total_revenue").desc())
    )

# COMMAND ----------

def run_gold_aggregates() -> dict:
    """Rebuilds both Gold aggregate tables from current Silver data.
    Gold is fully derived and reproducible — every run overwrites the
    prior result rather than merging, since Gold has no independent
    state of its own."""
    run_started_at = datetime.now(timezone.utc).isoformat()
    logger.info("Gold aggregates run started")

    df_customers = spark.read.table(SILVER_CUSTOMERS_TABLE)
    df_products = spark.read.table(SILVER_PRODUCTS_TABLE)
    df_orders = spark.read.table(SILVER_ORDERS_TABLE)

    df_customer_summary = build_customer_country_summary(df_customers)
    df_revenue_summary = build_revenue_by_category(df_orders, df_products)

    df_customer_summary.write.mode("overwrite").saveAsTable(GOLD_CUSTOMER_SUMMARY_TABLE)
    df_revenue_summary.write.mode("overwrite").saveAsTable(GOLD_REVENUE_TABLE)

    metrics = {
        "run_started_at": run_started_at,
        "run_completed_at": datetime.now(timezone.utc).isoformat(),
        "customer_summary_rows": df_customer_summary.count(),
        "revenue_summary_rows": df_revenue_summary.count(),
        "status": "success",
    }

    logger.info(f"Gold aggregates run complete: {metrics}")
    return metrics

# COMMAND ----------

metrics = run_gold_aggregates()
metrics