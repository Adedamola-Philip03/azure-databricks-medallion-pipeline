# Databricks notebook source
# ---------------------------------------------------------
# Bronze Layer — Customers (Cloud Source: Azure SQL Database)
# Reads raw staging data via JDBC, attaches lineage, writes append-only
# to Bronze. Uses shared configuration from 00_config for connection
# details and environment-aware catalog/schema names.
# ---------------------------------------------------------

# COMMAND ----------

%run /Workspace/Users/adedeji2503@gmail.com/00_config

# COMMAND ----------

import logging
import uuid
from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, sha2, concat_ws, current_user

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bronze_customers")

SOURCE_TABLE = "dbo.customers_raw_import"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_customers"

# COMMAND ----------

def read_source_table() -> DataFrame:
    """Reads the raw staging table from Azure SQL Database via JDBC."""
    connection_properties = get_connection_properties()
    return spark.read.jdbc(url=JDBC_URL, table=SOURCE_TABLE, properties=connection_properties)

# COMMAND ----------

def attach_lineage(df: DataFrame, batch_id: str, ingested_at: str) -> DataFrame:
    """Attaches lineage columns to every row."""
    return (
        df
        .withColumn("batch_id", lit(batch_id))
        .withColumn("source_file", lit(f"Azure SQL: {SOURCE_TABLE}"))
        .withColumn("ingested_at", lit(ingested_at))
        .withColumn("ingested_by", current_user())
        .withColumn("row_hash", sha2(concat_ws("||", *[c for c in df.columns]), 256))
    )

# COMMAND ----------

def run_bronze_customers() -> dict:
    """Executes the full Bronze ingestion run: generate batch identity,
    read from source, attach lineage, write append-only to Bronze."""
    run_started_at = datetime.now(timezone.utc).isoformat()

    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc).isoformat()

    logger.info(f"Bronze Customers run started — batch_id: {batch_id}")

    df_raw = read_source_table()
    rows_received = df_raw.count()

    df_bronze = attach_lineage(df_raw, batch_id, ingested_at)

    (
        df_bronze.write
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(BRONZE_TABLE)
    )

    metrics = {
        "run_started_at": run_started_at,
        "run_completed_at": datetime.now(timezone.utc).isoformat(),
        "batch_id": batch_id,
        "rows_received": rows_received,
        "rows_written": rows_received,
        "status": "success",
    }

    logger.info(f"Bronze Customers run complete: {metrics}")
    return metrics

# COMMAND ----------

metrics = run_bronze_customers()
metrics