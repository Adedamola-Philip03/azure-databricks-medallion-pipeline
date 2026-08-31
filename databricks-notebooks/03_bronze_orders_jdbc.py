# Databricks notebook source
# ---------------------------------------------------------
# Bronze Layer — Orders (Cloud Source: Azure SQL Database)
# Quantity and OrderDate stay untouched as raw strings here — casting
# and date parsing are Silver concerns, not Bronze's.
# ---------------------------------------------------------

# COMMAND ----------

import logging
import uuid
from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, sha2, concat_ws, current_user

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bronze_orders")

# COMMAND ----------

JDBC_HOSTNAME = "wolfsanalytics.database.windows.net"
JDBC_PORT = 1433
JDBC_DATABASE = "DataEngineeringPractice"
SOURCE_TABLE = "dbo.orders_raw_import"
BRONZE_TABLE = "dataengineering.cloud_pipeline.bronze_orders"

JDBC_URL = f"jdbc:sqlserver://{JDBC_HOSTNAME}:{JDBC_PORT};database={JDBC_DATABASE};encrypt=true;trustServerCertificate=false;loginTimeout=30"

# COMMAND ----------

def get_connection_properties() -> dict:
    """Builds JDBC connection properties, retrieving the password
    securely from Databricks Secrets at call time — never hardcoded."""
    sql_password = dbutils.secrets.get(scope="azure-sql-secrets", key="sql-admin-password")
    return {
        "user": "wolfsanalytics_admin",
        "password": sql_password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

# COMMAND ----------

def read_source_table() -> DataFrame:
    """Reads the raw staging table from Azure SQL Database via JDBC."""
    connection_properties = get_connection_properties()
    return spark.read.jdbc(url=JDBC_URL, table=SOURCE_TABLE, properties=connection_properties)

# COMMAND ----------

def attach_lineage(df: DataFrame, batch_id: str, ingested_at: str) -> DataFrame:
    """Attaches lineage columns to every row. Quantity and OrderDate
    stay untouched here (still raw string) — casting and date parsing
    are Silver concerns, not Bronze's."""
    return (
        df
        .withColumn("batch_id", lit(batch_id))
        .withColumn("source_file", lit(f"Azure SQL: {SOURCE_TABLE}"))
        .withColumn("ingested_at", lit(ingested_at))
        .withColumn("ingested_by", current_user())
        .withColumn("row_hash", sha2(concat_ws("||", *[c for c in df.columns]), 256))
    )

# COMMAND ----------

def run_bronze_orders() -> dict:
    """Executes the full Bronze ingestion run for Orders."""
    run_started_at = datetime.now(timezone.utc).isoformat()

    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc).isoformat()

    logger.info(f"Bronze Orders run started — batch_id: {batch_id}")

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

    logger.info(f"Bronze Orders run complete: {metrics}")
    return metrics

# COMMAND ----------

metrics = run_bronze_orders()
metrics