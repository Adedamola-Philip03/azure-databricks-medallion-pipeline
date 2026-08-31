# Databricks notebook source
# ---------------------------------------------------------
# Bronze Layer — Orders (Cloud Source: Azure SQL Database)
# Quantity read as string (not integer, unlike direct CSV read) —
# Silver applies explicit cast. OrderDate also string, mixed formats.
# ---------------------------------------------------------

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS dataengineering.cloud_pipeline")

# COMMAND ----------

import uuid
from datetime import datetime, timezone
from pyspark.sql.functions import lit, sha2, concat_ws, current_user

jdbc_hostname = "wolfsanalytics.database.windows.net"
jdbc_port = 1433
jdbc_database = "DataEngineeringPractice"

jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};database={jdbc_database};encrypt=true;trustServerCertificate=false;loginTimeout=30"

sql_password = dbutils.secrets.get(scope="azure-sql-secrets", key="sql-admin-password")

connection_properties = {
    "user": "wolfsanalytics_admin",
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

batch_id = str(uuid.uuid4())
ingested_at = datetime.now(timezone.utc).isoformat()

print(f"Batch ID: {batch_id}")
print(f"Ingested at: {ingested_at}")

# COMMAND ----------

df_raw = spark.read.jdbc(url=jdbc_url, table="dbo.orders_raw_import", properties=connection_properties)

row_count_received = df_raw.count()
print(f"Rows received: {row_count_received}")
df_raw.printSchema()
df_raw.show(5, truncate=False)

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("batch_id", lit(batch_id))
    .withColumn("source_file", lit("Azure SQL: dbo.orders_raw_import"))
    .withColumn("ingested_at", lit(ingested_at))
    .withColumn("ingested_by", current_user())
    .withColumn("row_hash", sha2(concat_ws("||", *[c for c in df_raw.columns]), 256))
)

# COMMAND ----------

bronze_table = "dataengineering.cloud_pipeline.bronze_orders"

(
    df_bronze.write
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(bronze_table)
)

print(f"Written {row_count_received} rows to {bronze_table}")