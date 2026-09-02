# Databricks notebook source
# ---------------------------------------------------------
# Shared Configuration
# Single source of truth for connection details and catalog/schema
# names used across all Bronze, Silver, and Gold notebooks. Referenced
# via %run from every other notebook, so an environment change (e.g.
# switching Azure SQL servers, or promoting dev -> prod) only requires
# editing this one file.
# ---------------------------------------------------------

# COMMAND ----------

dbutils.widgets.text("environment", "prod", "Environment")
ENVIRONMENT = dbutils.widgets.get("environment")

# COMMAND ----------

JDBC_HOSTNAME = "wolfsanalytics.database.windows.net"
JDBC_PORT = 1433
JDBC_DATABASE = "DataEngineeringPractice"
JDBC_URL = f"jdbc:sqlserver://{JDBC_HOSTNAME}:{JDBC_PORT};database={JDBC_DATABASE};encrypt=true;trustServerCertificate=false;loginTimeout=30"

CATALOG = "dataengineering"
SCHEMA = "cloud_pipeline" if ENVIRONMENT == "prod" else f"cloud_pipeline_{ENVIRONMENT}"

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