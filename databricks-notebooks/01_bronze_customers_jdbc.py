# Databricks notebook source
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

df_test = spark.read.jdbc(url=jdbc_url, table="INFORMATION_SCHEMA.TABLES", properties=connection_properties)
df_test.show()

# COMMAND ----------

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

df_test = spark.read.jdbc(url=jdbc_url, table="INFORMATION_SCHEMA.TABLES", properties=connection_properties)
df_test.show()