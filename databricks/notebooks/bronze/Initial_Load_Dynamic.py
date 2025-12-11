# Databricks notebook source
# Initial Load Dynamic - Process full table load to Delta Lake

from pyspark.sql.functions import lit, current_timestamp, col, max as max_col
from delta.tables import DeltaTable
import json

# Widget definitions for parameters
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("table_id", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("primary_key_columns", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("sql_server", "")
dbutils.widgets.text("sql_database", "")
dbutils.widgets.text("sql_username", "")
dbutils.widgets.text("sql_password", "")

# Retrieve parameters
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
table_id = dbutils.widgets.get("table_id")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
source_system = dbutils.widgets.get("source_system")
primary_key_columns = dbutils.widgets.get("primary_key_columns")
storage_account = dbutils.widgets.get("storage_account")
sql_server = dbutils.widgets.get("sql_server")
sql_database = dbutils.widgets.get("sql_database")
sql_username = dbutils.widgets.get("sql_username")
sql_password = dbutils.widgets.get("sql_password")

# Define paths
bronze_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/{source_system}/{schema_name}/{table_name}/initial/{pipeline_run_id}"
silver_table_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/silver/{source_system}/{schema_name}/{table_name}"

print(f"Reading from: {bronze_path}")
print(f"Writing to: {silver_table_path}")

# Read Parquet data
try:
    df = spark.read.parquet(bronze_path)
except Exception as e:
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "error": f"No data found: {str(e)}"}))

# Add metadata columns
df_enriched = df \
    .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
    .withColumn("_load_timestamp", current_timestamp()) \
    .withColumn("_source_system", lit(source_system)) \
    .withColumn("_is_deleted", lit(False)) \
    .withColumn("_cdc_operation", lit("INITIAL"))

# Extract max sync version from data
max_sync_version = df_enriched.select(max_col("_current_sync_version")).collect()[0][0]
if max_sync_version is None:
    max_sync_version = 0

# Write to Delta (Overwrite for Initial Load)
df_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", silver_table_path) \
    .saveAsTable(f"{source_system}_{schema_name}_{table_name}")

print("Delta Table Created.")

# Update control table via JDBC
jdbc_url = f"jdbc:sqlserver://{sql_server}:1433;database={sql_database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
connection_properties = {
    "user": sql_username,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

update_query = f"""
UPDATE control.table_metadata
SET initial_load_completed = 1,
    last_load_status = 'SUCCESS',
    last_pipeline_run_id = '{pipeline_run_id}',
    last_sync_version = {max_sync_version},
    records_loaded = {df_enriched.count()},
    last_load_timestamp = GETDATE(),
    bronze_path = '{bronze_path}',
    silver_path = '{silver_table_path}'
WHERE table_id = {table_id}
"""

print(f"Executing Metadata Update: {update_query}")

try:
    import java.sql.DriverManager as DriverManager
    connection = DriverManager.getConnection(jdbc_url, sql_username, sql_password)
    statement = connection.createStatement()
    statement.executeUpdate(update_query)
    connection.close()
    print("Metadata updated successfully.")
except Exception as e:
    print(f"Failed to update metadata via JDBC: {e}")

dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "rows": df_enriched.count(), "version": max_sync_version}))
