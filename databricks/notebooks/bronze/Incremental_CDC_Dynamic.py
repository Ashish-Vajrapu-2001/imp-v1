# Databricks notebook source
# Incremental CDC Dynamic - Process CDC changes and merge to Delta Lake

from pyspark.sql.functions import lit, current_timestamp, col, max as max_col
from delta.tables import DeltaTable
import json

# Widget definitions
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
bronze_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/{source_system}/{schema_name}/{table_name}/incremental/{pipeline_run_id}"
silver_table_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/silver/{source_system}/{schema_name}/{table_name}"

# Read CDC data
try:
    df_cdc = spark.read.parquet(bronze_path)
except Exception as e:
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED", "msg": "No data found"}))

# Enrich with metadata
df_enriched = df_cdc \
    .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
    .withColumn("_load_timestamp", current_timestamp()) \
    .withColumn("_source_system", lit(source_system)) \
    .withColumnRenamed("SYS_CHANGE_OPERATION", "_cdc_operation")

# Get new max sync version
max_sync_version = df_enriched.select(max_col("_current_sync_version")).collect()[0][0]

# Parse primary keys
pk_list = [x.strip() for x in primary_key_columns.split(',')]
merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_list])

# Merge Logic
if DeltaTable.isDeltaTable(spark, silver_table_path):
    target_table = DeltaTable.forPath(spark, silver_table_path)

    # Get columns to update (exclude CDC metadata)
    update_cols = {c: f"source.{c}" for c in df_enriched.columns
                   if c not in ['_cdc_operation', 'SYS_CHANGE_VERSION', '_current_sync_version']}

    # Execute Merge
    target_table.alias("target").merge(
        df_enriched.alias("source"),
        merge_condition
    ).whenMatchedUpdate(
        condition="source._cdc_operation = 'D'",
        set={"_is_deleted": "lit(True)", "_pipeline_run_id": "source._pipeline_run_id"}
    ).whenMatchedUpdate(
        condition="source._cdc_operation != 'D'",
        set=update_cols
    ).whenNotMatchedInsert(
        condition="source._cdc_operation != 'D'",
        values=update_cols
    ).execute()

    print("Merge completed successfully.")
else:
    print("Target Delta table does not exist. Failing incremental load.")
    raise Exception("Target Delta table not found. Initial Load must run first.")

# Update Metadata via JDBC
jdbc_url = f"jdbc:sqlserver://{sql_server}:1433;database={sql_database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

update_query = f"""
UPDATE control.table_metadata
SET last_load_status = 'SUCCESS',
    last_pipeline_run_id = '{pipeline_run_id}',
    last_sync_version = {max_sync_version},
    records_loaded = {df_enriched.count()},
    last_load_timestamp = GETDATE()
WHERE table_id = {table_id}
"""

try:
    import java.sql.DriverManager as DriverManager
    connection = DriverManager.getConnection(jdbc_url, sql_username, sql_password)
    statement = connection.createStatement()
    statement.executeUpdate(update_query)
    connection.close()
except Exception as e:
    print(f"Failed to update metadata via JDBC: {e}")

dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "rows": df_enriched.count(), "new_version": max_sync_version}))
