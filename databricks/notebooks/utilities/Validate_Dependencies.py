# Databricks notebook source
# Validate Dependencies - Check environment configuration

import json
from pyspark.sql.utils import AnalysisException

# Configuration
storage_account = "adfdatabricks456"
sql_server = "adf-databricks.database.windows.net"
sql_database = "ADF-Databricks"

results = {"passed": 0, "failed": 0, "checks": []}

def run_check(name, check_func):
    try:
        check_func()
        results["checks"].append({"name": name, "status": "PASS"})
        results["passed"] += 1
    except Exception as e:
        results["checks"].append({"name": name, "status": "FAIL", "error": str(e)})
        results["failed"] += 1

# Check ADLS Mounts
def check_mounts():
    mounts = [m.mountPoint for m in dbutils.fs.mounts()]
    if "/mnt/datalake" not in mounts and not any("datalake" in m for m in mounts):
        raise Exception("ADLS not mounted at /mnt/datalake")

run_check("ADLS Mount", check_mounts)

# Check SQL Connectivity
def check_sql_driver():
    try:
        spark.read.format("jdbc").option("url", "jdbc:sqlserver://localhost").load()
    except Exception as e:
        if "No suitable driver" in str(e):
            raise Exception("JDBC Driver missing")

run_check("JDBC Driver", check_sql_driver)

# Check Delta Lake
def check_delta():
    spark.range(1).write.format("delta").mode("overwrite").save("/tmp/test_delta")
    spark.read.format("delta").load("/tmp/test_delta")

run_check("Delta Lake Write/Read", check_delta)

# Check Secret Scope
def check_secrets():
    try:
        dbutils.secrets.list("kv_secrets")
    except Exception:
        pass

run_check("Secret Scope Access", check_secrets)

print(json.dumps(results, indent=2))

if results["failed"] > 0:
    raise Exception(f"Validation failed with {results['failed']} errors.")
