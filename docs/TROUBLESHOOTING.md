# Troubleshooting Guide

## Common Issues

### 1. Pipeline Always Runs Initial Load

**Symptom**: `PL_Master_Orchestrator` always takes the "True" path for Initial Load.

**Cause**: `initial_load_completed` in `control.table_metadata` is 0.

**Fix**:
1. Check `sp_UpdateTableMetadata` execution logs
2. Verify `Notebook_InitialLoad` completed successfully
3. Check if JDBC update in the notebook failed (see Driver logs)
4. Manual Fix:
   ```sql
   UPDATE control.table_metadata SET initial_load_completed = 1 WHERE table_name = 'YourTable';
   ```

### 2. MERGE Fails in Incremental Load

**Symptom**: Databricks error "AnalysisException: cannot resolve...".

**Cause**: Mismatch between DataFrame columns and Delta Table columns, or Primary Keys defined incorrectly in metadata.

**Fix**:
1. Verify `primary_key_columns` in `control.table_metadata` matches actual columns
2. For composite keys (e.g., `CITY,STATE`), ensure they are comma-separated without extra spaces

### 3. Change Tracking Version Invalid

**Symptom**: `sys.change_tracking_tables` error or "cleanup task has removed data".

**Cause**: Pipeline hasn't run within the retention period (2 days).

**Fix**:
1. Reset metadata to force Initial Load:
   ```sql
   UPDATE control.table_metadata SET initial_load_completed = 0, last_sync_version = 0 WHERE table_id = X;
   ```

### 4. Connection Failures

**Symptom**: ADF or Databricks cannot connect to SQL/Storage.

**Checklist**:
1. Is the firewall on `adf-databricks` allowing Azure Services?
2. Are credentials in Linked Services correct?
3. Did the Databricks PAT token expire?

## How to Add New Tables

1. Enable Change Tracking on the source table
2. Insert a record into `control.table_metadata`
3. If it has dependencies, add to `control.load_dependencies`
4. The Master Orchestrator will automatically pick it up (Dynamic)
