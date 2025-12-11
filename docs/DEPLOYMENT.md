# Deployment Guide: Metadata-Driven CDC Pipeline

## Prerequisites

### Azure Resources
- Storage Account: `adfdatabricks456`
- SQL Server: `adf-databricks`
- SQL Database: `ADF-Databricks`
- Databricks Workspace: `Adf-databricks456`

### Permissions
- ADF Managed Identity must have `Storage Blob Data Contributor`
- Databricks Service Principal must have access to KeyVault

### Change Tracking
Must be enabled on source SQL Database and all source tables:

```sql
ALTER DATABASE [ADF-Databricks] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
ALTER TABLE ERP.OE_ORDER_HEADERS_ALL ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);
-- Repeat for all tables
```

## Step-by-Step Deployment

### 1. Database Setup
1. Open SSMS or Azure Data Studio connected to `adf-databricks.database.windows.net`
2. Run `sql/control_tables/01_create_control_tables.sql`
3. Run `sql/control_tables/02_populate_control_tables.sql`
4. Run `sql/stored_procedures/sp_GetCDCChanges.sql`
5. Run `sql/stored_procedures/sp_UpdateTableMetadata.sql`
6. Run `sql/stored_procedures/sp_GetTableLoadOrder.sql`

### 2. ADF Linked Services
1. Go to ADF Manage -> Linked Services
2. Create `LS_AzureSQL_Control` (Update `{{PLACEHOLDER_SQL_PASSWORD}}`)
3. Create `LS_AzureDataLakeStorage` (Update `{{PLACEHOLDER_STORAGE_KEY}}`)
4. Create `LS_AzureDatabricks` (Update Token and Cluster ID)

### 3. ADF Datasets
1. Create `DS_ControlDB`, `DS_AzureSQL_Source`, `DS_Parquet_Bronze`
2. Ensure parameters match the JSON definitions provided

### 4. Databricks Setup
1. Create a Cluster (Runtime 11.3 LTS or higher recommended)
2. Create Secret Scope `kv_secrets` backed by KeyVault
3. Import Notebooks from `databricks/notebooks/` folder
4. Run `setup/Mount_ADLS` to mount storage
5. Run `utilities/Validate_Dependencies` to confirm environment

### 5. ADF Pipelines
1. Import `PL_Initial_Load_Single_Table`
2. Import `PL_Incremental_CDC_Single_Table`
3. Import `PL_Master_Orchestrator`

### 6. Initial Execution
1. Trigger `PL_Master_Orchestrator`
2. Monitor `control.table_metadata`. The `initial_load_completed` flag should turn to `1` upon success
