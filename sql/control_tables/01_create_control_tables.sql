/*
  FILE: sql/control_tables/01_create_control_tables.sql
  PURPOSE: Create control schema and metadata tables for CDC pipeline
*/

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'control')
BEGIN
    EXEC('CREATE SCHEMA [control]')
END
GO

-- Source Systems Registry
IF OBJECT_ID('control.source_systems', 'U') IS NOT NULL DROP TABLE control.source_systems;
CREATE TABLE control.source_systems (
    source_system_id INT IDENTITY(1,1) PRIMARY KEY,
    source_system_name NVARCHAR(100) NOT NULL,
    source_system_type NVARCHAR(50) NOT NULL,
    is_active BIT DEFAULT 1,
    created_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Table Metadata Configuration
IF OBJECT_ID('control.table_metadata', 'U') IS NOT NULL DROP TABLE control.table_metadata;
CREATE TABLE control.table_metadata (
    table_id INT IDENTITY(1,1) PRIMARY KEY,
    source_system_id INT NOT NULL,
    schema_name NVARCHAR(50) NOT NULL,
    table_name NVARCHAR(100) NOT NULL,
    primary_key_columns NVARCHAR(255) NOT NULL,
    load_type NVARCHAR(20) DEFAULT 'CDC',
    is_active BIT DEFAULT 1,
    initial_load_completed BIT DEFAULT 0,
    last_sync_version BIGINT DEFAULT 0,
    last_load_status NVARCHAR(20) DEFAULT 'PENDING',
    last_load_timestamp DATETIME2,
    last_pipeline_run_id NVARCHAR(100),
    records_loaded INT DEFAULT 0,
    bronze_path NVARCHAR(500),
    silver_path NVARCHAR(500),
    load_priority INT DEFAULT 100,
    created_date DATETIME2 DEFAULT GETDATE(),
    modified_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_Metadata_SourceSystem FOREIGN KEY (source_system_id)
        REFERENCES control.source_systems(source_system_id)
);
GO

-- Load Dependencies
IF OBJECT_ID('control.load_dependencies', 'U') IS NOT NULL DROP TABLE control.load_dependencies;
CREATE TABLE control.load_dependencies (
    dependency_id INT IDENTITY(1,1) PRIMARY KEY,
    table_id INT NOT NULL,
    depends_on_table_id INT NOT NULL,
    dependency_type NVARCHAR(50) DEFAULT 'FOREIGN_KEY',
    CONSTRAINT FK_Dep_Table FOREIGN KEY (table_id) REFERENCES control.table_metadata(table_id),
    CONSTRAINT FK_Dep_Parent FOREIGN KEY (depends_on_table_id) REFERENCES control.table_metadata(table_id)
);
GO

-- Pipeline Execution Log
IF OBJECT_ID('control.pipeline_execution_log', 'U') IS NOT NULL DROP TABLE control.pipeline_execution_log;
CREATE TABLE control.pipeline_execution_log (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id NVARCHAR(100) NOT NULL,
    table_id INT NOT NULL,
    execution_status NVARCHAR(20),
    records_processed INT,
    start_time DATETIME2 DEFAULT GETDATE(),
    end_time DATETIME2,
    error_message NVARCHAR(MAX),
    CONSTRAINT FK_Log_Table FOREIGN KEY (table_id) REFERENCES control.table_metadata(table_id)
);
GO

-- Data Quality Rules
IF OBJECT_ID('control.data_quality_rules', 'U') IS NOT NULL DROP TABLE control.data_quality_rules;
CREATE TABLE control.data_quality_rules (
    rule_id INT IDENTITY(1,1) PRIMARY KEY,
    table_id INT NOT NULL,
    rule_name NVARCHAR(100),
    rule_type NVARCHAR(50),
    rule_expression NVARCHAR(MAX),
    is_active BIT DEFAULT 1,
    CONSTRAINT FK_Rule_Table FOREIGN KEY (table_id) REFERENCES control.table_metadata(table_id)
);
GO
