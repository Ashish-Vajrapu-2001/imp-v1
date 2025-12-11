# Metadata-Driven CDC Pipeline (Azure SQL to Delta Lake)

## Overview

This project implements a scalable, metadata-driven Change Data Capture (CDC) pipeline. It extracts data from Azure SQL Database using SQL Change Tracking, stages it in ADLS Gen2 (Parquet), and merges it into Delta Lake using Databricks.

## Architecture

```
[Azure SQL] --(CDC)--> [ADF Copy] --(Parquet)--> [ADLS Bronze]
                                                     |
                                                [Databricks]
                                                     |
[Control DB] <--(Metadata Update)-- [Spark Merge] --(Delta)--> [ADLS Silver]
```

## File Structure

- `sql/`: Database scripts for control tables and stored procs
- `adf/`: JSON definitions for pipelines, datasets, and linked services
- `databricks/`: PySpark notebooks for Initial and Incremental processing
- `docs/`: Deployment and troubleshooting guides

## Quick Start

1. **Deploy SQL**: Run scripts in `sql/`
2. **Configure ADF**: Create Linked Services pointing to your resources (`adfdatabricks456`, etc.)
3. **Configure Databricks**: Mount storage and set up secrets
4. **Run**: Trigger `PL_Master_Orchestrator`

## Key Features

- **Idempotent**: Uses `pipeline_run_id` for unique data paths
- **Self-Healing**: Automatically switches between Initial and Incremental loads based on metadata state
- **Dynamic**: Adding a table only requires a metadata insert; no code changes
- **Composite Key Support**: Handles single and composite primary keys for MERGE operations
