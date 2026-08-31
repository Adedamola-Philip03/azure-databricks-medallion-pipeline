# Azure Databricks Medallion Pipeline

A cloud-based data engineering pipeline demonstrating medallion architecture (Bronze → Silver → Gold) using Azure SQL Database as the source system and Databricks Serverless as the compute/processing layer.

## Architecture

- **Source / Landing Zone:** Azure SQL Database — raw CSVs (Customers, Products, Orders) imported as staging tables
- **Bronze Layer:** Raw data with full lineage (batch_id, file_hash, row_hash, ingested_at, ingested_by) — append-only, no transformation
- **Silver Layer:** Deduplicated, cleaned, and validated data — hard rejection rules for identity-breaking issues, soft quality flags for non-critical issues (e.g. missing email)
- **Gold Layer:** Business-ready aggregates for reporting

## Key Design Decisions

- **Batch ID vs File Hash:** every ingestion run gets a unique batch_id; file_hash detects whether the same file content has been re-ingested, independent of run count
- **Hard rejection vs soft flag:** a broken email doesn't disqualify a customer from being counted in revenue reporting — it's flagged (`email_quality_flag`), not rejected. Country validity, however, is a hard rule since it reflects data integrity, not just contact reachability
- **Referential integrity (Orders):** orders referencing a non-existent CustomerID or ProductID are rejected with a specific reason, not silently dropped

## Real Bugs Found and Fixed

- **Literal "null" string vs true NULL:** a source file contained the text `"null"` instead of a real empty value — caught via profiling, not assumption
- **Delta schema evolution:** `MERGE` silently dropped a new column (`email_quality_flag`) until `MERGE WITH SCHEMA EVOLUTION` was used
- **SQL Server Agent / Named Pipes:** scheduled jobs failed silently until Named Pipes protocol was enabled at the OS level — diagnosed via the Agent-specific error log, not the SQL Server engine log
- **Mixed date formats:** `OrderDate` contained both `M/D/YYYY` and `D/M/YYYY` in the same column — resolved via `try_to_date` with a fallback pattern

## Tech Stack

- Azure SQL Database
- Databricks (PySpark, Serverless Compute)
- T-SQL (stored procedures, custom INITCAP function)
- SQL Server Agent (automation)
- Databricks Secrets (secure credential handling)

## Folder Structure

- `sql-scripts/` — schema creation, table definitions, stored procedures
- `databricks-notebooks/` — PySpark ingestion and transformation logic
- `docs/` — additional documentation