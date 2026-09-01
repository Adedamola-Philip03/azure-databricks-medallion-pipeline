
`silver_orders` depends on both `silver_customers` and `silver_products`
completing successfully, since it validates every order against both tables.
`gold_aggregates` depends on `silver_customers` and `silver_orders`. Any
upstream failure correctly blocks downstream tasks rather than cascading
silently — verified in practice: a syntax error in Bronze Products and a
misconfigured notebook path in Silver Orders were both caught this way during
testing, with every dependent task correctly staying unrun until fixed.

A time-based trigger (interval schedule) was tested and verified to run the
full chain unattended, correctly picking up a manually inserted new customer
record and reflecting it all the way through to the Gold aggregate — proving
genuine end-to-end automation, not just a working manual script.

## Key Design Decisions

- **Batch ID vs file hash:** every ingestion run gets a unique batch_id;
  file_hash detects whether the same file content has been re-ingested,
  independent of run count. For JDBC-sourced Bronze tables (Azure SQL, not a
  static file), file_hash does not apply — row_hash covers row-level
  lineage instead
- **Hard rejection vs soft flag:** a broken email doesn't disqualify a
  customer from being counted in revenue reporting — it's flagged
  (`email_quality_flag`), not rejected. Country validity, product price, and
  order referential integrity are hard rules, since they reflect data
  integrity rather than just contact reachability
- **Referential integrity (Orders):** orders referencing a non-existent
  CustomerID or ProductID are rejected with a specific reason, not silently
  dropped
- **Config-driven business rules:** validation thresholds and table names are
  defined as constants at the top of each notebook, separate from the
  transformation logic, so rule changes don't require touching pipeline code
- **Structured run metrics:** every notebook returns a metrics dict
  (row counts, timestamps, status) instead of scattered print statements,
  suitable for logging to a monitoring system
- **Profiling before transformation:** every Silver notebook profiles its
  Bronze source (null counts, fake-null detection, cardinality checks)
  before any cleaning or validation logic runs — proving data quality issues
  with evidence rather than assuming them from memory

## Real Bugs Found and Fixed

- **Literal "null" string vs true NULL:** a source file contained the text
  `"null"` instead of a real empty value — caught via profiling, not
  assumption
- **Delta schema evolution:** `MERGE` silently dropped a new column
  (`email_quality_flag`) until `MERGE WITH SCHEMA EVOLUTION` was used
- **SQL Server Agent / Named Pipes:** scheduled jobs failed silently until
  Named Pipes protocol was enabled at the OS level — diagnosed via the
  Agent-specific error log, not the SQL Server engine log
- **Mixed date formats:** `OrderDate` contained both `M/D/YYYY` and
  `D/M/YYYY` in the same column — resolved via `try_to_date` with a fallback
  pattern
- **Corrupted PowerShell token:** a control character introduced during an
  interactive terminal paste caused a silent `Bad Request` from the
  Databricks CLI — diagnosed by inspecting the raw HTTP request in debug
  mode, not by re-guessing the token
- **Job orchestration failures:** a stray unquoted line broke Python syntax
  in the Bronze Products notebook, and Silver Orders' Job task pointed to a
  stale duplicate notebook — both caught because the Job's dependency graph
  correctly blocked all downstream tasks rather than running against broken
  or incomplete data

## BI Connectivity

A Databricks SQL Warehouse (Serverless) is provisioned and running,
exposing the Gold tables (`gold_customer_country_summary`,
`gold_revenue_by_category`) for connection via Power BI's native Databricks
connector. Connection details (server hostname, HTTP path) are available
from the SQL Warehouse's connection panel. Building out the actual Power BI
report/dashboard on top of this connection is tracked as a next step (see
Production Roadmap).

## Known Limitations

- No `file_hash` equivalent for JDBC/live-table sources — only applicable to
  file-based ingestion (documented trade-off, not an oversight)
- Azure SQL Database firewall currently allows all IPs to support Databricks
  Serverless connectivity, since Databricks does not publish a stable,
  easily-allowlisted IP range for this tier — a production environment would
  use private networking (VNet peering / Private Link) instead
- Full Bronze reprocessing on every Silver run — no incremental bookmark
  table yet (tracked in the production roadmap)
- New source data currently requires a manual insert/import into the Azure
  SQL staging tables — no upstream system continuously lands new data

See `docs/production_roadmap.md` for the full phased plan, including
incremental processing, parameterization, and further BI reporting work.

## Tech Stack

- Azure SQL Database
- Databricks (PySpark, Serverless Compute, Jobs/Workflows, SQL Warehouses)
- T-SQL (stored procedures, custom INITCAP function) — parallel local
  SQL Server implementation
- SQL Server Agent (local pipeline automation)
- Databricks Secrets (secure credential handling)
- Power BI (reporting, via Databricks SQL Warehouse connection)

## Folder Structure

- `sql-scripts/` — schema creation, table definitions, stored procedures
  (local SQL Server implementation)
- `databricks-notebooks/` — PySpark ingestion, transformation, and
  aggregation logic (cloud pipeline)
- `docs/` — production readiness roadmap and additional documentation