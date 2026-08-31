# Production Readiness Roadmap

This document tracks the gap between "working pipeline" (what exists today)
and "production-grade" (the target), and the order we're closing that gap.

## ✅ Already Complete

- Cloud-sourced medallion architecture: Azure SQL Database → Databricks (Bronze → Silver → Gold)
- Full lineage tracking (batch_id, ingested_at, ingested_by, row_hash)
- Data profiling before transformation (null checks, fake-null detection, cardinality checks)
- Modular, documented PySpark functions (not monolithic scripts)
- Config-driven business rules (table names, validation thresholds separated from logic)
- Structured run metrics (dict output, not scattered print statements)
- Referential integrity checks (Orders → Customers/Products)
- Hard rejection vs. soft quality flag distinction, applied per business rule
- Secure credential handling via Databricks Secrets (no hardcoded passwords)
- Version control with meaningful, incremental commit history
- Parallel local SQL Server implementation (T-SQL stored procedures, SQL Server Agent automation)

## 🔲 Phase 1 — Orchestration (in progress)

- [ ] Databricks Workflow/Job chaining Bronze → Silver → Gold in dependency order
- [ ] Job fails clearly and stops downstream steps if an upstream step fails
      (no silent cascade — e.g. Silver Orders should not run if Silver
      Customers failed)
- [ ] Scheduled trigger (e.g. daily) — direct equivalent of the SQL Server
      Agent job already built for the local pipeline

## 🔲 Phase 2 — Documentation Overhaul

- [ ] README updated to reflect Gold layer, referential integrity, and
      the full JDBC/Secrets setup
- [ ] Architecture diagram (described in words, or an actual image if time
      allows) showing the full data flow
- [ ] "Known limitations" section — documented honestly, e.g.:
  - No file_hash equivalent for live-table sources (only file-based ingestion)
  - Firewall currently allows all IPs for Databricks Serverless connectivity
    (documented trade-off, not an oversight)
  - Full Bronze reprocessing on every Silver run (no incremental bookmark yet)

## 🔲 Phase 3 — Incremental Processing (future improvement)

- [ ] Control/bookmark table tracking last-processed batch per source table
- [ ] Silver reads only new Bronze batches since last successful run,
      not full history every time

## 🔲 Phase 4 — Parameterization (future improvement)

- [ ] Move hardcoded values (table names, connection details) into
      Databricks widgets or a central config notebook/file
- [ ] Support running the same notebook against different environments
      (dev/prod) without editing code

## Explicitly Out of Scope (for now)

- CI/CD pipeline (GitHub Actions running tests on push) — valuable, but a
  distinct skill from data engineering itself; noted as a future direction
- Automated data quality alerting (e.g. Slack/email on rejection spike) —
  same reasoning