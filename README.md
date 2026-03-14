# SEC EDGAR Lakehouse Batch (Bronze → Silver → Gold)
DuckDB + Parquet + Airflow + dbt + Great Expectations  
AWS-Ready Architecture (S3 + Athena | us-east-2 | Free Tier Safe)

## Overview
This repository implements a portfolio-grade Lakehouse Batch architecture using clear Bronze, Silver, and Gold layers.
It is designed to:
- Demonstrate real-world Data Engineering patterns used in US-focused environments
- Be fully reproducible locally (Docker-based execution)
- Map directly to AWS services (S3 + Athena) while staying Free Tier safe
- Use a live, official US data source with continuous updates (SEC EDGAR)

## Real-World Dataset (Always Current)
SEC EDGAR (Submissions API + optional XBRL facts)
- Official EDGAR developer resources confirm JSON APIs on data.sec.gov.
- Scripted access is allowed with fair-access guidance and request rate limits.

Why EDGAR:
- US-market relevance (finance, analytics, compliance workloads)
- Real operational complexity: incremental ingestion, schema drift, normalization
- Strong “enterprise” signal for portfolio work

## Architecture (Logical Flow)
SEC EDGAR (latest pulls)
  → Raw (downloaded JSON)
  → Bronze (raw-ish Parquet, partitioned by ingest_date)
  → Great Expectations quality gate
  → dbt transformations → Silver (conformed)
  → dbt marts → Gold (analytics tables)
  → DuckDB locally + Athena externally (Phase 1)

## Local Technology Stack
- Apache Airflow (orchestration)
- DuckDB (local analytical engine)
- Parquet + Snappy (columnar storage)
- dbt-duckdb (transformations + tests)
- Great Expectations (data validation gate)
- Docker + Make (reproducible runtime)

## AWS Cloud Alignment (Phase 1 | Free Tier Safe)
**Region:** us-east-2  
**Guardrails:**
- Use S3 + Athena only (no always-on services)
- Partitioned data to reduce Athena scan costs
- Optional: AWS Budgets alert at $1

Local → AWS mapping:
- Local Parquet lake → S3 (bronze/silver/gold prefixes)
- DuckDB queries → Athena external tables on Gold
- Airflow (local) → MWAA (conceptual mapping, not deployed in Phase 1)
- dbt + GE → Glue jobs (optional Phase 2)

## Repository Structure (Target)
sec-edgar-lakehouse-bsg/
  README.md
  RUNBOOK.md
  Makefile
  docker-compose.yml
  requirements.txt

  data/
    raw/
    bronze/
    silver/
    gold/
    lakehouse.duckdb

  src/
    ingest/
      edgar_submissions_download.py
      edgar_to_bronze.py
    ge/
      ge_run.py
    utils/
      config.py
      paths.py

  airflow/
    dags/
      edgar_lakehouse_bsg_dag.py

  dbt/
    lakehouse_dbt/
      dbt_project.yml
      profiles.yml.example
      models/
        silver/
          stg_companies.sql
          stg_filings.sql
        gold/
          mart_filings_daily.sql
      tests/
        schema.yml

  cloud/
    aws/
      README.md
      athena/
        setup.sql
        gold_tables.sql
        queries.sql
      iam/
        local_user_policy.json

  scripts/
    sync_to_s3.ps1

## Expected Outputs
Local:
- Raw EDGAR JSON under `data/raw/edgar/`
- Bronze Parquet partitioned by ingest_date under `data/bronze/edgar/`
- DuckDB database: `data/lakehouse.duckdb`
- Gold example mart: `mart_filings_daily`

AWS (Phase 1):
- S3 lake storage (bronze/silver/gold prefixes)
- Athena external tables over Gold
- Queryable analytics in Athena console

## Quickstart (Local)
```bash
make up
# Airflow UI: http://localhost:8080 (admin/admin)

make run-local
make logs
