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
- Official developer resources confirm EDGAR JSON APIs on data.sec.gov.  
- Scripted access is allowed, with fair-access guidance and request rate limits.

Why EDGAR:
- US-market relevance (finance, analytics, compliance workloads)
- Real operational complexity: incremental ingestion, schema drift, normalization
- Strong “enterprise” signal for portfolio work

## Architecture – Logical Flow
SEC EDGAR (latest pulls) 
  → Raw (downloaded JSON)
  → Bronze (raw-ish Parquet, partitioned by ingest_date)
  → Great Expectations quality gate
  → dbt transformations → Silver (conformed)
  → dbt marts → Gold (analytics tables)
  → DuckDB locally + Athena externally (Phase 1)

## Local Stack
- Apache Airflow (Orchestration)
- DuckDB (Local analytical engine)
- Parquet + Snappy (Columnar storage)
- dbt-duckdb (Transformations + tests)
- Great Expectations (Data validation gate)
- Docker + Make (Reproducible runtime)

## AWS Cloud Alignment (Phase 1)
- Amazon S3: Bronze/Silver/Gold storage layout
- Amazon Athena: query Gold layer via external tables (SQL versioned in repo)
- AWS IAM: least-privilege policy docs included

## Quickstart
```bash
make up
# Airflow UI: http://localhost:8080 (admin/admin)

make run-local
make logs
