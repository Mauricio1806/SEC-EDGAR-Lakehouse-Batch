# SEC EDGAR Lakehouse

![Python](https://img.shields.io/badge/Python-3.12-blue?style=flat-square&logo=python)
![Airflow](https://img.shields.io/badge/Airflow-2.10.4-orange?style=flat-square&logo=apache-airflow)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)
![AWS](https://img.shields.io/badge/AWS-S3-FF9900?style=flat-square&logo=amazonaws)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

> End-to-end data engineering pipeline collecting, processing and analyzing SEC EDGAR financial filings for 50 S&P 500 companies using a Medallion Lakehouse architecture.

**[Live Dashboard](http://mauricio-sec-edgar-lakehouse.s3-website-us-east-1.amazonaws.com)** · Python · Apache Airflow · Docker · AWS S3

---

## Architecture

Bronze: Raw filings + company facts — 92,776 records — 50 companies
Silver: Clean data + filing metrics + anomaly detection — 36,193 records
Gold:   Financial metrics + revenue growth YoY + compliance ranking
AWS S3: Object storage + static dashboard + free tier

---

## Key Metrics

| Metric | Value |
|--------|-------|
| Companies monitored | 50 S&P 500 |
| Total filings ingested | 92,776 |
| Clean records (Silver) | 36,193 |
| Financial data points | 3,155 |
| Revenue growth records | 455 |
| Anomalies detected | 39 |
| Pipeline duration | ~85s |
| Sectors covered | 8 |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.10.4 + LocalExecutor |
| Containerization | Docker + Docker Compose |
| Metadata DB | PostgreSQL 15 |
| ETL | Python 3.12 + Pandas + Requests |
| Storage | AWS S3 free tier |
| Data Source | SEC EDGAR REST API |
| Dashboard | HTML + Chart.js + S3 Static Website |

---

## Project Structure

    SEC_EDGAR_Lakehouse/
    +-- docker-compose.yml
    +-- requirements.txt
    +-- airflow/
    |   +-- dags/
    |       +-- sec_edgar_dag.py     # DAG v2 @daily 3 tasks XCom retry
    +-- src/
    |   +-- extract.py               # Bronze: 3 endpoints rate limiting
    |   +-- transform.py             # Silver: validation anomaly detection
    |   +-- load.py                  # Gold: financial metrics compliance
    |   +-- main.py                  # Local pipeline runner
    +-- dashboard/
    |   +-- index.html               # Live dashboard hosted on S3
    +-- data/
        +-- bronze/                  # Raw data gitignored
        +-- silver/                  # Clean data gitignored
        +-- gold/                    # Analytical outputs gitignored

---

## Pipeline Details

### Bronze Layer — extract.py
- Collects 3 SEC EDGAR endpoints per company: submissions, companyfacts, tickers
- Rate limiting: 0.12s per request to respect SEC API policy
- HTTP 429 handling with exponential backoff
- Saves raw JSON per company plus combined CSV
- Output: 92,776 filing records + 50 company fact files

### Silver Layer — transform.py
- Schema validation with null percentage reporting per column
- Deduplication and date type casting
- Filing frequency metrics: avg days between filings per company
- Anomaly detection: statistical gap analysis using mean + 2.5 sigma threshold
- Missing annual report detection per fiscal year
- Compliance score computed per company (0-100)
- Output: 36,193 clean records + 39 anomalies detected

### Gold Layer — load.py
- Financial metrics from XBRL companyfacts: Revenue, NetIncome, Assets, Equity
- Revenue growth YoY: year-over-year percentage change per company
- Sector analysis: filing volume by sector 2010-2024
- Compliance ranking: 50 companies scored and tiered
- Anomaly summary: aggregated with severity levels HIGH / MEDIUM
- Output: 5 analytical datasets ready for consumption

---

## How to Run

### Prerequisites
- Docker Desktop running
- Python 3.10+
- AWS CLI configured

### Setup

    git clone https://github.com/Mauricio1806/SEC-EDGAR-Lakehouse-Batch.git
    cd SEC-EDGAR-Lakehouse-Batch

    mkdir airflow\logs airflow\plugins data\bronze data\silver data\gold

    docker compose up airflow-init
    docker compose up -d

    pip install -r requirements.txt
    python src/main.py

### Access Airflow

    URL:      http://localhost:8081
    Username: admin
    Password: admin

### Deploy to AWS S3

    aws s3 sync data/bronze/ s3://YOUR-BUCKET/bronze/
    aws s3 sync data/silver/ s3://YOUR-BUCKET/silver/
    aws s3 sync data/gold/   s3://YOUR-BUCKET/gold/
    aws s3 cp dashboard/index.html s3://YOUR-BUCKET/index.html --content-type text/html

---

## SEC EDGAR API

| Endpoint | Usage |
|----------|-------|
| /submissions/CIK{cik}.json | Filing history per company |
| /api/xbrl/companyfacts/CIK{cik}.json | Financial facts Revenue Assets Equity |
| /files/company_tickers.json | Ticker to CIK mapping |

Rate limit: 10 requests per second. This pipeline uses 0.12s delay between requests.

---

## 👨‍💻 Author

**Mauricio Esquivel**
Data Engineer | Analytics Engineer

Focus: Lakehouse Architecture, Orchestration, Cloud Data Platforms
