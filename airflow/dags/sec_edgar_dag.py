"""
SEC EDGAR Lakehouse — Airflow DAG v2
Bronze -> Silver -> Gold | 50 companies | @daily
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys, logging

sys.path.insert(0, "/opt/airflow/src")
logger = logging.getLogger(__name__)

default_args = {
    "owner":           "airflow",
    "retries":         3,
    "retry_delay":     timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}


def task_extract(**context):
    from extract import run_extract
    result = run_extract()
    if not result:
        raise ValueError("Bronze extraction failed — no output file")
    context["ti"].xcom_push(key="bronze_path", value=result)
    return result


def task_transform(**context):
    from transform import run_transform
    result = run_transform()
    context["ti"].xcom_push(key="silver_path", value=result)
    return result


def task_load(**context):
    from load import run_load
    result = run_load()
    context["ti"].xcom_push(key="gold_path", value=result)
    return result


with DAG(
    dag_id="sec_edgar_pipeline_v2",
    description="SEC EDGAR Lakehouse — 50 companies, financial metrics, anomaly detection",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["sec", "edgar", "lakehouse", "financial", "s&p500"],
    doc_md="""
    ## SEC EDGAR Lakehouse Pipeline v2

    Collects financial data for 50 S&P 500 companies from SEC EDGAR API.

    ### Layers
    - **Bronze**: Raw submissions + company facts (revenue, assets, equity)
    - **Silver**: Clean data + filing metrics + anomaly detection
    - **Gold**: Financial metrics, revenue growth YoY, sector analysis, compliance ranking
    """,
) as dag:

    extract = PythonOperator(
        task_id="extract_bronze",
        python_callable=task_extract,
        execution_timeout=timedelta(minutes=15),
    )

    transform = PythonOperator(
        task_id="transform_silver",
        python_callable=task_transform,
        execution_timeout=timedelta(minutes=5),
    )

    load = PythonOperator(
        task_id="load_gold",
        python_callable=task_load,
        execution_timeout=timedelta(minutes=5),
    )

    extract >> transform >> load
