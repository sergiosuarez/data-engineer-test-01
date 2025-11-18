"""Airflow DAG that triggers the Airbnb analytics pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def _run_orchestrator() -> None:
    """Invoke the Python orchestrator entrypoint."""

    from src.pipeline.orchestrator import run_pipeline

    run_pipeline()


default_args = {
    "owner": "data-eng",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="airbnb_etl_pipeline",
    description="Daily ETL for Airbnb analytics model",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["airbnb", "etl", "analytics"],
) as dag:
    run_full_pipeline = PythonOperator(
        task_id="run_orchestrator_pipeline",
        python_callable=_run_orchestrator,
    )

    run_full_pipeline
