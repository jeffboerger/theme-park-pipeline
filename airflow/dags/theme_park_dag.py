from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timezone
import sys
import os

sys.path.insert(0, "/Users/jeffboerger/Dev/theme-park-pipeline")

from etl.extract import fetch_wait_times
from etl.load import load_wait_times

def run_pipeline():
    """
    Entry point for the Airflow DAG task.
    Fetches live wait time data from the ThemeParks.wiki API
    and loads it into Snowflake.
    """
    wait_rows, forecast_rows = fetch_wait_times()
    load_wait_times(wait_rows, forecast_rows)

with DAG(
    dag_id="theme_park_wait_times",
    description="Hourly ingestion of Disney World wait times into Snowflake",
    schedule="@hourly",
    start_date=datetime(2026, 4, 6, tzinfo=timezone.utc),
    catchup=False,
    tags=["theme-park", "snowflake", "disney"]
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_wait_times",
        python_callable=run_pipeline
    )