from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timezone
import sys
import os
from dotenv import load_dotenv

# Find .env relative to this DAG file — works on any machine
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_HOME = os.path.dirname(os.path.dirname(DAG_DIR))
load_dotenv(os.path.join(PROJECT_HOME, ".env"))

# print(f"DAG_DIR: {DAG_DIR}")
# print(f"PROJECT_HOME: {PROJECT_HOME}")

sys.path.insert(0, PROJECT_HOME)

from etl.extract import fetch_wait_times, fetch_weather
from etl.load import load_wait_times, load_weather

def run_pipeline():
    """
    Fetches live wait time data from the ThemeParks.wiki API
    and loads it into Snowflake.
    """
    wait_rows, forecast_rows = fetch_wait_times()
    load_wait_times(wait_rows, forecast_rows)

def run_weather():
    """
    Fetches current weather conditions for Orlando
    and loads into Snowflake.
    """
    weather_row = fetch_weather()
    load_weather(weather_row)

with DAG(
    dag_id="theme_park_wait_times",
    description="Hourly ingestion of Disney World wait times and Orlando weather into Snowflake",
    schedule="@hourly",
    start_date=datetime(2026, 4, 6, tzinfo=timezone.utc),
    catchup=False,
    tags=["theme-park", "snowflake", "disney", "weather"]
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_wait_times",
        python_callable=run_pipeline
    )

    weather_task = PythonOperator(
        task_id="ingest_weather",
        python_callable=run_weather
    )

    dbt_task = BashOperator(
        task_id="run_dbt_models",
        bash_command=f"cd {PROJECT_HOME}/theme_park_dbt && {PROJECT_HOME}/venv/bin/dbt run"
    )

    [ingest_task, weather_task] >> dbt_task