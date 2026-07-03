"""Creates the raw BigQuery tables for the theme park pipeline.

Replaces the Snowflake setup scripts (setup_weather_table.py and whatever
created raw_wait_times / raw_forecast). Run once after creating the datasets.

Requires env vars:
  GOOGLE_APPLICATION_CREDENTIALS  -> path to service account JSON
  GCP_PROJECT                     -> your GCP project id
  BQ_RAW_DATASET                  -> defaults to "raw"
"""
import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

PROJECT = os.getenv("GCP_PROJECT")
RAW_DATASET = os.getenv("BQ_RAW_DATASET", "raw")

client = bigquery.Client(project=PROJECT)


def create_dataset(name):
    dataset_id = f"{PROJECT}.{name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"  # keep raw + analytics in the same location
    client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset ready: {dataset_id}")


def create_table(table_name, schema):
    table_id = f"{PROJECT}.{RAW_DATASET}.{table_name}"
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)
    print(f"Table ready: {table_id}")


# Datasets: raw for ingestion, analytics for dbt output
create_dataset(RAW_DATASET)
create_dataset(os.getenv("BQ_ANALYTICS_DATASET", "analytics"))

# raw_wait_times — one row per attraction per hourly snapshot
create_table("raw_wait_times", [
    bigquery.SchemaField("ride_id", "STRING"),
    bigquery.SchemaField("ride_name", "STRING"),
    bigquery.SchemaField("park_id", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("standby_wait", "INTEGER"),
    bigquery.SchemaField("lightning_lane_state", "STRING"),
    bigquery.SchemaField("lightning_lane_return_start", "STRING"),
    bigquery.SchemaField("collected_at", "TIMESTAMP"),
])

# raw_forecast — one row per forecasted hour per attraction per snapshot
create_table("raw_forecast", [
    bigquery.SchemaField("ride_id", "STRING"),
    bigquery.SchemaField("ride_name", "STRING"),
    bigquery.SchemaField("park_id", "STRING"),
    bigquery.SchemaField("forecasted_time", "TIMESTAMP"),
    bigquery.SchemaField("wait_time", "INTEGER"),
    bigquery.SchemaField("percentage", "FLOAT"),
    bigquery.SchemaField("collected_at", "TIMESTAMP"),
])

# raw_weather — one row per weather snapshot
create_table("raw_weather", [
    bigquery.SchemaField("collected_at", "TIMESTAMP"),
    bigquery.SchemaField("temperature_f", "FLOAT"),
    bigquery.SchemaField("humidity_pct", "INTEGER"),
    bigquery.SchemaField("precipitation_mm", "FLOAT"),
    bigquery.SchemaField("weather_code", "INTEGER"),
    bigquery.SchemaField("wind_speed_kmh", "FLOAT"),
])

print("All raw tables created.")
