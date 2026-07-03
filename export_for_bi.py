"""Exports analytics marts to CSV for Tableau/Power BI.

Replaces the Snowflake export scripts. Uses the same auth as the rest of the
pipeline (GOOGLE_APPLICATION_CREDENTIALS + GCP_PROJECT).
"""
import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

PROJECT = os.getenv("GCP_PROJECT")
ANALYTICS = os.getenv("BQ_ANALYTICS_DATASET", "analytics")

client = bigquery.Client(project=PROJECT)

tables = [
    "mart_wait_times_by_park",
    "mart_wait_times_by_ride",
    "mart_weather_vs_wait_times",
]

for table in tables:
    df = client.query(f"SELECT * FROM `{PROJECT}.{ANALYTICS}.{table}`").to_dataframe()
    df.to_csv(f"{table}.csv", index=False)
    print(f"Exported {len(df)} rows from {table}")

print("Done.")
