import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

# Auth comes from GOOGLE_APPLICATION_CREDENTIALS (path to service-account JSON).
# Set this in your .env locally and as a GitHub secret in CI.
PROJECT = os.getenv("GCP_PROJECT")
RAW_DATASET = os.getenv("BQ_RAW_DATASET", "raw")


def get_client():
    """Returns an authenticated BigQuery client.

    Credentials are read automatically from the file path in the
    GOOGLE_APPLICATION_CREDENTIALS environment variable.
    """
    return bigquery.Client(project=PROJECT)


def _table_id(table_name):
    return f"{PROJECT}.{RAW_DATASET}.{table_name}"


# Column order MUST match the tuple order coming out of etl/extract.py.
# If you change the SELECT/return order in extract.py, update these lists.
WAIT_COLUMNS = [
    "ride_id", "ride_name", "park_id", "status", "standby_wait",
    "lightning_lane_state", "lightning_lane_return_start", "collected_at",
]

FORECAST_COLUMNS = [
    "ride_id", "ride_name", "park_id", "forecasted_time",
    "wait_time", "percentage", "collected_at",
]

WEATHER_COLUMNS = [
    "collected_at", "temperature_f", "humidity_pct",
    "precipitation_mm", "weather_code", "wind_speed_kmh",
]


def _rows_to_dicts(rows, columns):
    """Converts positional tuples into column-keyed dicts for BigQuery.

    Timestamps are serialized to ISO strings so JSON load jobs accept them.
    """
    dicts = []
    for row in rows:
        record = {}
        for col, val in zip(columns, row):
            if hasattr(val, "isoformat"):   # datetime -> ISO string
                val = val.isoformat()
            record[col] = val
        dicts.append(record)
    return dicts


def _load(rows, columns, table_name):
    """Inserts rows via a load job (free tier, unlimited) rather than
    the streaming insert API (not free)."""
    if not rows:
        print(f"No rows to load into {table_name}, skipping.")
        return

    client = get_client()
    records = _rows_to_dicts(rows, columns)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # Table already exists (created by your setup script), so don't autodetect —
        # rely on the existing schema. Set autodetect=True only for first load.
        autodetect=False,
    )

    job = client.load_table_from_json(
        records, _table_id(table_name), job_config=job_config
    )
    job.result()  # wait for completion; raises on failure
    print(f"Loaded {len(records)} rows into {table_name}")


def load_wait_times(wait_rows, forecast_rows):
    """Loads current wait-time snapshots and hourly forecasts into BigQuery."""
    _load(wait_rows, WAIT_COLUMNS, "raw_wait_times")
    _load(forecast_rows, FORECAST_COLUMNS, "raw_forecast")


def load_weather(weather_row):
    """Loads a single weather snapshot into BigQuery.

    fetch_weather() returns one tuple, so wrap it in a list.
    """
    _load([weather_row], WEATHER_COLUMNS, "raw_weather")
