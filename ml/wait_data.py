"""
wait_data.py — shared loader + feature contract for the wait-time model.

Everything downstream (baselines.py, train.py) takes a plain DataFrame, so
compute logic is testable without a warehouse. Only load_features() touches
BigQuery.
"""
import os

from dotenv import load_dotenv

load_dotenv()

PROJECT = os.getenv("GCP_PROJECT")
DBT_DATASET = os.getenv("BQ_DBT_DATASET", "dbt")

# The model's feature contract. avg_wait is the current-hour value (lag 0
# relative to the label); target_next_hour is the ONLY forward-looking
# column and is never in this list.
NUMERIC_FEATURES = [
    "avg_wait", "lag_1h", "lag_2h", "lag_3h", "lag_24h", "lag_168h",
    "roll_mean_3h", "roll_mean_24h", "roll_max_24h",
    "hour_of_day", "day_of_week",
]
BOOL_FEATURES = ["is_weekend"]
CATEGORICAL_FEATURES = ["ride_id", "park_name"]
ALL_FEATURES = NUMERIC_FEATURES + BOOL_FEATURES + CATEGORICAL_FEATURES
TARGET = "target_next_hour"


def load_features():
    """Pull fct_wait_features from BigQuery into a DataFrame."""
    from google.cloud import bigquery
    if not PROJECT:
        raise SystemExit("GCP_PROJECT is not set — check your .env")
    client = bigquery.Client(project=PROJECT)
    sql = f"""
        SELECT * FROM `{PROJECT}.{DBT_DATASET}.fct_wait_features`
        ORDER BY feature_hour, ride_id
    """
    df = client.query(sql).result().to_dataframe()
    return prepare(df)


def prepare(df):
    """Normalize dtypes; safe to call on warehouse or synthetic frames."""
    df = df.copy()
    df["feature_hour"] = df["feature_hour"].astype("datetime64[us, UTC]")
    df["is_weekend"] = df["is_weekend"].astype(bool)
    for c in CATEGORICAL_FEATURES:
        df[c] = df[c].astype("category")
    return df.sort_values(["feature_hour", "ride_id"]).reset_index(drop=True)


def history_days(df) -> float:
    span = df["feature_hour"].max() - df["feature_hour"].min()
    return span.total_seconds() / 86400


def honesty_banner(df) -> str | None:
    """The roadmap rule, encoded: numbers from short history are plumbing
    proof, not performance claims."""
    d = history_days(df)
    if d < 60:
        return (f"!! ONLY {d:.1f} DAYS OF HISTORY (threshold: 60). These "
                f"numbers prove the pipeline runs; they are NOT claimable "
                f"performance metrics. Re-run when the warehouse matures.")
    return None
