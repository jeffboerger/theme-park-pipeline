"""
train.py — Phase 3: gradient boosting with walk-forward validation.

Validation is TIME-ORDERED ONLY. K expanding-window folds over the final
TEST_FRAC of the time range: each fold trains on everything before its
window and predicts the window — exactly how the deployed model will live
(train on the past, predict the next hour). Random splits on time-series
data leak the future into training; not making that mistake is the story.

Every fold also scores the persistence baseline on identical rows, so the
model-vs-baseline comparison is apples-to-apples.

Writes ml/RESULTS.md. Usage: python ml/train.py
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb

sys.path.insert(0, str(Path(__file__).parent))
from wait_data import (ALL_FEATURES, TARGET, history_days, honesty_banner,
                       load_features)

TEST_FRAC = 0.40   # final fraction of the time range used across test folds
N_FOLDS = 4
PARAMS = dict(
    n_estimators=300, learning_rate=0.05, max_depth=6,
    subsample=0.9, colsample_bytree=0.9, min_child_weight=5,
    objective="reg:absoluteerror",       # optimize the metric we report
    tree_method="hist", enable_categorical=True, random_state=42,
)


def walk_forward_folds(df, test_frac=TEST_FRAC, n_folds=N_FOLDS):
    """Yield (train_df, test_df, label) with strictly time-ordered splits."""
    hours = df["feature_hour"]
    start, end = hours.min(), hours.max()
    test_start = start + (end - start) * (1 - test_frac)
    edges = pd.date_range(test_start, end, periods=n_folds + 1)
    for i in range(n_folds):
        lo, hi = edges[i], edges[i + 1]
        train = df[hours < lo]
        test = df[(hours >= lo) & (hours < hi)] if i < n_folds - 1 \
            else df[(hours >= lo) & (hours <= hi)]
        if len(train) and len(test):
            yield train, test, f"{lo:%m-%d %H:%M} → {hi:%m-%d %H:%M}"


def run_walk_forward(df):
    """Returns (fold_table, importance_series, overall_dict)."""
    rows, importances = [], []
    all_pred, all_persist, all_y = [], [], []

    for train, test, label in walk_forward_folds(df):
        model = xgb.XGBRegressor(**PARAMS)
        model.fit(train[ALL_FEATURES], train[TARGET])
        pred = model.predict(test[ALL_FEATURES])
        y = test[TARGET].to_numpy()
        persist = test["avg_wait"].to_numpy()

        rows.append({
            "fold": label, "train_rows": len(train), "test_rows": len(test),
            "model_mae": float(np.mean(np.abs(pred - y))),
            "persistence_mae": float(np.mean(np.abs(persist - y))),
        })
        importances.append(pd.Series(
            model.feature_importances_, index=ALL_FEATURES))
        all_pred.append(pred); all_persist.append(persist); all_y.append(y)

    if not rows:
        raise SystemExit("Not enough data to form even one walk-forward fold.")

    y = np.concatenate(all_y)
    overall = {
        "model_mae": float(np.mean(np.abs(np.concatenate(all_pred) - y))),
        "persistence_mae": float(np.mean(np.abs(np.concatenate(all_persist) - y))),
        "test_rows": int(len(y)),
    }
    overall["improvement_pct"] = 100 * (1 - overall["model_mae"]
                                        / overall["persistence_mae"])
    fold_table = pd.DataFrame(rows).round(2)
    importance = (pd.concat(importances, axis=1).mean(axis=1)
                    .sort_values(ascending=False).round(4))
    return fold_table, importance, overall


def log_run_to_bq(fold_table, overall, history_d) -> str | None:
    """Append this run's metrics to BigQuery so results are queryable
    history, not just markdown. One row per fold plus an OVERALL row.
    Lands in the raw dataset (app-written, like raw_wait_times); dbt can
    model on top of it later. Never fatal — metrics logging must not
    kill a training run."""
    import os
    from datetime import datetime, timezone
    try:
        from google.cloud import bigquery
        from wait_data import PROJECT
        dataset = os.getenv("BQ_RAW_DATASET", "raw")
        table_id = f"{PROJECT}.{dataset}.ml_training_runs"
        run_at = datetime.now(timezone.utc)

        rows = fold_table.copy()
        rows["fold"] = rows["fold"].astype(str)
        overall_row = pd.DataFrame([{
            "fold": "OVERALL", "train_rows": int(fold_table["train_rows"].sum()),
            "test_rows": overall["test_rows"],
            "model_mae": overall["model_mae"],
            "persistence_mae": overall["persistence_mae"],
        }])
        rows = pd.concat([rows, overall_row], ignore_index=True)
        rows.insert(0, "run_at", run_at)
        rows["history_days"] = float(history_d)
        rows["model_version"] = "xgb-walkforward-v1"

        client = bigquery.Client(project=PROJECT)
        job = client.load_table_from_dataframe(
            rows, table_id,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema_update_options=["ALLOW_FIELD_ADDITION"]))
        job.result()
        return table_id
    except Exception as e:
        print(f"  ! BigQuery metrics logging skipped: {e}")
        return None


def main():
    df = load_features()
    banner = honesty_banner(df)
    fold_table, importance, overall = run_walk_forward(df)

    beat = overall["model_mae"] < overall["persistence_mae"]
    verdict = (f"Model beats persistence by {overall['improvement_pct']:.1f}% "
               f"({overall['model_mae']:.2f} vs "
               f"{overall['persistence_mae']:.2f} min MAE)."
               if beat else
               f"Model does NOT beat persistence "
               f"({overall['model_mae']:.2f} vs "
               f"{overall['persistence_mae']:.2f} min MAE) — a lesson, "
               f"not a bullet. Likely insufficient history for the lag "
               f"features to carry signal.")

    lines = ["# Model — walk-forward validation (Phase 3)", ""]
    if banner:
        lines += [f"> **{banner}**", ""]
    lines += [
        f"History: **{history_days(df):.1f} days** · "
        f"gradient boosting (XGBoost, MAE objective) · "
        f"{len(fold_table)} expanding-window folds over the final "
        f"{int(TEST_FRAC * 100)}% of the time range · "
        f"{overall['test_rows']:,} total test rows", "",
        f"**{verdict}**", "",
        "## Per-fold comparison", "",
        fold_table.to_markdown(index=False), "",
        "## Feature importance (mean across folds)", "",
        importance.to_frame("importance").to_markdown(), "",
    ]
    out = Path(__file__).parent / "RESULTS.md"
    out.write_text("\n".join(lines) + "\n")
    print(f"{verdict}\n")
    print(fold_table.to_string(index=False))
    print(f"\nWrote {out}")

    logged = log_run_to_bq(fold_table, overall, history_days(df))
    if logged:
        print(f"Metrics appended to {logged} "
              f"(query run history: SELECT * FROM `{logged}` ORDER BY run_at)")


if __name__ == "__main__":
    main()
