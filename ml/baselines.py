"""
baselines.py — Phase 1: the numbers any model must beat.

Three baselines for next-hour wait, evaluated on a time-ordered holdout
(never random — that's the leakage mistake this project exists to not make):

  persistence     predict T+1h = the wait right now. Embarrassingly strong
                  for 1-hour horizons; the true opponent.
  hod_mean        predict T+1h = this ride's mean wait at that hour-of-day,
                  computed on TRAINING rows only.
  seasonal_naive  predict T+1h = the wait exactly one week earlier.
                  Needs >7 days of history; reports n/a until it has it.

Writes ml/BASELINES.md. Usage: python ml/baselines.py
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from wait_data import TARGET, history_days, honesty_banner, load_features

HOLDOUT_FRAC = 0.25       # final fraction of the time range held out
MIN_SEASONAL_MATCH = 0.30 # below this holdout match rate, seasonal = n/a


def time_split(df, holdout_frac=HOLDOUT_FRAC):
    """Split on TIME, not rows: every ride's holdout covers the same
    final window, mimicking deployment."""
    hours = df["feature_hour"]
    cutoff = hours.min() + (hours.max() - hours.min()) * (1 - holdout_frac)
    return df[hours <= cutoff], df[hours > cutoff], cutoff


def compute_baselines(df, holdout_frac=HOLDOUT_FRAC):
    """Returns (overall_results, per_ride_table, cutoff). Pure pandas —
    testable on any frame with the mart's columns."""
    train, test, cutoff = time_split(df, holdout_frac)
    if len(test) == 0 or len(train) == 0:
        raise SystemExit("Not enough data to form a train/holdout split.")
    y = test[TARGET].to_numpy()
    preds = {}

    # 1. persistence: current wait carries forward
    preds["persistence"] = test["avg_wait"].to_numpy()

    # 2. hour-of-day mean per ride, TRAIN ONLY (ride mean as fallback)
    hod = (train.groupby(["ride_id", "hour_of_day"], observed=True)[TARGET]
                .mean().rename("hod_pred"))
    ride_mean = train.groupby("ride_id", observed=True)[TARGET].mean()
    t = test.join(hod, on=["ride_id", "hour_of_day"])
    t["hod_pred"] = t["hod_pred"].fillna(t["ride_id"].map(ride_mean))
    preds["hod_mean"] = t["hod_pred"].to_numpy()

    # 3. seasonal naive: value at (T+1h) - 168h == avg_wait at T - 167h.
    #    Self-merge on the shifted timestamp; sparse history -> few matches.
    lookup = df.set_index(["ride_id", "feature_hour"])["avg_wait"]
    key = pd.MultiIndex.from_arrays(
        [test["ride_id"], test["feature_hour"] - pd.Timedelta(hours=167)])
    seasonal = lookup.reindex(key).to_numpy()
    match_rate = float(np.mean(~np.isnan(seasonal)))
    preds["seasonal_naive"] = seasonal if match_rate >= MIN_SEASONAL_MATCH else None

    overall, per_ride = {}, {}
    for name, p in preds.items():
        if p is None:
            overall[name] = None
            continue
        mask = ~np.isnan(p)
        overall[name] = float(np.mean(np.abs(p[mask] - y[mask])))
        e = pd.Series(np.abs(p - y), index=test.index)
        per_ride[name] = e.groupby(test["ride_name"], observed=True).mean()

    table = pd.DataFrame(per_ride).round(2).sort_values(
        "persistence") if per_ride else pd.DataFrame()
    meta = {"holdout_rows": len(test), "train_rows": len(train),
            "seasonal_match_rate": match_rate,
            "history_days": history_days(df)}
    return overall, table, cutoff, meta


def main():
    df = load_features()
    overall, table, cutoff, meta = compute_baselines(df)
    banner = honesty_banner(df)

    lines = ["# Baselines — next-hour wait prediction (Phase 1)", ""]
    if banner:
        lines += [f"> **{banner}**", ""]
    lines += [
        f"History: **{meta['history_days']:.1f} days** · "
        f"train rows: {meta['train_rows']:,} · "
        f"holdout rows: {meta['holdout_rows']:,} "
        f"(time-ordered, cutoff {cutoff:%Y-%m-%d %H:%M UTC})", "",
        "| Baseline | Holdout MAE (min) |", "|---|---|",
    ]
    for name, mae in overall.items():
        val = (f"{mae:.2f}" if mae is not None else
               f"n/a — only {meta['seasonal_match_rate']:.0%} of holdout has "
               f"a same-hour-last-week match (needs >7 days of history)")
        lines.append(f"| {name} | {val} |")
    lines += ["", "## Per-attraction MAE (minutes)", "",
              table.to_markdown() if not table.empty else "_no rows_", "",
              "These are the numbers to beat. A model that can't beat "
              "persistence is a lesson, not a bullet."]

    out = Path(__file__).parent / "BASELINES.md"
    out.write_text("\n".join(lines) + "\n")
    print("\n".join(lines[:14]))
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
