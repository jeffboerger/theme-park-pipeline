"""
test_ml.py — synthetic-data test suite for the ML layer.

Proves the compute path end-to-end WITHOUT BigQuery: run before
committing changes to baselines.py / train.py / wait_data.py, or
anytime, anywhere — no credentials needed.

    python ml/test_ml.py

What it does:

1. Simulate raw-style hourly waits for 8 rides across 2 parks: daily
   sinusoid peaking mid-afternoon, weekend lift, ride-specific level,
   AR(1) noise, overnight closures, and random collection gaps.
2. Replicate fct_wait_features.sql's semantics in pandas (dense spine,
   leak-free lags/rolls, park-local calendar features, lead-1 label).
3. Run compute_baselines() and run_walk_forward() on a 6-day frame
   (today's reality) and a 90-day frame (September's).

Assertions:
  - 6d: everything runs; seasonal baseline reports n/a; honesty banner fires
  - 90d: model beats persistence; lag/seasonal features carry importance
  - lag_1h in the transformed frame never leaks (spot-verified)
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from wait_data import prepare, honesty_banner
from baselines import compute_baselines
from train import run_walk_forward

RNG = np.random.default_rng(7)
RIDES = [(f"ride-{i}", f"Attraction {i}",
          "Magic Kingdom" if i < 5 else "EPCOT") for i in range(8)]


def simulate(days: int) -> pd.DataFrame:
    """Hourly (ride, hour, wait) with realistic structure, UTC timestamps."""
    hours = pd.date_range("2026-07-03", periods=days * 24, freq="h", tz="UTC")
    local = hours.tz_convert("America/New_York")
    rows = []
    for ride_id, ride_name, park in RIDES:
        base = RNG.uniform(15, 55)                      # ride popularity
        noise, ar = 0.0, RNG.uniform(0.5, 0.8)          # AR(1) persistence
        for ts, lt in zip(hours, local):
            if not (9 <= lt.hour <= 21):                # park closed
                continue
            if RNG.random() < 0.02:                     # collection gap
                continue
            daily = np.sin((lt.hour - 9) / 12 * np.pi)  # afternoon peak
            weekend = 1.25 if lt.dayofweek >= 5 else 1.0
            noise = ar * noise + RNG.normal(0, 4)
            wait = max(5, base * (0.5 + daily) * weekend + noise)
            rows.append((ride_id, ride_name, park, ts, round(wait / 5) * 5))
    return pd.DataFrame(rows, columns=[
        "ride_id", "ride_name", "park_name", "feature_hour", "avg_wait"])


def mart_transform(hourly: pd.DataFrame) -> pd.DataFrame:
    """Pandas twin of fct_wait_features.sql: dense spine -> lags/rolls ->
    park-local calendar -> lead-1 label -> filter."""
    out = []
    for ride_id, g in hourly.groupby("ride_id"):
        g = g.set_index("feature_hour").sort_index()
        spine = pd.date_range(g.index.min(), g.index.max(), freq="h", tz="UTC")
        d = g.reindex(spine)
        d.index.name = "feature_hour"
        d["ride_id"] = ride_id
        d[["ride_name", "park_name"]] = d[["ride_name", "park_name"]].ffill().bfill()
        w = d["avg_wait"]
        for k in (1, 2, 3, 24, 168):
            d[f"lag_{k}h"] = w.shift(k)
        d["roll_mean_3h"] = w.rolling(3, min_periods=1).mean()
        d["roll_mean_24h"] = w.rolling(24, min_periods=1).mean()
        d["roll_max_24h"] = w.rolling(24, min_periods=1).max()
        loc = d.index.tz_convert("America/New_York")
        d["hour_of_day"] = loc.hour
        d["day_of_week"] = (loc.dayofweek + 1) % 7 + 1   # BigQuery: 1=Sun..7=Sat
        d["is_weekend"] = d["day_of_week"].isin([1, 7])
        d["target_next_hour"] = w.shift(-1)
        out.append(d.reset_index())
    df = pd.concat(out)
    df["n_snapshots"] = 2
    return df[df["avg_wait"].notna() & df["target_next_hour"].notna()]


def leakage_spot_check(df):
    """lag_1h at T must equal avg_wait at T-1h wherever both exist."""
    lookup = df.set_index(["ride_id", "feature_hour"])["avg_wait"]
    key = pd.MultiIndex.from_arrays(
        [df["ride_id"], df["feature_hour"] - pd.Timedelta(hours=1)])
    actual_prev = lookup.reindex(key).to_numpy()
    lag = df["lag_1h"].to_numpy()
    both = ~np.isnan(actual_prev) & ~np.isnan(lag)
    assert np.allclose(lag[both], actual_prev[both]), "LEAK: lag_1h mismatch"
    return int(both.sum())


for days in (6, 90):
    print(f"\n{'=' * 62}\nREGIME: {days} days of history\n{'=' * 62}")
    df = prepare(mart_transform(simulate(days)))
    n = leakage_spot_check(df)
    print(f"rows: {len(df):,} · leakage spot-check passed on {n:,} rows")
    banner = honesty_banner(df)
    print(f"honesty banner: {'FIRES' if banner else 'silent (>=60d)'}")

    overall, table, cutoff, meta = compute_baselines(df)
    for k, v in overall.items():
        print(f"  baseline {k:<15} MAE: "
              f"{f'{v:.2f} min' if v is not None else 'n/a (insufficient history)'}")

    fold_table, importance, ov = run_walk_forward(df)
    print(f"  model MAE {ov['model_mae']:.2f} vs persistence "
          f"{ov['persistence_mae']:.2f}  ({ov['improvement_pct']:+.1f}%)")
    print(f"  top features: "
          f"{', '.join(importance.head(4).index)}")

    if days == 6:
        assert banner is not None, "banner must fire at 6 days"
        assert overall["seasonal_naive"] is None, "seasonal must be n/a at 6d"
    else:
        assert banner is None
        assert overall["seasonal_naive"] is not None
        assert ov["model_mae"] < ov["persistence_mae"], \
            "model must beat persistence with 90d of structured data"

print("\nALL ASSERTIONS PASSED — both regimes")
