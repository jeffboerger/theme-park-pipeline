"""
Phase 0 — Data audit for the wait-time prediction model.

Answers, from BigQuery, every question on the roadmap's Phase 0 checklist:
  1. Row counts: total snapshots, per attraction, date range
  2. Gap analysis: hours with no data at all (collection failures / the
     Snowflake->BigQuery migration seam) vs. hours where the parks were
     simply closed (rows exist, but nothing OPERATING)
  3. Usable training volume: OPERATING rows with a non-null standby_wait
  4. A proceed/pause verdict against the roadmap's ~2 month threshold

Writes DATA_AUDIT.md to the repo root.

Run from the repo root (same env setup as the rest of the pipeline):
    python ml/data_audit.py

Requires GOOGLE_APPLICATION_CREDENTIALS and GCP_PROJECT in .env,
exactly like etl/load.py.
"""

import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

PROJECT = os.getenv("GCP_PROJECT")
RAW_DATASET = os.getenv("BQ_RAW_DATASET", "raw")
TABLE = f"`{PROJECT}.{RAW_DATASET}.raw_wait_times`"

# Roadmap rule: if usable history is under ~2 months, pause and keep collecting.
MIN_USABLE_DAYS = 60

# Inclusion rule candidate: a ride must appear in at least this fraction of
# park-open hours to make the training set. Tune after seeing the numbers.
MIN_COVERAGE_PCT = 50.0


def q(client: bigquery.Client, sql: str) -> list[dict]:
    """Run a query and return rows as plain dicts."""
    return [dict(row) for row in client.query(sql).result()]


# ---------------------------------------------------------------------------
# Queries. Each maps to one checklist item. All operate on the raw table so
# the audit is independent of dbt build state.
# ---------------------------------------------------------------------------

OVERVIEW_SQL = f"""
SELECT
  COUNT(*)                                   AS total_rows,
  COUNT(DISTINCT ride_id)                    AS distinct_rides,
  COUNT(DISTINCT park_id)                    AS distinct_parks,
  MIN(collected_at)                          AS first_snapshot,
  MAX(collected_at)                          AS last_snapshot,
  COUNT(DISTINCT TIMESTAMP_TRUNC(collected_at, HOUR)) AS distinct_hours,
  COUNTIF(status = 'OPERATING' AND standby_wait IS NOT NULL) AS usable_rows
FROM {TABLE}
"""

# One row per calendar hour between first and last snapshot, classified as:
#   COLLECTED  - rows exist for that hour
#   MISSING    - no rows at all (Actions failure, outage, migration seam)
# Within COLLECTED hours we also note whether anything was OPERATING, which
# separates real signal hours from overnight/closure hours.
HOUR_SPINE_SQL = f"""
WITH bounds AS (
  SELECT
    TIMESTAMP_TRUNC(MIN(collected_at), HOUR) AS start_h,
    TIMESTAMP_TRUNC(MAX(collected_at), HOUR) AS end_h
  FROM {TABLE}
),
spine AS (
  SELECT h
  FROM bounds, UNNEST(GENERATE_TIMESTAMP_ARRAY(start_h, end_h, INTERVAL 1 HOUR)) AS h
),
per_hour AS (
  SELECT
    TIMESTAMP_TRUNC(collected_at, HOUR) AS h,
    COUNT(*) AS rows_in_hour,
    COUNTIF(status = 'OPERATING' AND standby_wait IS NOT NULL) AS operating_rows
  FROM {TABLE}
  GROUP BY 1
)
SELECT
  spine.h,
  IFNULL(per_hour.rows_in_hour, 0)  AS rows_in_hour,
  IFNULL(per_hour.operating_rows, 0) AS operating_rows
FROM spine
LEFT JOIN per_hour USING (h)
ORDER BY spine.h
"""

# Per-attraction coverage, measured against hours where the park as a whole
# had OPERATING rides (so overnight hours don't penalize anyone).
PER_RIDE_SQL = f"""
WITH open_hours AS (
  SELECT
    park_id,
    TIMESTAMP_TRUNC(collected_at, HOUR) AS h
  FROM {TABLE}
  WHERE status = 'OPERATING' AND standby_wait IS NOT NULL
  GROUP BY 1, 2
),
park_open_counts AS (
  SELECT park_id, COUNT(*) AS park_open_hours
  FROM open_hours
  GROUP BY 1
),
ride_hours AS (
  SELECT
    ride_id,
    ANY_VALUE(ride_name) AS ride_name,
    park_id,
    COUNT(DISTINCT TIMESTAMP_TRUNC(collected_at, HOUR)) AS ride_operating_hours,
    COUNTIF(status = 'OPERATING' AND standby_wait IS NOT NULL) AS usable_rows,
    MIN(collected_at) AS first_seen,
    MAX(collected_at) AS last_seen
  FROM {TABLE}
  WHERE status = 'OPERATING' AND standby_wait IS NOT NULL
  GROUP BY ride_id, park_id
)
SELECT
  r.ride_name,
  r.park_id,
  r.usable_rows,
  r.ride_operating_hours,
  p.park_open_hours,
  ROUND(100 * r.ride_operating_hours / p.park_open_hours, 1) AS coverage_pct,
  r.first_seen,
  r.last_seen
FROM ride_hours r
JOIN park_open_counts p USING (park_id)
ORDER BY coverage_pct DESC, usable_rows DESC
"""

STATUS_SQL = f"""
SELECT
  status,
  COUNT(*) AS rows_,
  COUNTIF(standby_wait IS NULL) AS null_wait_rows
FROM {TABLE}
GROUP BY status
ORDER BY rows_ DESC
"""

PARK_NAMES = {
    "75ea578a-adc8-4116-a54d-dccb60765ef9": "Magic Kingdom",
    "47f90d2c-e191-4239-a466-5892ef59a88b": "EPCOT",
    "288747d1-8b4f-4a64-867e-ea7c9b27bad8": "Hollywood Studios",
    "1c84a229-8862-4648-9c71-378ddd2c7693": "Animal Kingdom",
}


def find_gaps(hours: list[dict]) -> list[dict]:
    """Group consecutive zero-row hours into gap windows."""
    gaps = []
    current = None
    for row in hours:
        if row["rows_in_hour"] == 0:
            if current is None:
                current = {"start": row["h"], "end": row["h"], "hours": 1}
            else:
                current["end"] = row["h"]
                current["hours"] += 1
        elif current is not None:
            gaps.append(current)
            current = None
    if current is not None:
        gaps.append(current)
    return sorted(gaps, key=lambda g: -g["hours"])


def main() -> None:
    if not PROJECT:
        raise SystemExit("GCP_PROJECT is not set — check your .env")

    client = bigquery.Client(project=PROJECT)

    overview = q(client, OVERVIEW_SQL)[0]
    hours = q(client, HOUR_SPINE_SQL)
    rides = q(client, PER_RIDE_SQL)
    statuses = q(client, STATUS_SQL)

    gaps = find_gaps(hours)
    total_spine_hours = len(hours)
    missing_hours = sum(1 for h in hours if h["rows_in_hour"] == 0)
    signal_hours = sum(1 for h in hours if h["operating_rows"] > 0)

    history_days = (overview["last_snapshot"] - overview["first_snapshot"]).days
    verdict_proceed = history_days >= MIN_USABLE_DAYS

    included = [r for r in rides if r["coverage_pct"] >= MIN_COVERAGE_PCT]
    excluded = [r for r in rides if r["coverage_pct"] < MIN_COVERAGE_PCT]

    lines = []
    add = lines.append

    add("# Data Audit — Wait-Time Prediction (Phase 0)")
    add(f"\n*Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC} "
        f"by `ml/data_audit.py` against `{RAW_DATASET}.raw_wait_times`.*\n")

    add("## Verdict\n")
    if verdict_proceed:
        add(f"**PROCEED.** {history_days} days of history "
            f"(threshold: {MIN_USABLE_DAYS}). Phase 1 (baselines) is unblocked.\n")
    else:
        add(f"**PAUSE.** Only {history_days} days of history "
            f"(threshold: {MIN_USABLE_DAYS}). Let the pipeline keep collecting; "
            f"re-run this audit later. Baselines (Phase 1) could still be "
            f"prototyped, but no model bullets until real history exists.\n")

    add("## Overview\n")
    add("| Metric | Value |")
    add("|---|---|")
    add(f"| Total snapshot rows | {overview['total_rows']:,} |")
    add(f"| Usable rows (OPERATING, non-null wait) | {overview['usable_rows']:,} |")
    add(f"| Distinct attractions | {overview['distinct_rides']} |")
    add(f"| Distinct parks | {overview['distinct_parks']} |")
    add(f"| First snapshot | {overview['first_snapshot']:%Y-%m-%d %H:%M UTC} |")
    add(f"| Last snapshot | {overview['last_snapshot']:%Y-%m-%d %H:%M UTC} |")
    add(f"| History span | {history_days} days |")
    add(f"| Calendar hours in span | {total_spine_hours:,} |")
    add(f"| Hours with data | {total_spine_hours - missing_hours:,} |")
    add(f"| Hours with zero rows (true gaps) | {missing_hours:,} |")
    add(f"| Hours with operating rides (signal hours) | {signal_hours:,} |\n")

    add("## Gap analysis\n")
    add("Zero-row hours are collection failures or the migration seam — the "
        "parks being closed does *not* create these, because the workflow "
        "runs 24/7 and closed rides still produce rows.\n")
    if gaps:
        add("| Gap start (UTC) | Gap end (UTC) | Hours |")
        add("|---|---|---|")
        for g in gaps[:15]:
            add(f"| {g['start']:%Y-%m-%d %H:%M} | {g['end']:%Y-%m-%d %H:%M} | {g['hours']} |")
        if len(gaps) > 15:
            add(f"\n*…and {len(gaps) - 15} smaller gaps.*")
    else:
        add("No zero-row hours. Clean spine.")
    add("")

    add("## Status distribution\n")
    add("| Status | Rows | Null standby_wait |")
    add("|---|---|---|")
    for s in statuses:
        add(f"| {s['status']} | {s['rows_']:,} | {s['null_wait_rows']:,} |")
    add("")

    add("## Target definition\n")
    add("Predict the **standby wait at T+1h, per attraction**, where T is a "
        "park-open hour. Ground truth = mean of that attraction's non-null "
        "OPERATING `standby_wait` snapshots within hour T+1 (snapshots land "
        "~2x/hour at :23 and :53; averaging them is the hourly grain).\n")

    add("## Inclusion rules\n")
    add(f"An attraction enters the training set if it has non-null OPERATING "
        f"waits in at least {MIN_COVERAGE_PCT:.0f}% of its park's open hours.\n")
    add(f"**Included: {len(included)} attractions. Excluded: {len(excluded)}.**\n")

    add("### Included attractions\n")
    add("| Ride | Park | Usable rows | Coverage |")
    add("|---|---|---|---|")
    for r in included:
        park = PARK_NAMES.get(r["park_id"], r["park_id"][:8])
        add(f"| {r['ride_name']} | {park} | {r['usable_rows']:,} | {r['coverage_pct']}% |")
    add("")

    if excluded:
        add("### Excluded (sparse) attractions\n")
        add("| Ride | Park | Usable rows | Coverage |")
        add("|---|---|---|---|")
        for r in excluded:
            park = PARK_NAMES.get(r["park_id"], r["park_id"][:8])
            add(f"| {r['ride_name']} | {park} | {r['usable_rows']:,} | {r['coverage_pct']}% |")
        add("")

    out_path = os.path.join(os.path.dirname(__file__), "..", "DATA_AUDIT.md")
    out_path = os.path.abspath(out_path)
    with open(out_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    print(f"Wrote {out_path}")
    print(f"Verdict: {'PROCEED' if verdict_proceed else 'PAUSE'} "
          f"({history_days} days of history)")


if __name__ == "__main__":
    main()
