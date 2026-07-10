"""Phase 0 data audit for the wait-time prediction project.

Runs read-only aggregate queries against the raw dataset and writes
ml/DATA_AUDIT.md with everything Phase 1 needs to know: usable history,
gaps, per-ride coverage, target distribution, and benchmark availability.

Usage (from repo root, same env as the pipeline):
    python ml/phase0_audit.py
"""

import os
from datetime import datetime

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
PROJECT = os.getenv("GCP_PROJECT")
RAW = os.getenv("BQ_RAW_DATASET", "raw")


def q(client, sql):
    return list(client.query(sql).result())


def main():
    client = bigquery.Client(project=PROJECT)
    t = f"`{PROJECT}.{RAW}.raw_wait_times`"
    f = f"`{PROJECT}.{RAW}.raw_forecast`"
    w = f"`{PROJECT}.{RAW}.raw_weather`"
    lines = [f"# Data Audit - {datetime.now():%Y-%m-%d}\n"]

    # ---- overall span and volume ----
    r = q(client, f"""
        SELECT COUNT(*) n_rows, MIN(collected_at) first, MAX(collected_at) last,
               COUNT(DISTINCT ride_id) rides,
               COUNT(DISTINCT DATE(collected_at)) days,
               COUNT(DISTINCT TIMESTAMP_TRUNC(collected_at, HOUR)) hours
        FROM {t}""")[0]
    span_days = (r.last - r.first).days or 1
    lines += [f"## Wait-time snapshots\n",
              f"- Rows: **{r.n_rows:,}** across **{r.rides}** rides",
              f"- Range: **{r.first:%Y-%m-%d} -> {r.last:%Y-%m-%d}** ({span_days} days)",
              f"- Distinct snapshot hours: **{r.hours:,}** "
              f"(a perfect hourly run would be ~{span_days * 24:,} -> "
              f"~{100 * r.hours / (span_days * 24):.0f}% hour coverage)\n"]

    # ---- gap analysis: days with missing hours ----
    rows = q(client, f"""
        SELECT DATE(collected_at) d,
               COUNT(DISTINCT TIMESTAMP_TRUNC(collected_at, HOUR)) h
        FROM {t} GROUP BY d HAVING h < 20 ORDER BY d""")
    lines.append("## Gap days (fewer than 20 snapshot-hours)\n")
    if rows:
        for x in rows:
            lines.append(f"- {x.d}: {x.h} hours")
        lines.append(f"\n{len(rows)} gap day(s) - check Actions history / "
                     "migration seam; decide exclude-vs-keep per day.\n")
    else:
        lines.append("None - collection has been clean.\n")

    # ---- status mix (target only meaningful while OPERATING) ----
    lines.append("## Status distribution\n")
    for x in q(client, f"""
        SELECT status, COUNT(*) n,
               ROUND(100*COUNT(*)/SUM(COUNT(*)) OVER(),1) pct,
               ROUND(100*COUNTIF(standby_wait IS NOT NULL)/COUNT(*),1) has_wait_pct
        FROM {t} GROUP BY status ORDER BY n DESC"""):
        lines.append(f"- {x.status}: {x.n:,} rows ({x.pct}%), "
                     f"standby_wait present {x.has_wait_pct}%")
    lines.append("")

    # ---- per-ride coverage: modeling-readiness ranking ----
    lines.append("## Ride coverage (top 20 by usable rows: OPERATING + non-null wait)\n")
    lines.append("| ride | park | usable rows | median wait | p95 |")
    lines.append("|---|---|---|---|---|")
    for x in q(client, f"""
        SELECT ride_name, ANY_VALUE(park_id) park_id, COUNT(*) usable,
               APPROX_QUANTILES(standby_wait, 100)[OFFSET(50)] med,
               APPROX_QUANTILES(standby_wait, 100)[OFFSET(95)] p95
        FROM {t}
        WHERE status = 'OPERATING' AND standby_wait IS NOT NULL
        GROUP BY ride_name ORDER BY usable DESC LIMIT 20"""):
        lines.append(f"| {x.ride_name} | {x.park_id[:8]} | {x.usable:,} "
                     f"| {x.med} | {x.p95} |")
    n_thin = q(client, f"""
        SELECT COUNT(*) n FROM (
          SELECT ride_name FROM {t}
          WHERE status='OPERATING' AND standby_wait IS NOT NULL
          GROUP BY ride_name HAVING COUNT(*) < 500)""")[0].n
    lines.append(f"\nRides with <500 usable rows (candidates to exclude): **{n_thin}**\n")

    # ---- benchmark availability: the API's own forecast ----
    r = q(client, f"""
        SELECT COUNT(*) n_rows, MIN(collected_at) first, MAX(collected_at) last,
               COUNT(DISTINCT ride_id) rides FROM {f}""")[0]
    lines += ["## Forecast table (benchmark: beat the API's own predictions)\n",
              f"- {r.n_rows:,} forecast rows for {r.rides} rides, "
              f"{r.first:%Y-%m-%d} -> {r.last:%Y-%m-%d}\n"]

    # ---- weather coverage (Phase 2 features) ----
    r = q(client, f"""
        SELECT COUNT(*) n_rows, MIN(collected_at) first, MAX(collected_at) last
        FROM {w}""")[0]
    lines += ["## Weather table (Phase 2 features)\n",
              f"- {r.n_rows:,} rows, {r.first:%Y-%m-%d} -> {r.last:%Y-%m-%d}\n"]

    # ---- target definition (locked here, cited by later phases) ----
    lines += ["## Target definition (Phase 1+)\n",
              "- Predict `standby_wait` at T+1 hour, per ride, using data <= T",
              "- Training rows restricted to `status = 'OPERATING'` and "
              "non-null standby_wait",
              "- Rides under the usable-rows floor excluded (list above)",
              "- Time-ordered splits only; final N weeks held out for testing\n"]

    os.makedirs("ml", exist_ok=True)
    with open("ml/DATA_AUDIT.md", "w") as fh:
        fh.write("\n".join(lines))
    print("Wrote ml/DATA_AUDIT.md")
    print("\n".join(lines[:12]))


if __name__ == "__main__":
    main()
