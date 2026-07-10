# ML Layer — Next-Hour Wait Prediction

Predicts each attraction's standby wait one hour ahead, built on the
pipeline's own collected data. This folder holds the audit, baseline, and
training scripts; the feature engineering lives in dbt where it belongs.

**Status: PAUSED for data.** Built and validated July 2026 on ~6 days of
history — enough to prove the machinery, not enough to claim numbers
(model currently loses to persistence, as expected and as documented in
RESULTS.md). Re-run in September when the warehouse holds 60+ days.
See the September runbook at the bottom.

---

## Where the data lives

Everything flows through BigQuery (project: `theme-park-pipeline`,
location US). Nothing ML-related is stored only on a laptop.

| Table | Dataset | Written by | What it is |
|---|---|---|---|
| `raw_wait_times` | `raw` | `etl/load.py` via GitHub Actions (hourly, :23 and :53) | Source of truth: per-ride snapshots |
| `stg_wait_times` | `analytics` | dbt | Cleaned staging view (park names, wait categories) |
| `fct_wait_features` | `analytics` | dbt (`dbt build`) | **The ML feature mart** — one row per (ride, hour) on a dense hourly spine: lags (1/2/3/24/168h), rolling stats, park-local calendar features, and the `target_next_hour` label |
| `ml_training_runs` | `raw` | `ml/train.py` (append per run) | Metrics history: per-fold and overall MAE for every training run, stamped with `run_at` and `history_days` |

Local artifacts (committed to the repo, regenerated on every run):
`ml/DATA_AUDIT.md`, `ml/BASELINES.md`, `ml/RESULTS.md`.

Rule of thumb for the two datasets: `analytics` belongs to dbt — nothing
else writes there. App code (the ETL, `train.py`) writes to `raw`, and dbt
models on top.

---

## Seeing the results (where's my data?)

Three kinds of runs leave three kinds of residue — knowing which is which
saves confusion:

| Run | Leaves behind |
|---|---|
| `python ml/test_ml.py` | **Nothing** — synthetic data, in memory, by design |
| `./dbtl build` | Tables in BigQuery (`analytics.fct_wait_features`) |
| `data_audit.py` / `baselines.py` / `train.py` | Markdown files locally **+** (train only) rows appended to `raw.ml_training_runs` |

Terminal output is just a preview; the files and tables are the record.

**Local files:**
```bash
cat DATA_AUDIT.md          # repo root — audit verdict + gap table
cat ml/BASELINES.md        # baseline MAE table
cat ml/RESULTS.md          # walk-forward folds + feature importance
```

**BigQuery console** — https://console.cloud.google.com/bigquery with the
`theme-park-pipeline` project selected. In the left-hand explorer:
- `analytics` → `fct_wait_features` → **Preview** tab: the actual training
  rows (every ride-hour with its lags and label).
- `raw` → `ml_training_runs` → **Preview**: every training run's metrics.

**Pulling data down programmatically** — same pattern everywhere in this
repo (client → query → DataFrame):

```python
from google.cloud import bigquery
from dotenv import load_dotenv; load_dotenv()
import os

client = bigquery.Client(project=os.getenv("GCP_PROJECT"))

# metrics history
runs = client.query("""
    SELECT run_at, fold, model_mae, persistence_mae, history_days
    FROM `theme-park-pipeline.raw.ml_training_runs`
    ORDER BY run_at DESC
""").result().to_dataframe()

# the feature mart itself (what the model trains on)
features = client.query("""
    SELECT * FROM `theme-park-pipeline.analytics.fct_wait_features`
    ORDER BY feature_hour DESC
    LIMIT 1000
""").result().to_dataframe()
```

Or in one line via the existing loader: `from ml.wait_data import
load_features; df = load_features()` (run from the repo root).

Note: none of this is *visualized* yet — charts of predictions vs actuals
and rolling MAE are Phase 4 (a panel in the existing Streamlit dashboard).
Until then the results live as files, tables, and queries.

---

## Setup (once per machine)

```bash
source venv/bin/activate
pip install -r ml/requirements.txt      # xgboost, scikit-learn, pandas,
                                        # tabulate, db-dtypes
```

`.env` at the repo root needs (first three already exist for the ETL):

```
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
GCP_PROJECT=theme-park-pipeline
BQ_RAW_DATASET=raw
BQ_DBT_DATASET=analytics      # must match dataset: in theme_park_dbt/profiles.yml
```

The Python scripts read `.env` themselves. **dbt does not** — it needs real
shell env vars, which is what the `./dbtl` wrapper handles (sources `.env`,
then runs dbt from `theme_park_dbt/`).

---

## The scripts

Run everything from the repo root. Order matters the first time:
audit → dbt build → baselines → train.

### 1. `ml/data_audit.py` — is there enough data?

```bash
python ml/data_audit.py
```

Queries `raw.raw_wait_times` directly (independent of dbt state) and writes
`DATA_AUDIT.md` at the repo root: row counts, per-attraction coverage,
collection-gap windows (zero-row hours — the workflow runs 24/7, so parks
being closed never creates these), and a **PROCEED/PAUSE verdict** against
the 60-day threshold. This is the gate: PAUSE means stop here and let the
pipeline collect.

### 2. Build the feature mart (dbt, not Python)

```bash
./dbtl deps                                  # first time only
./dbtl build --select fct_wait_features
```

Runs inside BigQuery. Builds the mart **and** its tests, including
`tests/assert_no_feature_leakage.sql`, which makes the no-leakage property
executable: every `lag_1h` value at hour T must equal `avg_wait` at T-1h,
or the build fails. The hourly GitHub Actions run rebuilds this
automatically, so the mart is always fresh.

### 3. `ml/baselines.py` — the numbers to beat

```bash
python ml/baselines.py        # writes ml/BASELINES.md
```

Pulls the feature mart and scores three baselines on a time-ordered
holdout (final 25% of the time range — never random splits):

- **persistence** — predict next hour = the wait right now. The real
  opponent; embarrassingly strong at 1-hour horizons.
- **hod_mean** — this ride's average wait at this hour-of-day, computed
  on training rows only.
- **seasonal_naive** — the wait exactly one week earlier. Reports `n/a`
  honestly until history exceeds a week with decent coverage.

A model that can't beat persistence is a lesson, not a bullet.

### 4. `ml/train.py` — the model

```bash
python ml/train.py            # writes ml/RESULTS.md + appends to BigQuery
```

XGBoost (MAE objective, categorical support) evaluated with
**walk-forward validation**: 4 expanding-window, strictly time-ordered
folds over the final 40% of the time range — train on the past, predict
the next window, exactly how the deployed model will live. Persistence is
scored on identical rows per fold, so the comparison is apples-to-apples.
Deterministic (`random_state=42`): rerunning on the same data reproduces
the same numbers, so any change in results is a change in data.

Outputs: verdict + per-fold table + feature importance in `RESULTS.md`,
and a metrics append to `raw.ml_training_runs`.

`ml/wait_data.py` is the shared module (BigQuery loader, feature contract,
the honesty banner) — imported by the others, never run directly.

### Reading the outputs honestly

Any run on under 60 days of history stamps a banner across both .md files:
the numbers prove the pipeline runs, they are NOT claimable performance
metrics. Between now and September, an occasional `train.py` run is worth
it anyway — every run logs `history_days` alongside MAE, building a
queryable learning curve of the model improving as the dataset grows:

```sql
SELECT run_at, history_days, model_mae, persistence_mae
FROM `theme-park-pipeline.raw.ml_training_runs`
WHERE fold = 'OVERALL'
ORDER BY run_at
```

---

## 🗓️ September runbook (~30 minutes)

The whole point of building this in July: September is a re-run, not a
build. Target date: **on/after Sept 5, 2026** (60 days from the July 3
collection restart) — and only in a week where applications went out.

```bash
cd ~/Dev/theme-park-pipeline && source venv/bin/activate

# 0. Sanity: is the pipeline still alive? (Actions tab should show hourly
#    runs; GitHub disables cron after 60 days of repo inactivity — any
#    commit resets that clock.)

# 1. The gate
python ml/data_audit.py            # want: PROCEED, 60+ days, gaps understood

# 2. Fresh feature mart (hourly Actions has been rebuilding it anyway)
./dbtl build --select fct_wait_features

# 3. Real baselines — seasonal_naive should now report a number
python ml/baselines.py

# 4. The moment of truth
python ml/train.py
```

Then judge like an adult:

- **Model beats persistence and seasonal-naive by a stateable margin** →
  the target claim from ML_ROADMAP.md unlocks. Update the resume bullet
  bank with the real MAE comparison; every number is defendable straight
  from RESULTS.md and `ml_training_runs`.
- **Model loses or ties** → the honest write-up of *why* (which features
  failed, what the folds show) is still interview material — and Phase 4
  waits until there's a model worth serving.
- Either way: commit the regenerated .md files, then proceed to **Phase 4**
  (hourly batch predictions via the existing Actions schedule + a
  predicted-vs-actual panel with rolling MAE in the Streamlit dashboard)
  per ML_ROADMAP.md.

Known data caveats to mention in any write-up: the July 3 collection
start (no Snowflake backfill), the July 9 outage window (dead workflow
trigger — visible in the audit's gap table and in the missing walk-forward
fold), and summer-vs-fall seasonality drift.
