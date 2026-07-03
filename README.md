# 🎢 Theme Park Analytics Pipeline

A data engineering pipeline that ingests hourly ride wait times from Walt Disney World into BigQuery, orchestrated with GitHub Actions and transformed with dbt.

**Live Dashboard:** https://theme-park-pipeline-hjrcuhrabfjsumpqd4kdyr.streamlit.app/

---

## Why This Project

Built to demonstrate a production-grade modern data stack using real, live data from Walt Disney World — the kind of pipeline that theme park operations teams actually run. Designed as a portfolio project targeting Data Engineer roles in the Orlando market.

The entire stack runs on free tiers: BigQuery (10 GB storage / 1 TB queries per month), GitHub Actions (scheduled hourly runs), and Streamlit Community Cloud. No always-on server, no local machine dependency, no monthly cost.

---

## Stack

- **Python 3.11** — core language
- **ThemeParks.wiki API** — free, real-time ride wait time data for Disney and Universal
- **Open-Meteo API** — free hourly weather data for the Orlando area
- **GitHub Actions** — orchestrates hourly data pulls on a cron schedule (`0 * * * *`), replacing local Airflow so the pipeline runs whether or not any machine is on
- **BigQuery** — cloud data warehouse storing raw and transformed data on the free tier
- **dbt** — transforms raw data into analytics-ready models with data quality tests
- **Streamlit** — live dashboard visualizing wait times, trends, and weather correlation

> **Note on orchestration:** The pipeline was originally built and prototyped with Apache Airflow locally (the DAG remains in `dags/` for reference). For zero-cost always-on hosting, production scheduling was moved to GitHub Actions — a deliberate cost-engineering tradeoff. Batch loads run hourly via `load_table_from_json`, which uses BigQuery load jobs (free) rather than the streaming insert API (billed).

---

## What It Does

Every hour, a scheduled GitHub Actions workflow triggers the pipeline, which pulls live wait time data across all four Walt Disney World theme parks — Magic Kingdom, EPCOT, Hollywood Studios, and Animal Kingdom. For each attraction it captures the current standby wait, Lightning Lane status, and hourly forecasted wait times for the day, alongside a matching Orlando weather snapshot. All data lands in BigQuery with a `collected_at` timestamp, building a time series that enables trend and weather-correlation analysis across hours, days, and attractions.

---

## Architecture

```
ThemeParks.wiki API ─┐
                     ├─> run_pipeline.py ─> BigQuery (raw dataset) ─> dbt ─> BigQuery (analytics dataset) ─> Streamlit
Open-Meteo API ──────┘        ▲
                              │
                    GitHub Actions (hourly cron)
```

---

## Project Structure

```
theme-park-pipeline/
├── .github/
│   └── workflows/
│       └── pipeline.yml         # GitHub Actions — hourly ingestion + dbt run
├── dags/
│   └── theme_park_dag.py        # Original Airflow DAG (reference only)
├── etl/
│   ├── extract.py               # API calls to ThemeParks.wiki + Open-Meteo
│   └── load.py                  # BigQuery load jobs
├── theme_park_dbt/
│   ├── packages.yml             # dbt package dependencies (dbt_utils)
│   ├── profiles.yml             # BigQuery connection profile
│   └── models/
│       ├── staging/
│       │   ├── stg_wait_times.sql
│       │   ├── stg_forecast.sql
│       │   ├── stg_weather.sql
│       │   └── sources.yml
│       └── marts/
│           ├── mart_wait_times_by_park.sql
│           ├── mart_wait_times_by_ride.sql
│           ├── mart_weather_vs_wait_times.sql
│           └── schema.yml       # dbt data quality tests
├── setup_bigquery.py            # Creates datasets + raw tables
├── run_pipeline.py              # Production ingestion entrypoint
├── export_for_bi.py             # Exports marts to CSV for Tableau/Power BI
├── streamlit_app.py             # Live dashboard
├── requirements.txt             # Full local/dev dependencies
├── requirements-pipeline.txt    # Slim deps for the hourly Action
├── requirements-streamlit.txt   # Deps for Streamlit Cloud
├── .env                         # Credentials (not committed)
└── README.md
```

---

## Data Model

**`raw.raw_wait_times`** — one row per attraction per hourly snapshot
- `ride_id`, `ride_name`, `park_id`, `status`, `standby_wait`, `lightning_lane_state`, `lightning_lane_return_start`, `collected_at`

**`raw.raw_forecast`** — one row per forecasted hour per attraction per snapshot
- `ride_id`, `ride_name`, `park_id`, `forecasted_time`, `wait_time`, `percentage`, `collected_at`

**`raw.raw_weather`** — one row per weather snapshot
- `collected_at`, `temperature_f`, `humidity_pct`, `precipitation_mm`, `weather_code`, `wind_speed_kmh`

**`analytics.stg_wait_times`** — cleaned wait times with `park_name` and `wait_category` derived columns

**`analytics.stg_forecast`** — cleaned forecasts with `forecast_hour`, `day_of_week`, `hour_of_day`

**`analytics.stg_weather`** — cleaned weather with `weather_hour`, `weather_condition`, `temp_category`

**`analytics.mart_wait_times_by_park`** — avg/max/min wait times per park per hour, with operating/closed ride counts

**`analytics.mart_wait_times_by_ride`** — avg/max/min wait times per ride across recent snapshots

**`analytics.mart_weather_vs_wait_times`** — hourly weather joined to hourly park wait times for correlation analysis

---

## Setup

### Prerequisites
- Python 3.11
- Google Cloud project with the BigQuery API enabled
- A GCP service account with **BigQuery Data Editor** + **BigQuery Job User** roles, and its JSON key

### Installation
```bash
git clone https://github.com/jeffboerger/theme-park-pipeline
cd theme-park-pipeline
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file in the project root:

```
GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service-account-key.json
GCP_PROJECT=your-gcp-project-id
BQ_RAW_DATASET=raw
BQ_ANALYTICS_DATASET=analytics
```

### BigQuery Setup
```bash
python setup_bigquery.py   # Creates raw + analytics datasets and all raw tables
```

### First Ingestion
```bash
python run_pipeline.py     # Pulls live data and loads it into BigQuery
```

### dbt Setup
```bash
cd theme_park_dbt
dbt deps       # Install dbt_utils
dbt debug      # Verify BigQuery connection
dbt run        # Build all staging + mart models
dbt test       # Run data quality tests
```

### Automated Hourly Runs (GitHub Actions)
1. Push the repo to GitHub.
2. In repo **Settings → Secrets and variables → Actions**, add a secret `GCP_SA_KEY` containing the full service-account JSON, and a variable `GCP_PROJECT` with your project id.
3. The workflow in `.github/workflows/pipeline.yml` runs hourly and can also be triggered manually from the Actions tab.

### Run Dashboard Locally
```bash
streamlit run streamlit_app.py
```

For Streamlit Community Cloud, add the service-account JSON under a `[gcp_service_account]` table in the app's Secrets settings.

---

## Future Improvements

### Dashboard
- Replace default Streamlit charts with Plotly for better interactivity and styling
- Add an interactive map showing wait times by attraction location within each park
- Add a park selector filter and a historical time-range slider
- Color code wait categories on the ride table

### Pipeline
- Add Universal Orlando Resort parks
- Add email/Slack alerting on GitHub Actions run failures
- Build a forecast accuracy mart comparing predicted vs actual wait times
- Partition raw tables by ingestion date to keep queries within the free tier as history grows

### Portfolio
- Add a write-up blog post explaining the architecture decisions
- Record a short demo video for LinkedIn

---

## Author

Jeff Boerger — [jeff.boerger.co](https://jeff.boerger.co) | [GitHub](https://github.com/jeffboerger) | [LinkedIn](https://linkedin.com/in/jeffboerger)
