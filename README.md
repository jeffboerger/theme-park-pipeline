# 🎢 Theme Park Analytics Pipeline

A real-time data engineering pipeline that ingests hourly ride wait times from Walt Disney World into Snowflake, orchestrated with Apache Airflow and transformed with dbt.

**Live Dashboard:** https://theme-park-pipeline-hjrcuhrabfjsumpqd4kdyr.streamlit.app/

---

## Why This Project

Built to demonstrate a production-grade modern data stack using real, live data from Walt Disney World — the kind of pipeline that theme park operations teams actually run. Designed as a portfolio project targeting Data Engineer roles in the Orlando market.

---

## Stack

- **Python 3.11** — core language
- **ThemeParks.wiki API** — free, real-time ride wait time data for Disney and Universal
- **Apache Airflow** — orchestrates hourly data pulls on a cron schedule (`0 * * * *`)
- **Snowflake** — cloud data warehouse storing raw and transformed data
- **dbt** — transforms raw data into analytics-ready models with data quality tests
- **Streamlit** — live dashboard visualizing wait times and trends

---

## What It Does

Every hour, Airflow triggers the pipeline which pulls live wait time data across all four Walt Disney World theme parks — Magic Kingdom, EPCOT, Hollywood Studios, and Animal Kingdom. For each attraction it captures the current standby wait, Lightning Lane status, and hourly forecasted wait times for the day. All data lands in Snowflake with a `collected_at` timestamp, building a time series that enables trend analysis across hours, days, and attractions.

---

## Project Structure

theme-park-pipeline/
├── dags/
│   └── theme_park_dag.py        # Airflow DAG — hourly ingestion schedule
├── etl/
│   ├── extract.py               # API calls to ThemeParks.wiki
│   └── load.py                  # Snowflake connection and inserts
├── theme_park_dbt/
│   └── models/
│       ├── staging/
│       │   ├── stg_wait_times.sql   # Cleans raw wait time data
│       │   ├── stg_forecast.sql     # Cleans raw forecast data
│       │   └── sources.yml          # dbt source definitions
│       └── marts/
│           ├── mart_wait_times_by_park.sql  # Aggregated by park and hour
│           └── mart_wait_times_by_ride.sql  # Aggregated by ride
├── streamlit_app.py             # Live dashboard
├── .env                         # Credentials (not committed)
└── requirements.txt


---

## Data Model

**`RAW.raw_wait_times`** — one row per attraction per hourly snapshot
- `ride_id`, `ride_name`, `park_id`, `status`, `standby_wait`, `lightning_lane_state`, `collected_at`

**`RAW.raw_forecast`** — one row per forecasted hour per attraction per snapshot
- `ride_id`, `ride_name`, `park_id`, `forecasted_time`, `wait_time`, `percentage`, `collected_at`

**`ANALYTICS.stg_wait_times`** — cleaned wait times with `park_name` and `wait_category` derived columns

**`ANALYTICS.stg_forecast`** — cleaned forecasts with `forecast_hour`, `day_of_week`, `hour_of_day`

**`ANALYTICS.mart_wait_times_by_park`** — avg/max/min wait times per park per hour

**`ANALYTICS.mart_wait_times_by_ride`** — avg/max/min wait times per ride across all snapshots

---

## Setup

### Prerequisites
- Python 3.11
- Snowflake account
- Airflow 3.x

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

SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password


### Snowflake Setup
```bash
python test_api.py  # Creates database, schema, warehouse, and tables
```

### dbt Setup
```bash
cd theme_park_dbt
dbt debug      # Verify connection
dbt run        # Build all models
```

### Start Airflow
```bash
export AIRFLOW_HOME=~/Dev/theme-park-pipeline/airflow
airflow standalone
```

Navigate to `http://localhost:8080`, find `theme_park_wait_times`, and enable it.

### Run Dashboard Locally
```bash
streamlit run streamlit_app.py
```

---

## Future Improvements

### Dashboard
- Replace default Streamlit charts with Plotly for better interactivity and styling
- Add an interactive map showing wait times by attraction location within each park
- Add a park selector filter so users can drill into a single park
- Add a time range slider to explore historical trends
- Color code wait categories (green/yellow/red) on the ride table
- Add Lightning Lane availability tracking

### Pipeline
- Add Universal Orlando Resort parks
- Deploy Airflow to a cloud server (AWS EC2 or Astronomer) so the pipeline runs when the local machine is off
- Migrate from Snowflake to BigQuery after trial period for long-term free hosting
- Add dbt data quality tests for null ride IDs and negative wait times
- Add email alerting in Airflow for pipeline failures
- Build a forecast accuracy mart comparing predicted vs actual wait times

### Portfolio
- Add a write-up blog post explaining the architecture decisions
- Record a short demo video for LinkedIn

---

## Author

Jeff Boerger — [jeff.boerger.co](https://jeff.boerger.co) | [GitHub](https://github.com/jeffboerger) | [LinkedIn](https://linkedin.com/in/jeffboerger)