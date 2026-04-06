import streamlit as st
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

st.set_page_config(
    page_title="Theme Park Wait Times",
    page_icon="🎢",
    layout="wide"
)

@st.cache_resource(ttl=3600)
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse="THEME_PARK_WH",
        database="THEME_PARK_DB",
        schema="ANALYTICS"
    )

@st.cache_resource(ttl=3600)
def load_park_data():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM mart_wait_times_by_park ORDER BY hour_collected DESC", conn)
    return df

@st.cache_resource(ttl=3600)
def load_ride_data():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM mart_wait_times_by_ride ORDER BY avg_wait_minutes DESC", conn)
    return df

# Header
st.title("🎢 Walt Disney World Wait Time Analytics")
st.caption("Hourly data ingested via Apache Airflow → Snowflake → dbt")

# Load data
park_df = load_park_data()
ride_df = load_ride_data()

# KPI row
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Snapshots", f"{park_df.shape[0]:,}")
with col2:
    st.metric("Avg Wait (All Parks)", f"{park_df['AVG_WAIT_MINUTES'].mean():.0f} min")
with col3:
    st.metric("Peak Wait Recorded", f"{park_df['MAX_WAIT_MINUTES'].max():.0f} min")
with col4:
    st.metric("Parks Tracked", park_df['PARK_NAME'].nunique())

st.divider()

# Park comparison
st.subheader("Average Wait Times by Park")
park_summary = park_df.groupby("PARK_NAME")["AVG_WAIT_MINUTES"].mean().sort_values(ascending=False).reset_index()
st.bar_chart(park_summary.set_index("PARK_NAME"))

st.divider()

# Top rides
st.subheader("Top 10 Busiest Rides")
top_rides = ride_df.head(10)[["RIDE_NAME", "PARK_NAME", "AVG_WAIT_MINUTES", "MAX_WAIT_MINUTES"]]
st.dataframe(top_rides, use_container_width=True)

st.divider()

# Wait times over time
st.subheader("Average Wait Times Over Time by Park")
time_df = park_df.sort_values("HOUR_COLLECTED")
time_chart = time_df.pivot_table(
    index="HOUR_COLLECTED",
    columns="PARK_NAME",
    values="AVG_WAIT_MINUTES"
)
st.line_chart(time_chart)