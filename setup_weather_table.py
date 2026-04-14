import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse="THEME_PARK_WH",
    database="THEME_PARK_DB",
    schema="RAW"
)

cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_weather (
        collected_at        TIMESTAMP_TZ,
        temperature_f       FLOAT,
        humidity_pct        INTEGER,
        precipitation_mm    FLOAT,
        weather_code        INTEGER,
        wind_speed_kmh      FLOAT
    )
""")

print("Weather table created!")
cursor.close()
conn.close()