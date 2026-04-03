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

cursor.execute("SELECT COUNT(*) FROM raw_wait_times")
print(f"Wait times rows: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM raw_forecast")
print(f"Forecast rows: {cursor.fetchone()[0]}")

cursor.execute("SELECT ride_name, status, standby_wait, collected_at FROM raw_wait_times LIMIT 5")
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()
