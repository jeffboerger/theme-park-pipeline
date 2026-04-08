import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse="THEME_PARK_WH",
    database="THEME_PARK_DB",
    schema="ANALYTICS"
)

# Export both marts to CSV
park_df = pd.read_sql("SELECT * FROM mart_wait_times_by_park", conn)
ride_df = pd.read_sql("SELECT * FROM mart_wait_times_by_ride", conn)

park_df.to_csv("mart_wait_times_by_park.csv", index=False)
ride_df.to_csv("mart_wait_times_by_ride.csv", index=False)

print(f"Exported {len(park_df)} park rows")
print(f"Exported {len(ride_df)} ride rows")

conn.close()