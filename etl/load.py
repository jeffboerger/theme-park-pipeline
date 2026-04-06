import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

def get_connection():
    """
    Establishes and returns a Snowflake database connection
    using credentials loaded from the .env file.
    """
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse="THEME_PARK_WH",
        database="THEME_PARK_DB",
        schema="RAW"
    )


def load_wait_times(wait_rows, forecast_rows):
    """
    Loads extracted wait time and forecast data into Snowflake.
    
    Args:
        wait_rows (list): List of tuples containing current ride wait time snapshots.
        forecast_rows (list): List of tuples containing hourly forecasted wait times.
    
    Inserts all rows in batches using executemany for performance,
    then commits the transaction to finalize the load.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.executemany("""
        INSERT INTO raw_wait_times 
        (ride_id, ride_name, park_id, status, standby_wait, lightning_lane_state, lightning_lane_return_start, collected_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, wait_rows)

    cursor.executemany("""
        INSERT INTO raw_forecast
        (ride_id, ride_name, park_id, forecasted_time, wait_time, percentage, collected_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, forecast_rows)

    conn.commit()
    print(f"Inserted {len(wait_rows)} wait time rows")
    print(f"Inserted {len(forecast_rows)} forecast rows")

    cursor.close()
    conn.close()