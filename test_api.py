from etl.extract import fetch_wait_times
from etl.load import load_wait_times

wait_rows, forecast_rows = fetch_wait_times()
print(f"wait_rows: {len(wait_rows)}")
print(f"forecast_rows: {len(forecast_rows)}")
load_wait_times(wait_rows, forecast_rows)

