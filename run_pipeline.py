"""Production pipeline entrypoint. Called hourly by GitHub Actions.

This is the cleaned-up version of test_api.py, renamed to reflect that it's
now the production ingestion step rather than a throwaway test script.

Flow: fetch wait times + forecasts -> load, then fetch weather -> load.
"""
from etl.extract import fetch_wait_times, fetch_weather
from etl.load import load_wait_times, load_weather


def main():
    wait_rows, forecast_rows = fetch_wait_times()
    print(f"Fetched {len(wait_rows)} wait rows, {len(forecast_rows)} forecast rows")
    load_wait_times(wait_rows, forecast_rows)

    # Weather is enrichment, not the product — its failure must never
    # kill a run that already loaded wait times.
    try:
        weather_row = fetch_weather()
        print(f"Fetched weather: {weather_row}")
        load_weather(weather_row)
    except Exception as e:
        print(f"  ! weather skipped this run: {e}")

    print("Pipeline run complete.")


if __name__ == "__main__":
    main()
