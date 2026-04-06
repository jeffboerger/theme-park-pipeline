import requests
from datetime import datetime, timezone

PARKS = {
    "75ea578a-adc8-4116-a54d-dccb60765ef9": "Magic Kingdom",
    "47f90d2c-e191-4239-a466-5892ef59a88b": "EPCOT",
    "288747d1-8b4f-4a64-867e-ea7c9b27bad8": "Hollywood Studios",
    "1c84a229-8862-4648-9c71-378ddd2c7693": "Animal Kingdom"
}

def fetch_wait_times():
    """
    Fetches live wait time and forecast data for all configured parks
    from the ThemeParks.wiki API.

    Iterates over each park in PARKS, pulls all attraction entities,
    and builds two lists of tuples ready for database insertion:
        - Current wait time snapshots per ride
        - Hourly forecasted wait times per ride

    Returns:
        tuple: (wait_rows, forecast_rows)
            wait_rows (list): One tuple per attraction containing current
                              status, standby wait, and lightning lane info.
            forecast_rows (list): One tuple per forecasted hour per attraction
                                  containing predicted wait time and percentage.
    """
    collected_at = datetime.now(timezone.utc)
    wait_rows = []
    forecast_rows = []

    for park_id, park_name in PARKS.items():
        print(f"Pulling {park_name}...")
        url = f"https://api.themeparks.wiki/v1/entity/{park_id}/live"
        response = requests.get(url)
        data = response.json()

        for ride in data['liveData']:
            if ride['entityType'] != "ATTRACTION":
                continue

            standby_wait = ride.get("queue", {}).get("STANDBY", {}).get("waitTime")
            ll = ride.get("queue", {}).get("RETURN_TIME", {})
            ll_state = ll.get("state")
            ll_return_start = ll.get('returnStart')

            wait_rows.append((
                ride['id'],
                ride['name'],
                park_id,
                ride.get("status"),
                standby_wait,
                ll_state,
                ll_return_start,
                collected_at
            ))

            for f in ride.get("forecast", []):
                forecast_rows.append((
                    ride["id"],
                    ride["name"],
                    park_id,
                    f["time"],
                    f["waitTime"],
                    f["percentage"],
                    collected_at  
                ))

    return wait_rows, forecast_rows