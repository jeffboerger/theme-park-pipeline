with weather as (
    select
        weather_hour,
        temperature_f,
        humidity_pct,
        precipitation_mm,
        weather_condition,
        temp_category,
        wind_speed_kmh
    from {{ ref('stg_weather') }}
),

wait_times as (
    select
        date_trunc('hour', collected_at)    as wait_hour,
        park_name,
        avg(standby_wait)                   as avg_wait_minutes,
        max(standby_wait)                   as max_wait_minutes,
        count(distinct ride_id)             as rides_tracked
    from {{ ref('stg_wait_times') }}
    where standby_wait is not null
    and status = 'OPERATING'
    group by date_trunc('hour', collected_at), park_name
),

joined as (
    select
        w.wait_hour,
        w.park_name,
        w.avg_wait_minutes,
        w.max_wait_minutes,
        w.rides_tracked,
        wt.temperature_f,
        wt.humidity_pct,
        wt.precipitation_mm,
        wt.weather_condition,
        wt.temp_category,
        wt.wind_speed_kmh
    from wait_times w
    left join weather wt
        on w.wait_hour = wt.weather_hour
)

select * from joined
order by wait_hour desc, park_name