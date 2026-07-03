-- theme_park_dbt/models/marts/mart_wait_times_by_park.sql
-- BigQuery port. Only Snowflake-specific functions changed:
--   dateadd(...)          -> timestamp_sub(...)
--   date_trunc('hour', x) -> timestamp_trunc(x, hour)
-- All columns and logic preserved from the original.

with base as (
    select * from {{ ref('stg_wait_times') }}
    where standby_wait is not null
      and collected_at >= timestamp_sub(current_timestamp(), interval 24 hour)
),

aggregated as (
    select
        park_name,
        timestamp_trunc(collected_at, hour) as hour_collected,
        count(distinct ride_id)          as total_rides,
        avg(standby_wait)                as avg_wait_minutes,
        max(standby_wait)                as max_wait_minutes,
        min(standby_wait)                as min_wait_minutes,
        count(case when status = 'OPERATING' then 1 end) as rides_operating,
        count(case when status = 'CLOSED' then 1 end)    as rides_closed
    from base
    group by park_name, timestamp_trunc(collected_at, hour)
)

select * from aggregated
order by hour_collected desc, park_name
