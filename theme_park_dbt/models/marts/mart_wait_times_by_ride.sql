-- theme_park_dbt/models/marts/mart_wait_times_by_ride.sql
-- BigQuery port. Only Snowflake-specific functions changed:
--   dateadd(...) -> timestamp_sub(...)
-- All columns and logic preserved from the original.
-- NOTE: depends on wait_category = 'VERY_LONG' from stg_wait_times.

with base as (
    select * from {{ ref('stg_wait_times') }}
    where standby_wait is not null
      and status = 'OPERATING'
      and collected_at >= timestamp_sub(current_timestamp(), interval 24 hour)
),

aggregated as (
    select
        ride_id,
        ride_name,
        park_name,
        count(*)                as total_snapshots,
        avg(standby_wait)       as avg_wait_minutes,
        max(standby_wait)       as max_wait_minutes,
        min(standby_wait)       as min_wait_minutes,
        count(case when wait_category = 'VERY_LONG' then 1 end) as very_long_wait_count
    from base
    group by ride_id, ride_name, park_name
)

select * from aggregated
order by avg_wait_minutes desc
