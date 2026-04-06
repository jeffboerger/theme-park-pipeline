with base as (
    select * from {{ ref('stg_wait_times') }}
    where standby_wait is not null
    and status = 'OPERATING'
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
