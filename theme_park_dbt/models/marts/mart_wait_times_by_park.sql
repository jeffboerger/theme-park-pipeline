with base as (
    select * from {{ ref('stg_wait_times') }}
    where standby_wait is not null
    and collected_at >= dateadd('hour', -24, current_timestamp())
),

aggregated as (
    select
        park_name,
        date_trunc('hour', collected_at) as hour_collected,
        count(distinct ride_id)          as total_rides,
        avg(standby_wait)                as avg_wait_minutes,
        max(standby_wait)                as max_wait_minutes,
        min(standby_wait)                as min_wait_minutes,
        count(case when status = 'OPERATING' then 1 end) as rides_operating,
        count(case when status = 'CLOSED' then 1 end)    as rides_closed
    from base
    group by park_name, date_trunc('hour', collected_at)
)

select * from aggregated
order by hour_collected desc, park_name