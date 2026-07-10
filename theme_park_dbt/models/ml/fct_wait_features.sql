-- theme_park_dbt/models/ml/fct_wait_features.sql
-- ML feature mart: one row per (ride, hour) on a DENSE hourly spine.
--
-- Why a dense spine: LAG(1) over only the hours that have data would
-- silently reach across gaps ("1 hour ago" becoming "3 hours ago" over a
-- collection outage). Generating every calendar hour per ride first means
-- every lag is exactly what it claims to be; missing hours yield NULL
-- features, which is the honest answer.
--
-- Leakage rule: every feature at hour T is computed from data <= T.
-- The label (target_next_hour) is the ONLY forward-looking column.
--
-- Time features use park-local time (America/New_York): collected_at is
-- UTC, and "2pm at Magic Kingdom" is an Eastern-time concept.

with hourly as (

    -- hourly grain: mean wait across the ~2 snapshots per hour
    select
        ride_id,
        any_value(ride_name)  as ride_name,
        any_value(park_name)  as park_name,
        timestamp_trunc(collected_at, hour) as feature_hour,
        avg(standby_wait)     as avg_wait,
        count(*)              as n_snapshots
    from {{ ref('stg_wait_times') }}
    where status = 'OPERATING'
      and standby_wait is not null
    group by ride_id, timestamp_trunc(collected_at, hour)

),

ride_bounds as (

    select
        ride_id,
        min(feature_hour) as first_hour,
        max(feature_hour) as last_hour
    from hourly
    group by ride_id

),

spine as (

    -- every calendar hour each ride has been observable
    select
        b.ride_id,
        h as feature_hour
    from ride_bounds b,
         unnest(generate_timestamp_array(b.first_hour, b.last_hour,
                                         interval 1 hour)) as h

),

dense as (

    select
        s.ride_id,
        s.feature_hour,
        h.ride_name,
        h.park_name,
        h.avg_wait,
        h.n_snapshots
    from spine s
    left join hourly h
      on h.ride_id = s.ride_id and h.feature_hour = s.feature_hour

),

features as (

    select
        ride_id,
        -- carry names forward across closed hours
        last_value(ride_name ignore nulls) over w_all as ride_name,
        last_value(park_name ignore nulls) over w_all as park_name,
        feature_hour,
        avg_wait,
        n_snapshots,

        -- lags (dense spine => a lag of k rows IS k hours)
        lag(avg_wait, 1)   over w as lag_1h,
        lag(avg_wait, 2)   over w as lag_2h,
        lag(avg_wait, 3)   over w as lag_3h,
        lag(avg_wait, 24)  over w as lag_24h,
        lag(avg_wait, 168) over w as lag_168h,

        -- rolling stats over trailing windows ENDING AT T (inclusive):
        -- uses only data <= T, so no leakage
        avg(avg_wait) over (partition by ride_id order by feature_hour
                            rows between 2 preceding and current row)  as roll_mean_3h,
        avg(avg_wait) over (partition by ride_id order by feature_hour
                            rows between 23 preceding and current row) as roll_mean_24h,
        max(avg_wait) over (partition by ride_id order by feature_hour
                            rows between 23 preceding and current row) as roll_max_24h,

        -- calendar features in PARK-LOCAL time
        extract(hour     from datetime(feature_hour, 'America/New_York')) as hour_of_day,
        extract(dayofweek from datetime(feature_hour, 'America/New_York')) as day_of_week,
        extract(dayofweek from datetime(feature_hour, 'America/New_York')) in (1, 7)
            as is_weekend,

        -- THE LABEL: next hour's wait. The only forward-looking column.
        lead(avg_wait, 1) over w as target_next_hour

    from dense
    window
        w     as (partition by ride_id order by feature_hour),
        w_all as (partition by ride_id order by feature_hour
                  rows between unbounded preceding and current row)

)

select * from features
-- keep rows where a prediction is even possible: we need the current
-- value (persistence baseline / lag_0) and a label to learn from
where avg_wait is not null
  and target_next_hour is not null
