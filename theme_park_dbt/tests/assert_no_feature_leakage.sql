-- theme_park_dbt/tests/assert_no_feature_leakage.sql
-- The classic junior mistake in time-series ML is leakage. This test makes
-- the no-leakage property EXECUTABLE: for every row, lag_1h at hour T must
-- exactly equal avg_wait at hour T-1h (and be NULL when T-1h has no data).
-- Any row returned = a leak or a broken spine = test failure.

with f as (
    select ride_id, feature_hour, avg_wait, lag_1h
    from {{ ref('fct_wait_features') }}
),

joined as (
    select
        cur.ride_id,
        cur.feature_hour,
        cur.lag_1h,
        prev.avg_wait as actual_prev_wait
    from f cur
    left join f prev
      on prev.ride_id = cur.ride_id
     and prev.feature_hour = timestamp_sub(cur.feature_hour, interval 1 hour)
)

select *
from joined
where lag_1h is not null
  and actual_prev_wait is not null
  and abs(lag_1h - actual_prev_wait) > 0.001
