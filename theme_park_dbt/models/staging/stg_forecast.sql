-- theme_park_dbt/models/staging/stg_forecast.sql
-- BigQuery port. Snowflake functions changed:
--   date_trunc('hour', x) -> timestamp_trunc(x, hour)
--   dayname(x)            -> format_timestamp('%A', x)
--   hour(x)               -> extract(hour from x)

with source as (
    select * from {{ source('raw', 'raw_forecast') }}
),

renamed as (
    select
        ride_id,
        ride_name,
        park_id,
        forecasted_time,
        wait_time,
        percentage,
        collected_at,

        -- derived columns
        case park_id
            when '75ea578a-adc8-4116-a54d-dccb60765ef9' then 'Magic Kingdom'
            when '47f90d2c-e191-4239-a466-5892ef59a88b' then 'EPCOT'
            when '288747d1-8b4f-4a64-867e-ea7c9b27bad8' then 'Hollywood Studios'
            when '1c84a229-8862-4648-9c71-378ddd2c7693' then 'Animal Kingdom'
        end as park_name,

        timestamp_trunc(forecasted_time, hour)   as forecast_hour,
        format_timestamp('%A', forecasted_time)  as day_of_week,
        extract(hour from forecasted_time)       as hour_of_day

    from source
)

select * from renamed
