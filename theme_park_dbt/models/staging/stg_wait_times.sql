with source as (
    select * from {{ source('raw', 'raw_wait_times') }}
),

renamed as (
    select
        ride_id,
        ride_name,
        park_id,
        status,
        standby_wait,
        lightning_lane_state,
        lightning_lane_return_start,
        collected_at,

        -- derived columns
        case park_id
            when '75ea578a-adc8-4116-a54d-dccb60765ef9' then 'Magic Kingdom'
            when '47f90d2c-e191-4239-a466-5892ef59a88b' then 'EPCOT'
            when '288747d1-8b4f-4a64-867e-ea7c9b27bad8' then 'Hollywood Studios'
            when '1c84a229-8862-4648-9c71-378ddd2c7693' then 'Animal Kingdom'
        end as park_name,

        case
            when standby_wait is null then 'NO_DATA'
            when standby_wait <= 15 then 'SHORT'
            when standby_wait <= 30 then 'MODERATE'
            when standby_wait <= 60 then 'LONG'
            else 'VERY_LONG'
        end as wait_category

    from source
)

select * from renamed