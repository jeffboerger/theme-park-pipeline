with source as (
    select * from {{ source('raw', 'raw_weather') }}
),

renamed as (
    select
        collected_at,
        temperature_f,
        humidity_pct,
        precipitation_mm,
        weather_code,
        wind_speed_kmh,

        -- derived columns
        date_trunc('hour', collected_at)    as weather_hour,
        case
            when precipitation_mm > 0       then 'RAINY'
            when weather_code >= 95         then 'STORMY'
            when weather_code >= 61         then 'DRIZZLE'
            when weather_code >= 3          then 'CLOUDY'
            else                                 'CLEAR'
        end                                 as weather_condition,
        case
            when temperature_f >= 95        then 'VERY_HOT'
            when temperature_f >= 85        then 'HOT'
            when temperature_f >= 75        then 'WARM'
            when temperature_f >= 65        then 'MILD'
            else                                 'COOL'
        end                                 as temp_category

    from source
)

select * from renamed