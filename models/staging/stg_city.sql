-- Staging: Application.Cities (source for Dimension.City). Replaces GetCityUpdates → City_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'Cities') }}
),
renamed as (
    select
        Cityid as wwi_city_id,
        cityname as city,
        stateprovinceid as state_province_id,
        location,
        {{ cross_db_safe_cast_int_coalesce('latestrecordedpopulation', 0) }} as latest_recorded_population,
        {{ cross_db_cast_timestamp('validfrom') }} as valid_from,
        {{ cross_db_cast_timestamp('validto') }} as valid_to
    from source
)
select * from renamed
