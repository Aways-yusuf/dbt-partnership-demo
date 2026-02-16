-- Staging: Application.Cities (source for Dimension.City). Replaces GetCityUpdates → City_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'Cities') }}
),
renamed as (
    select
        city_id as wwi_city_id,
        city_name as city,
        state_province_id,
        location,
        coalesce(latest_recorded_population, 0) as latest_recorded_population,
        valid_from,
        valid_to
    from source
)
select * from renamed
