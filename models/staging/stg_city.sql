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
        coalesce(safe_cast(latestrecordedpopulation as int64), 0) as latest_recorded_population,
        safe_cast(substr(cast(validfrom as string), 1, 26) as timestamp) as valid_from,
        safe_cast(substr(cast(validto as string), 1, 26) as timestamp) as valid_to
    from source
)
select * from renamed
