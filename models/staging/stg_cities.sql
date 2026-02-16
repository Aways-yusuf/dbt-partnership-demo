{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'Cities') }}
),

renamed as (
    select
        cast(CityID as int64) as wwi_city_id,
        CityName as city_name,
        cast(StateProvinceID as int64) as state_province_id,
        cast(Location as float64) as location,
        cast(LatestRecordedPopulation as int64) as latest_recorded_population,
        {{ parse_timestamp('ValidFrom') }} as valid_from,
        {{ parse_timestamp('ValidTo') }} as valid_to,
        cast(LastEditedBy as int64) as last_edited_by
    from source
    where CityID is not null
)

select * from renamed

