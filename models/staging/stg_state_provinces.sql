{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'StateProvinces') }}
),

renamed as (
    select
        cast(StateProvinceID as int64) as state_province_id,
        StateProvinceCode as state_province_code,
        StateProvinceName as state_province_name,
        cast(CountryID as int64) as country_id,
        SalesTerritory as sales_territory,
        Border as border,
        cast(LatestRecordedPopulation as int64) as latest_recorded_population,
        {{ parse_timestamp('ValidFrom') }} as valid_from,
        {{ parse_timestamp('ValidTo') }} as valid_to,
        cast(LastEditedBy as int64) as last_edited_by
    from source
    where StateProvinceID is not null
)

select * from renamed

