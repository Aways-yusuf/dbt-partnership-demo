{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'Countries') }}
),

renamed as (
    select
        cast(CountryID as int64) as country_id,
        CountryName as country_name,
        FormalName as formal_name,
        IsoAlpha3Code as iso_alpha3_code,
        cast(IsoNumericCode as int64) as iso_numeric_code,
        CountryType as country_type,
        cast(LatestRecordedPopulation as int64) as latest_recorded_population,
        Continent as continent,
        Region as region,
        Subregion as subregion,
        Border as border,
        {{ parse_timestamp('ValidFrom') }} as valid_from,
        {{ parse_timestamp('ValidTo') }} as valid_to,
        cast(LastEditedBy as int64) as last_edited_by
    from source
    where CountryID is not null
)

select * from renamed

