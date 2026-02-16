-- Staging: Application.Countries (for Dimension.City).
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'Countries') }}
),
renamed as (
    select
        countryid as country_id,
        countryname as country,
        continent,
        region,
        subregion,
        cast(validfrom as timestamp) as valid_from,
        cast(validto as timestamp) as valid_to
    from source
)
select * from renamed