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
        safe_cast(substr(cast(validfrom as string), 1, 26) as timestamp) as valid_from,
        safe_cast(substr(cast(validto as string), 1, 26) as timestamp) as valid_to
    from source
)
select * from renamed