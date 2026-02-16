-- Staging: Application.StateProvinces (for Dimension.City).
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'StateProvinces') }}
),
renamed as (
    select
        stateprovinceid as state_province_id,
        stateprovincename as state_province,
        countryid as country_id,
        salesterritory as sales_territory,
        cast(validfrom as timestamp) as valid_from,
        cast(validto as timestamp) as valid_to
    from source
)
select * from renamed
