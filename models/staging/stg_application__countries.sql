-- Staging: Application.Countries (for Dimension.City).
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'Countries') }}
),
renamed as (
    select
        country_id,
        country_name as country,
        continent,
        region,
        subregion,
        valid_from,
        valid_to
    from source
)
select * from renamed