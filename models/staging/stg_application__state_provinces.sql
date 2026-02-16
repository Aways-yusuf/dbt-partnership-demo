-- Staging: Application.StateProvinces (for Dimension.City).
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'StateProvinces') }}
),
renamed as (
    select
        state_province_id,
        state_province_name as state_province,
        country_id,
        sales_territory,
        valid_from,
        valid_to
    from source
)
select * from renamed
