{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'CustomerCategories') }}
),

renamed as (
    select
        cast(CustomerCategoryID as int64) as customer_category_id,
        CustomerCategoryName as customer_category_name,
        {{ parse_timestamp('ValidFrom') }} as valid_from,
        {{ parse_timestamp('ValidTo') }} as valid_to,
        cast(LastEditedBy as int64) as last_edited_by
    from source
    where CustomerCategoryID is not null
)

select * from renamed

