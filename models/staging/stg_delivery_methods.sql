{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'DeliveryMethods') }}
),

renamed as (
    select
        cast(DeliveryMethodID as int64) as delivery_method_id,
        DeliveryMethodName as delivery_method_name,
        {{ parse_timestamp('ValidFrom') }} as valid_from,
        {{ parse_timestamp('ValidTo') }} as valid_to,
        cast(LastEditedBy as int64) as last_edited_by
    from source
    where DeliveryMethodID is not null
)

select * from renamed

