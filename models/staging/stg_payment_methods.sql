{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'PaymentMethods') }}
),

renamed as (
    select
        cast(PaymentMethodID as int64) as wwi_payment_method_id,
        PaymentMethodName as payment_method_name,
        {{ parse_timestamp('ValidFrom') }} as valid_from,
        {{ parse_timestamp('ValidTo') }} as valid_to,
        cast(LastEditedBy as int64) as last_edited_by
    from source
    where PaymentMethodID is not null
)

select * from renamed

