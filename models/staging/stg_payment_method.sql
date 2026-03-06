-- Staging: Application.PaymentMethods (source for Dimension.Payment Method). Replaces GetPaymentMethodUpdates → PaymentMethod_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'PaymentMethods') }}
),
renamed as (
    select
        paymentmethodid as wwi_payment_method_id,
        paymentmethodname as payment_method,
        {{ cross_db_cast_timestamp('validfrom') }} as validfrom,
        {{ cross_db_cast_timestamp('validto') }} as validto
    from source
)
select * from renamed
