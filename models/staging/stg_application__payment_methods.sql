-- Staging: Application.PaymentMethods (source for Dimension.Payment Method). Replaces GetPaymentMethodUpdates → PaymentMethod_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'PaymentMethods') }}
),
renamed as (
    select
        paymentmethodid as wwi_payment_method_id,
        paymentmethodname as payment_method,
        safe_cast(substr(cast(validfrom as string), 1, 26) as timestamp) as validfrom,
        safe_cast(substr(cast(validfrom as string), 1, 26) as timestamp) as validto
    from source
)
select * from renamed