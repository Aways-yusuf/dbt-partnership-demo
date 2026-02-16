-- Staging: Application.PaymentMethods (source for Dimension.Payment Method). Replaces GetPaymentMethodUpdates → PaymentMethod_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'PaymentMethods') }}
),
renamed as (
    select
        payment_method_id as wwi_payment_method_id,
        payment_method_name as payment_method,
        valid_from,
        valid_to
    from source
)
select * from renamed