-- Dimension Payment Method (SCD2). Replaces MigrateStagedPaymentMethodData → Dimension.Payment Method.
{{ config(materialized='table') }}
with pm as (select * from {{ ref('stg_application__payment_methods') }}),
with_valid_to as (
    select
        wwi_payment_method_id,
        payment_method,
        safe_cast(ValidFrom as timestamp) as valid_from,
        coalesce(safe_cast(lead(ValidFrom) over (partition by wwi_payment_method_id order by ValidFrom) as timestamp), timestamp('9999-12-31 23:59:59.999999')) as valid_to
    from pm
)
select
    row_number() over (order by wwi_payment_method_id, valid_from) as payment_method_key,
    wwi_payment_method_id,
    payment_method,
    valid_from,
    valid_to
from with_valid_to