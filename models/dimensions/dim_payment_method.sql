-- Dimension Payment Method (SCD2). Replaces MigrateStagedPaymentMethodData → Dimension.Payment Method.
{{ config(materialized='table') }}
with pm as (select * from {{ ref('int_payment_method') }}),
with_valid_to as (
    select wwi_payment_method_id, payment_method, validfrom as valid_from,
           coalesce(lead(validfrom) over (partition by wwi_payment_method_id order by validfrom), {{ cross_db_timestamp_max() }}) as valid_to
    from pm
)
select row_number() over (order by wwi_payment_method_id, valid_from) as payment_method_key,
       wwi_payment_method_id, payment_method, valid_from, valid_to
from with_valid_to