-- Dimension Transaction Type (SCD2). Replaces MigrateStagedTransactionTypeData → Dimension.Transaction Type.
{{ config(materialized='table') }}
with tt as (select * from {{ ref('stg_application__transaction_types') }}),
with_valid_to as (
    select wwi_transaction_type_id, transaction_type, valid_from,
           coalesce(lead(valid_from) over (partition by wwi_transaction_type_id order by valid_from), timestamp('9999-12-31 23:59:59.999999')) as valid_to
    from tt
)
select row_number() over (order by wwi_transaction_type_id, valid_from) as transaction_type_key,
       wwi_transaction_type_id, transaction_type, valid_from, valid_to
from with_valid_to