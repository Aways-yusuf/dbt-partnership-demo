-- Staging: Application.TransactionTypes (source for Dimension.Transaction Type). Replaces GetTransactionTypeUpdates → TransactionType_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'TransactionTypes') }}
),
renamed as (
    select
        transaction_type_id as wwi_transaction_type_id,
        transaction_type_name as transaction_type,
        valid_from,
        valid_to
    from source
)
select * from renamed