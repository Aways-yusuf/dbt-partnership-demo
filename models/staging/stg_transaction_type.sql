-- Staging: Application.TransactionTypes (source for Dimension.Transaction Type). Replaces GetTransactionTypeUpdates → TransactionType_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'TransactionTypes') }}
),
renamed as (
    select
        transactiontypeid as wwi_transaction_type_id,
        transactiontypename as transaction_type,
        {{ cross_db_cast_timestamp('validfrom') }} as validfrom,
        {{ cross_db_cast_timestamp('validto') }} as validto
    from source
)
select * from renamed
