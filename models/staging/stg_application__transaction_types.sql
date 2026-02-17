-- Staging: Application.TransactionTypes (source for Dimension.Transaction Type). Replaces GetTransactionTypeUpdates → TransactionType_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'TransactionTypes') }}
),
renamed as (
    select
        transactiontypeid as wwi_transaction_type_id,
        transactiontypename as transaction_type,
        safe_cast(substr(cast(validfrom as string), 1, 26) as timestamp) as validfrom,
        safe_cast(substr(cast(validto as string), 1, 26) as timestamp) as validto,
    from source
)
select * from renamed