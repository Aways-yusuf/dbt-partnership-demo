{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'TransactionTypes') }}
),

renamed as (
    select
        cast(TransactionTypeID as int64) as wwi_transaction_type_id,
        TransactionTypeName as transaction_type_name,
        {{ parse_timestamp('ValidFrom') }} as valid_from,
        {{ parse_timestamp('ValidTo') }} as valid_to,
        cast(LastEditedBy as int64) as last_edited_by
    from source
    where TransactionTypeID is not null
)

select * from renamed

