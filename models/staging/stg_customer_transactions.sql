{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'CustomerTransactions') }}
),

renamed as (
    select
        cast(CustomerTransactionID as int64) as customer_transaction_id,
        cast(CustomerID as int64) as customer_id,
        cast(TransactionTypeID as int64) as transaction_type_id,
        safe_cast(InvoiceID as int64) as invoice_id,
        safe_cast(PaymentMethodID as int64) as payment_method_id,
        cast(TransactionDate as date) as transaction_date,
        cast(AmountExcludingTax as float64) as amount_excluding_tax,
        cast(TaxAmount as float64) as tax_amount,
        cast(TransactionAmount as float64) as transaction_amount,
        cast(OutstandingBalance as float64) as outstanding_balance,
        cast(FinalizationDate as date) as finalization_date,
        cast(IsFinalized as int64) as is_finalized,
        cast(LastEditedBy as int64) as last_edited_by,
        {{ parse_timestamp('LastEditedWhen') }} as last_edited_when
    from source
    where CustomerTransactionID is not null
)

select * from renamed

