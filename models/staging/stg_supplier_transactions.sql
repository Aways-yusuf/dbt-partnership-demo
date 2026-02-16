{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'SupplierTransactions') }}
),

renamed as (
    select
        cast(SupplierTransactionID as int64) as supplier_transaction_id,
        cast(SupplierID as int64) as supplier_id,
        cast(TransactionTypeID as int64) as transaction_type_id,
        safe_cast(PurchaseOrderID as int64) as purchase_order_id,
        cast(PaymentMethodID as int64) as payment_method_id,
        SupplierInvoiceNumber as supplier_invoice_number,
        cast(TransactionDate as date) as transaction_date,
        cast(AmountExcludingTax as float64) as amount_excluding_tax,
        cast(TaxAmount as float64) as tax_amount,
        cast(TransactionAmount as float64) as transaction_amount,
        cast(OutstandingBalance as float64) as outstanding_balance,
        {{ parse_timestamp('FinalizationDate') }} as finalization_date,
        cast(IsFinalized as int64) as is_finalized,
        cast(LastEditedBy as int64) as last_edited_by,
        {{ parse_timestamp('LastEditedWhen') }} as last_edited_when
    from source
    where SupplierTransactionID is not null
)

select * from renamed

