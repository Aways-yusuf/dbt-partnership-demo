{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'StockItemTransactions') }}
),

renamed as (
    select
        cast(StockItemTransactionID as int64) as stock_item_transaction_id,
        cast(StockItemID as int64) as stock_item_id,
        cast(TransactionTypeID as int64) as transaction_type_id,
        safe_cast(CustomerID as int64) as customer_id,
        safe_cast(InvoiceID as int64) as invoice_id,
        safe_cast(SupplierID as int64) as supplier_id,
        safe_cast(PurchaseOrderID as int64) as purchase_order_id,
        {{ parse_timestamp('TransactionOccurredWhen') }} as transaction_occurred_when,
        cast(Quantity as float64) as quantity,
        cast(LastEditedBy as int64) as last_edited_by,
        {{ parse_timestamp('LastEditedWhen') }} as last_edited_when
    from source
    where StockItemTransactionID is not null
        and StockItemID is not null
)

select * from renamed

