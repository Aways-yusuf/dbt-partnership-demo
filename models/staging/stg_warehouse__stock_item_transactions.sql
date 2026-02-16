-- Staging: Warehouse.StockItemTransactions (source for Fact.Movement). Replaces GetMovementUpdates → Movement_Staging.
{{ config(materialized='view') }}
with source as (select * from {{ source('wwi_oltp', 'StockItemTransactions') }})
select
    stockitemtransactionid as wwi_stock_item_transaction_id,
    cast(transactionoccurredwhen as date) as date_key,
    stockitemid as wwi_stock_item_id,
    customerid as wwi_customer_id,
    supplierid as wwi_supplier_id,
    transactiontypeid as wwi_transaction_type_id,
    invoiceid as wwi_invoice_id,
    purchaseorderid as wwi_purchase_order_id,
    quantity,
    lasteditedwhen as last_modified_when
from source