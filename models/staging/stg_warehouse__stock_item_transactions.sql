-- Staging: Warehouse.StockItemTransactions (source for Fact.Movement). Replaces GetMovementUpdates → Movement_Staging.
{{ config(materialized='view') }}
with source as (select * from {{ source('wwi_oltp', 'StockItemTransactions') }})
select
    stock_item_transaction_id as wwi_stock_item_transaction_id,
    cast(transaction_occurred_when as date) as date_key,
    stock_item_id as wwi_stock_item_id,
    customer_id as wwi_customer_id,
    supplier_id as wwi_supplier_id,
    transaction_type_id as wwi_transaction_type_id,
    invoice_id as wwi_invoice_id,
    purchase_order_id as wwi_purchase_order_id,
    quantity,
    last_edited_when as last_modified_when
from source