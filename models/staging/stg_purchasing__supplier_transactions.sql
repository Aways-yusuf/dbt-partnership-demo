-- Staging: Purchasing.SupplierTransactions (part of Fact.Transaction). Replaces GetTransactionUpdates.
{{ config(materialized='view') }}
with source as (select * from {{ source('wwi_oltp', 'SupplierTransactions') }})
select
    supplier_transaction_id as wwi_supplier_transaction_id,
    cast(transaction_date as date) as date_key,
    supplier_id as wwi_supplier_id,
    transaction_type_id as wwi_transaction_type_id,
    payment_method_id as wwi_payment_method_id,
    purchase_order_id as wwi_purchase_order_id,
    supplier_invoice_number,
    amount_excluding_tax as total_excluding_tax,
    tax_amount,
    coalesce(transaction_amount, amount_excluding_tax + tax_amount) as total_including_tax,
    outstanding_balance,
    is_finalized,
    last_edited_when as last_modified_when
from source