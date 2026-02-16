-- Staging: Sales.CustomerTransactions (part of Fact.Transaction). Replaces GetTransactionUpdates.
{{ config(materialized='view') }}
with source as (select * from {{ source('wwi_oltp', 'customer_transactions') }})
select
    customer_transaction_id as wwi_customer_transaction_id,
    cast(transaction_date as date) as date_key,
    customer_id as wwi_customer_id,
    bill_to_customer_id as wwi_bill_to_customer_id,
    transaction_type_id as wwi_transaction_type_id,
    payment_method_id as wwi_payment_method_id,
    invoice_id as wwi_invoice_id,
    amount_excluding_tax as total_excluding_tax,
    tax_amount,
    transaction_amount as total_including_tax,
    outstanding_balance,
    is_finalized,
    last_edited_when as last_modified_when
from source