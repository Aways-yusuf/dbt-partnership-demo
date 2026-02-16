{{ config(materialized='view') }}

with customer_transactions as (
    select
        customer_transaction_id,
        customer_id as wwi_customer_id,
        null as wwi_bill_to_customer_id,
        null as wwi_supplier_id,
        transaction_type_id as wwi_transaction_type_id,
        payment_method_id as wwi_payment_method_id,
        transaction_date,
        invoice_id as wwi_invoice_id,
        null as wwi_purchase_order_id,
        null as supplier_invoice_number,
        amount_excluding_tax,
        tax_amount,
        transaction_amount,
        outstanding_balance,
        is_finalized,
        last_edited_when
    from {{ ref('stg_customer_transactions') }}
),

supplier_transactions as (
    select
        supplier_transaction_id,
        null as wwi_customer_id,
        null as wwi_bill_to_customer_id,
        supplier_id as wwi_supplier_id,
        transaction_type_id as wwi_transaction_type_id,
        payment_method_id as wwi_payment_method_id,
        transaction_date,
        null as wwi_invoice_id,
        purchase_order_id as wwi_purchase_order_id,
        supplier_invoice_number,
        amount_excluding_tax,
        tax_amount,
        transaction_amount,
        outstanding_balance,
        is_finalized,
        last_edited_when
    from {{ ref('stg_supplier_transactions') }}
)

select * from customer_transactions
union all
select * from supplier_transactions

