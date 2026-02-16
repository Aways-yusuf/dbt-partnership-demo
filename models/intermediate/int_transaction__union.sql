-- Union of Customer and Supplier transactions for Fact.Transaction. Replaces GetTransactionUpdates.
{{ config(materialized='view', schema='intermediate') }}
with ct as (
    select date_key, wwi_customer_transaction_id, cast(null as int64) as wwi_supplier_transaction_id,
           wwi_customer_id, wwi_bill_to_customer_id, cast(null as int64) as wwi_supplier_id,
           wwi_transaction_type_id, wwi_payment_method_id, wwi_invoice_id, cast(null as int64) as wwi_purchase_order_id, cast(null as string) as supplier_invoice_number,
           total_excluding_tax, tax_amount, total_including_tax, coalesce(outstanding_balance, 0) as outstanding_balance, coalesce(is_finalized, false) as is_finalized, last_modified_when
    from {{ ref('stg_sales__customer_transactions') }}
),
st as (
    select date_key, cast(null as int64) as wwi_customer_transaction_id, wwi_supplier_transaction_id,
           cast(null as int64) as wwi_customer_id, cast(null as int64) as wwi_bill_to_customer_id, wwi_supplier_id,
           wwi_transaction_type_id, wwi_payment_method_id, cast(null as int64) as wwi_invoice_id, wwi_purchase_order_id, supplier_invoice_number,
           total_excluding_tax, tax_amount, total_including_tax, coalesce(outstanding_balance, 0) as outstanding_balance, coalesce(is_finalized, false) as is_finalized, last_modified_when
    from {{ ref('stg_purchasing__supplier_transactions') }}
)
select * from ct
union all
select * from st