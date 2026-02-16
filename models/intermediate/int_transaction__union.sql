-- Union of Customer and Supplier transactions for Fact.Transaction. Replaces GetTransactionUpdates.
{{ config(materialized='view', schema='intermediate') }}
with ct as (
    select date_key, 
    safe_cast(wwi_customer_transaction_id as int64) as wwi_customer_transaction_id,
    cast(null as int64) as wwi_supplier_transaction_id,
    safe_cast(wwi_customer_id as int64) as wwi_customer_id, 
    safe_cast(wwi_bill_to_customer_id as int64) as wwi_bill_to_customer_id, 
    cast(null as int64) as wwi_supplier_id,
    wwi_transaction_type_id, 
    safe_cast(wwi_payment_method_id as int64) as wwi_payment_method_id, 
    safe_cast(wwi_invoice_id as int64) as wwi_invoice_id, 
    cast(null as int64) as wwi_purchase_order_id, 
    cast(null as string) as supplier_invoice_number,
    total_excluding_tax, taxamount as tax_amount, 
    total_including_tax, 
    coalesce(outstandingbalance, 0) as outstanding_balance, 
    coalesce(isfinalized, 0) as is_finalized, 
    last_modified_when
from {{ ref('stg_sales__customer_transactions') }}
),
st as (
    select date_key, 
    cast(null as int64) as wwi_customer_transaction_id, 
    wwi_supplier_transaction_id,
    cast(null as int64) as wwi_customer_id, 
    cast(null as int64) as wwi_bill_to_customer_id, 
    safe_cast(wwi_supplier_id as int64) as wwi_supplier_id,
    wwi_transaction_type_id, 
    safe_cast(wwi_payment_method_id as int64) as wwi_payment_method_id, 
    cast(null as int64) as wwi_invoice_id, 
    safe_cast(wwi_purchase_order_id as int64) as wwi_purchase_order_id, 
    supplierinvoicenumber as supplier_invoice_number,
    total_excluding_tax, 
    taxamount as tax_amount, 
    total_including_tax, 
    coalesce(outstandingbalance, 0) as outstanding_balance, 
    coalesce(isfinalized, 0) as is_finalized, 
    last_modified_when
    from {{ ref('stg_purchasing__supplier_transactions') }}
)
select * from ct
union all
select * from st