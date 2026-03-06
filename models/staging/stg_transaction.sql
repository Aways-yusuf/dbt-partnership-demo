-- Staging: Union of Customer and Supplier transactions (source for Fact.Transaction). Replaces GetTransactionUpdates.
{{ config(materialized='view') }}
with ct as (
    select date_key,
    {{ cross_db_safe_cast_int('wwi_customer_transaction_id') }} as wwi_customer_transaction_id,
    {{ cross_db_null_int() }} as wwi_supplier_transaction_id,
    {{ cross_db_safe_cast_int('wwi_customer_id') }} as wwi_customer_id,
    {{ cross_db_safe_cast_int('wwi_bill_to_customer_id') }} as wwi_bill_to_customer_id,
    {{ cross_db_null_int() }} as wwi_supplier_id,
    wwi_transaction_type_id,
    {{ cross_db_safe_cast_int('wwi_payment_method_id') }} as wwi_payment_method_id,
    {{ cross_db_safe_cast_int('wwi_invoice_id') }} as wwi_invoice_id,
    {{ cross_db_null_int() }} as wwi_purchase_order_id,
    {{ cross_db_null_string() }} as supplier_invoice_number,
    total_excluding_tax, taxamount as tax_amount,
    total_including_tax,
    coalesce(outstandingbalance, 0) as outstanding_balance,
    coalesce(isfinalized, 0) as is_finalized,
    last_modified_when
    from (
        select
            customertransactionid as wwi_customer_transaction_id,
            cast(transactiondate as date) as date_key,
            customerid as wwi_customer_id,
            customerid as wwi_bill_to_customer_id,
            transactiontypeid as wwi_transaction_type_id,
            paymentmethodid as wwi_payment_method_id,
            invoiceid as wwi_invoice_id,
            amountexcludingtax as total_excluding_tax,
            taxamount,
            transactionamount as total_including_tax,
            outstandingbalance,
            isfinalized,
            lasteditedwhen as last_modified_when
        from {{ source('wwi_oltp', 'CustomerTransactions') }}
    )
),
st as (
    select date_key,
    {{ cross_db_null_int() }} as wwi_customer_transaction_id,
    wwi_supplier_transaction_id,
    {{ cross_db_null_int() }} as wwi_customer_id,
    {{ cross_db_null_int() }} as wwi_bill_to_customer_id,
    {{ cross_db_safe_cast_int('wwi_supplier_id') }} as wwi_supplier_id,
    wwi_transaction_type_id,
    {{ cross_db_safe_cast_int('wwi_payment_method_id') }} as wwi_payment_method_id,
    {{ cross_db_null_int() }} as wwi_invoice_id,
    {{ cross_db_safe_cast_int('wwi_purchase_order_id') }} as wwi_purchase_order_id,
    supplierinvoicenumber as supplier_invoice_number,
    total_excluding_tax,
    taxamount as tax_amount,
    total_including_tax,
    coalesce(outstandingbalance, 0) as outstanding_balance,
    coalesce(isfinalized, 0) as is_finalized,
    last_modified_when
    from (
        select
            suppliertransactionid as wwi_supplier_transaction_id,
            cast(transactiondate as date) as date_key,
            supplierid as wwi_supplier_id,
            transactiontypeid as wwi_transaction_type_id,
            paymentmethodid as wwi_payment_method_id,
            purchaseorderid as wwi_purchase_order_id,
            supplierinvoicenumber,
            amountexcludingtax as total_excluding_tax,
            taxamount,
            coalesce(transactionamount, amountexcludingtax + taxamount) as total_including_tax,
            outstandingbalance,
            isfinalized,
            lasteditedwhen as last_modified_when
        from {{ source('wwi_oltp', 'SupplierTransactions') }}
    )
)
select * from ct
union all
select * from st
