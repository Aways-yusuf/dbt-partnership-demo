-- Staging: Purchasing.SupplierTransactions (part of Fact.Transaction). Replaces GetTransactionUpdates.
{{ config(materialized='view') }}
with source as (select * from {{ source('wwi_oltp', 'SupplierTransactions') }})
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
from source