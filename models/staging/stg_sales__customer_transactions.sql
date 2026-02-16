-- Staging: Sales.CustomerTransactions (part of Fact.Transaction). Replaces GetTransactionUpdates.
{{ config(materialized='view') }}
with source as (select * from {{ source('wwi_oltp', 'CustomerTransactions') }})
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
from source