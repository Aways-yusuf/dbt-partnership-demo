-- Fact Transaction. Replaces MigrateStagedTransactionData → Fact.Transaction.
{{ config(materialized='table') }}
with stg as (select * from {{ ref('int_transaction__union') }}),
     dcu as (select * from {{ ref('dim_customer') }}),
     dsu as (select * from {{ ref('dim_supplier') }}),
     dtt as (select * from {{ ref('dim_transaction_type') }}),
     dpm as (select * from {{ ref('dim_payment_method') }})
select
    stg.date_key,
    coalesce((select customer_key from dcu where dcu.wwi_customer_id = stg.wwi_customer_id and stg.last_modified_when > dcu.valid_from and stg.last_modified_when <= dcu.valid_to order by dcu.valid_from desc limit 1), 0) as customer_key,
    coalesce((select customer_key from dcu d2 where d2.wwi_customer_id = stg.wwi_bill_to_customer_id and stg.last_modified_when > d2.valid_from and stg.last_modified_when <= d2.valid_to order by d2.valid_from desc limit 1), 0) as bill_to_customer_key,
    coalesce((select supplier_key from dsu where dsu.wwi_supplier_id = stg.wwi_supplier_id and stg.last_modified_when > dsu.valid_from and stg.last_modified_when <= dsu.valid_to order by dsu.valid_from desc limit 1), 0) as supplier_key,
    coalesce((select transaction_type_key from dtt where dtt.wwi_transaction_type_id = stg.wwi_transaction_type_id and stg.last_modified_when > dtt.valid_from and stg.last_modified_when <= dtt.valid_to order by dtt.valid_from desc limit 1), 0) as transaction_type_key,
    coalesce((select payment_method_key from dpm where dpm.wwi_payment_method_id = stg.wwi_payment_method_id and stg.last_modified_when > dpm.valid_from and stg.last_modified_when <= dpm.valid_to order by dpm.valid_from desc limit 1), 0) as payment_method_key,
    stg.wwi_customer_transaction_id,
    stg.wwi_supplier_transaction_id,
    stg.wwi_invoice_id,
    stg.wwi_purchase_order_id,
    stg.supplier_invoice_number,
    stg.total_excluding_tax,
    stg.tax_amount,
    stg.total_including_tax,
    stg.outstanding_balance,
    stg.is_finalized
from stg