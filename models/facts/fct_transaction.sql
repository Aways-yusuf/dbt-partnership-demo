-- Fact Transaction. Replaces MigrateStagedTransactionData → Fact.Transaction.
{{ config(materialized='table') }}
with stg_base as (select * from {{ ref('int_transaction__union') }}),
     stg as (
       select *, row_number() over (order by wwi_customer_transaction_id, wwi_supplier_transaction_id, last_modified_when) as _row_id
       from stg_base
     ),
     dcu as (select * from {{ ref('dim_customer') }}),
     dsu as (select * from {{ ref('dim_supplier') }}),
     dtt as (select * from {{ ref('dim_transaction_type') }}),
     dpm as (select * from {{ ref('dim_payment_method') }}),

     customer_match as (
       select
         stg._row_id,
         dcu.customer_key,
         row_number() over (partition by stg._row_id order by dcu.valid_from desc) as rn
       from stg
       left join dcu
         on safe_cast(dcu.wwi_customer_id as int64) = safe_cast(stg.wwi_customer_id as int64)
         and stg.last_modified_when > dcu.valid_from
         and stg.last_modified_when <= dcu.valid_to
     ),
     bill_to_customer_match as (
       select
         stg._row_id,
         dcu.customer_key as bill_to_customer_key,
         row_number() over (partition by stg._row_id order by dcu.valid_from desc) as rn
       from stg
       left join dcu
         on safe_cast(dcu.wwi_customer_id as int64) = safe_cast(stg.wwi_bill_to_customer_id as int64)
         and stg.last_modified_when > dcu.valid_from
         and stg.last_modified_when <= dcu.valid_to
     ),
     supplier_match as (
       select
         stg._row_id,
         dsu.supplier_key,
         row_number() over (partition by stg._row_id order by dsu.valid_from desc) as rn
       from stg
       left join dsu
         on safe_cast(dsu.wwi_supplier_id as int64) = safe_cast(stg.wwi_supplier_id as int64)
         and stg.last_modified_when > dsu.valid_from
         and stg.last_modified_when <= dsu.valid_to
     ),
     transaction_type_match as (
       select
         stg._row_id,
         dtt.transaction_type_key,
         row_number() over (partition by stg._row_id order by dtt.valid_from desc) as rn
       from stg
       left join dtt
         on safe_cast(dtt.wwi_transaction_type_id as int64) = safe_cast(stg.wwi_transaction_type_id as int64)
         and stg.last_modified_when > dtt.valid_from
         and stg.last_modified_when <= dtt.valid_to
     ),
     payment_method_match as (
       select
         stg._row_id,
         dpm.payment_method_key,
         row_number() over (partition by stg._row_id order by dpm.valid_from desc) as rn
       from stg
       left join dpm
         on safe_cast(dpm.wwi_payment_method_id as int64) = safe_cast(stg.wwi_payment_method_id as int64)
         and stg.last_modified_when > dpm.valid_from
         and stg.last_modified_when <= dpm.valid_to
     )

select
    stg.date_key,
    coalesce(cu.customer_key, 0) as customer_key,
    coalesce(bcu.bill_to_customer_key, 0) as bill_to_customer_key,
    coalesce(su.supplier_key, 0) as supplier_key,
    coalesce(tt.transaction_type_key, 0) as transaction_type_key,
    coalesce(pm.payment_method_key, 0) as payment_method_key,
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
left join (select _row_id, customer_key from customer_match where rn = 1) cu on cu._row_id = stg._row_id
left join (select _row_id, bill_to_customer_key from bill_to_customer_match where rn = 1) bcu on bcu._row_id = stg._row_id
left join (select _row_id, supplier_key from supplier_match where rn = 1) su on su._row_id = stg._row_id
left join (select _row_id, transaction_type_key from transaction_type_match where rn = 1) tt on tt._row_id = stg._row_id
left join (select _row_id, payment_method_key from payment_method_match where rn = 1) pm on pm._row_id = stg._row_id
