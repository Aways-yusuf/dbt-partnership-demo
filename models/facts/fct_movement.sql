-- Fact Movement. Replaces MigrateStagedMovementData → Fact.Movement.
{{ config(materialized='table') }}
with stg as (select * from {{ ref('stg_warehouse__stock_item_transactions') }}),
     dsi as (select * from {{ ref('dim_stock_item') }}),
     dcu as (select * from {{ ref('dim_customer') }}),
     dsu as (select * from {{ ref('dim_supplier') }}),
     dtt as (select * from {{ ref('dim_transaction_type') }}),

     stock_item_match as (
       select
         stg.wwi_stock_item_transaction_id,
         dsi.stock_item_key,
         row_number() over (partition by stg.wwi_stock_item_transaction_id order by dsi.valid_from desc) as rn
       from stg
       left join dsi
         on safe_cast(dsi.wwi_stock_item_id as int64) = safe_cast(stg.wwi_stock_item_id as int64)
         and stg.last_modified_when > dsi.valid_from
         and stg.last_modified_when <= dsi.valid_to
     ),
     customer_match as (
       select
         stg.wwi_stock_item_transaction_id,
         dcu.customer_key,
         row_number() over (partition by stg.wwi_stock_item_transaction_id order by dcu.valid_from desc) as rn
       from stg
       left join dcu
         on safe_cast(dcu.wwi_customer_id as int64) = safe_cast(stg.wwi_customer_id as int64)
         and stg.last_modified_when > dcu.valid_from
         and stg.last_modified_when <= dcu.valid_to
     ),
     supplier_match as (
       select
         stg.wwi_stock_item_transaction_id,
         dsu.supplier_key,
         row_number() over (partition by stg.wwi_stock_item_transaction_id order by dsu.valid_from desc) as rn
       from stg
       left join dsu
         on safe_cast(dsu.wwi_supplier_id as int64) = safe_cast(stg.wwi_supplier_id as int64)
         and stg.last_modified_when > dsu.valid_from
         and stg.last_modified_when <= dsu.valid_to
     ),
     transaction_type_match as (
       select
         stg.wwi_stock_item_transaction_id,
         dtt.transaction_type_key,
         row_number() over (partition by stg.wwi_stock_item_transaction_id order by dtt.valid_from desc) as rn
       from stg
       left join dtt
         on safe_cast(dtt.wwi_transaction_type_id as int64) = safe_cast(stg.wwi_transaction_type_id as int64)
         and stg.last_modified_when > dtt.valid_from
         and stg.last_modified_when <= dtt.valid_to
     )

select
    stg.date_key as date_key,
    coalesce(si.stock_item_key, 0) as stock_item_key,
    coalesce(cu.customer_key, 0) as customer_key,
    coalesce(su.supplier_key, 0) as supplier_key,
    coalesce(tt.transaction_type_key, 0) as transaction_type_key,
    stg.wwi_stock_item_transaction_id,
    stg.wwi_invoice_id,
    stg.wwi_purchase_order_id,
    stg.quantity
from stg
left join (select wwi_stock_item_transaction_id, stock_item_key from stock_item_match where rn = 1) si
    on si.wwi_stock_item_transaction_id = stg.wwi_stock_item_transaction_id
left join (select wwi_stock_item_transaction_id, customer_key from customer_match where rn = 1) cu
    on cu.wwi_stock_item_transaction_id = stg.wwi_stock_item_transaction_id
left join (select wwi_stock_item_transaction_id, supplier_key from supplier_match where rn = 1) su
    on su.wwi_stock_item_transaction_id = stg.wwi_stock_item_transaction_id
left join (select wwi_stock_item_transaction_id, transaction_type_key from transaction_type_match where rn = 1) tt
    on tt.wwi_stock_item_transaction_id = stg.wwi_stock_item_transaction_id
