-- Fact Purchase. Replaces MigrateStagedPurchaseData → Fact.Purchase.
{{ config(materialized='table') }}
with stg_base as (select * from {{ ref('int_purchase') }}),
     stg as (
       select *, row_number() over (order by wwi_purchase_order_id, wwi_stock_item_id, orderedouters, last_modified_when) as _row_id
       from stg_base
     ),
     dsu as (select * from {{ ref('dim_supplier') }}),
     dsi as (select * from {{ ref('dim_stock_item') }}),

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
     stock_item_match as (
       select
         stg._row_id,
         dsi.stock_item_key,
         row_number() over (partition by stg._row_id order by dsi.valid_from desc) as rn
       from stg
       left join dsi
         on safe_cast(dsi.wwi_stock_item_id as int64) = safe_cast(stg.wwi_stock_item_id as int64)
         and stg.last_modified_when > dsi.valid_from
         and stg.last_modified_when <= dsi.valid_to
     )

select
    stg.date_key,
    coalesce(su.supplier_key, 0) as supplier_key,
    coalesce(si.stock_item_key, 0) as stock_item_key,
    stg.wwi_purchase_order_id,
    stg.orderedouters as ordered_outers,
    stg.ordered_quantity,
    stg.receivedouters as received_outers,
    stg.package,
    stg.is_order_finalized
from stg
left join (select _row_id, supplier_key from supplier_match where rn = 1) su on su._row_id = stg._row_id
left join (select _row_id, stock_item_key from stock_item_match where rn = 1) si on si._row_id = stg._row_id
