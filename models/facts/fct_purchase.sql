-- Fact Purchase. Replaces MigrateStagedPurchaseData → Fact.Purchase.
{{ config(materialized='table') }}
with stg as (select * from {{ ref('stg_purchasing__purchase_order_lines') }}),
     dsu as (select * from {{ ref('dim_supplier') }}),
     dsi as (select * from {{ ref('dim_stock_item') }})
select
    stg.date_key,
    coalesce((select supplier_key from dsu where dsu.wwi_supplier_id = stg.wwi_supplier_id and stg.last_modified_when > dsu.valid_from and stg.last_modified_when <= dsu.valid_to order by dsu.valid_from desc limit 1), 0) as supplier_key,
    coalesce((select stock_item_key from dsi where dsi.wwi_stock_item_id = stg.wwi_stock_item_id and stg.last_modified_when > dsi.valid_from and stg.last_modified_when <= dsi.valid_to order by dsi.valid_from desc limit 1), 0) as stock_item_key,
    stg.wwi_purchase_order_id,
    stg.ordered_outers,
    stg.ordered_quantity,
    stg.received_outers,
    stg.package,
    stg.is_order_finalized
from stg