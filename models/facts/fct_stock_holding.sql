-- Fact Stock Holding. Replaces MigrateStagedStockHoldingData → Fact.Stock Holding.
{{ config(materialized='table') }}
with stg_base as (select * from {{ ref('stg_warehouse__stock_item_holdings') }}),
     stg as (
       select *, row_number() over (order by wwi_stock_item_id, quantity_on_hand, binlocation, last_stocktake_quantity) as _row_id
       from stg_base
     ),
     dsi as (select * from {{ ref('dim_stock_item') }}),

     stock_item_match as (
       select
         stg._row_id,
         dsi.stock_item_key,
         row_number() over (partition by stg._row_id order by dsi.valid_from desc) as rn
       from stg
       left join dsi
         on safe_cast(dsi.wwi_stock_item_id as int64) = safe_cast(stg.wwi_stock_item_id as int64)
         and current_timestamp() > dsi.valid_from
         and current_timestamp() <= dsi.valid_to
     )

select
    coalesce(si.stock_item_key, 0) as stock_item_key,
    stg.quantity_on_hand,
    coalesce(stg.binlocation, '') as bin_location,
    stg.last_stocktake_quantity,
    stg.last_cost_price,
    stg.reorderlevel as reorder_level,
    stg.target_stock_level
from stg
left join (select _row_id, stock_item_key from stock_item_match where rn = 1) si on si._row_id = stg._row_id
