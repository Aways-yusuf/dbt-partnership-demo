-- Fact Stock Holding. Replaces MigrateStagedStockHoldingData → Fact.Stock Holding.
{{ config(materialized='table') }}
with stg as (select * from {{ ref('stg_warehouse__stock_item_holdings') }}),
     dsi as (select * from {{ ref('dim_stock_item') }})
select
    coalesce((select stock_item_key from dsi where dsi.wwi_stock_item_id = stg.wwi_stock_item_id and current_timestamp() > dsi.valid_from and current_timestamp() <= dsi.valid_to order by dsi.valid_from desc limit 1), 0) as stock_item_key,
    stg.quantity_on_hand,
    coalesce(stg.bin_location, '') as bin_location,
    stg.last_stocktake_quantity,
    stg.last_cost_price,
    stg.reorder_level,
    stg.target_stock_level
from stg