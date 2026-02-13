-- Staging: Warehouse.StockItemHoldings (source for Fact.Stock Holding). Replaces GetStockHoldingUpdates → StockHolding_Staging.
{{ config(materialized='view') }}
with source as (select * from {{ source('wwi_oltp', 'stock_item_holdings') }})
select
    stock_item_id as wwi_stock_item_id,
    quantity_on_hand as quantity_on_hand,
    bin_location,
    last_stocktake_quantity as last_stocktake_quantity,
    last_cost_price as last_cost_price,
    reorder_level,
    target_stock_level as target_stock_level
from source