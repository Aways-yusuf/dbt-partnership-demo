-- Staging: Warehouse.StockItemHoldings (source for Fact.Stock Holding). Replaces GetStockHoldingUpdates → StockHolding_Staging.
{{ config(materialized='view') }}
with source as (select * from {{ source('wwi_oltp', 'StockItemHoldings') }})
select
    stockitemid as wwi_stock_item_id,
    quantityonhand as quantity_on_hand,
    binlocation,
    laststocktakequantity as last_stocktake_quantity,
    lastcostprice as last_cost_price,
    reorderlevel,
    targetstocklevel as target_stock_level
from source
