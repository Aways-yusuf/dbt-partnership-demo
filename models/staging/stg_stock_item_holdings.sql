{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'StockItemHoldings') }}
),

renamed as (
    select
        cast(StockItemID as int64) as stock_item_id,
        cast(QuantityOnHand as int64) as quantity_on_hand,
        BinLocation as bin_location,
        cast(LastStocktakeQuantity as int64) as last_stocktake_quantity,
        cast(LastCostPrice as float64) as last_cost_price,
        cast(ReorderLevel as int64) as reorder_level,
        cast(TargetStockLevel as int64) as target_stock_level,
        cast(LastEditedBy as int64) as last_edited_by,
        {{ parse_timestamp('LastEditedWhen') }} as last_edited_when
    from source
    where StockItemID is not null
)

select * from renamed

