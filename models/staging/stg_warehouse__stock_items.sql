-- Staging: Warehouse.StockItems (source for Dimension.Stock Item). Replaces GetStockItemUpdates → StockItem_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'StockItems') }}
),
renamed as (
    select
        stock_item_id as wwi_stock_item_id,
        stock_item_name as stock_item,
        color_id,
        unit_package_id,
        outer_package_id,
        brand,
        size,
        lead_time_days,
        quantity_per_outer,
        is_chiller_stock,
        barcode,
        tax_rate,
        unit_price,
        recommended_retail_price,
        typical_weight_per_unit,
        valid_from,
        valid_to
    from source
)
select * from renamed