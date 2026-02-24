-- Staging: Warehouse.StockItems (source for Dimension.Stock Item). Replaces GetStockItemUpdates → StockItem_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'StockItems') }}
),
renamed as (
    select
        stockitemid as wwi_stock_item_id,
        stockitemname as stock_item,
        colorid as wwi_color_id,
        unitpackageid as wwi_unit_package_id,
        outerpackageid as wwi_outer_package_id,
        brand,
        size,
        leadtimedays,
        quantityperouter,
        ischillerstock,
        barcode,
        taxrate,
        unitprice,
        recommendedretailprice,
        typicalweightperunit,
        validfrom,
        validto
    from source
)
select * from renamed
