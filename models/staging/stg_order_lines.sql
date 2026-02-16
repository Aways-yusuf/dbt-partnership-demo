{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'OrderLines') }}
),

renamed as (
    select
        cast(OrderLineID as int64) as order_line_id,
        cast(OrderID as int64) as order_id,
        cast(StockItemID as int64) as stock_item_id,
        Description as description,
        cast(PackageTypeID as int64) as package_type_id,
        cast(Quantity as int64) as quantity,
        cast(UnitPrice as float64) as unit_price,
        cast(TaxRate as float64) as tax_rate,
        cast(PickedQuantity as int64) as picked_quantity,
        {{ parse_timestamp('PickingCompletedWhen') }} as picking_completed_when,
        cast(LastEditedBy as int64) as last_edited_by,
        {{ parse_timestamp('LastEditedWhen') }} as last_edited_when
    from source
    where OrderLineID is not null
        and OrderID is not null
        and StockItemID is not null
)

select * from renamed

