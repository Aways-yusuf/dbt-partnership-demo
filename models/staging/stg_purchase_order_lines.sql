{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'PurchaseOrderLines') }}
),

renamed as (
    select
        cast(PurchaseOrderLineID as int64) as purchase_order_line_id,
        cast(PurchaseOrderID as int64) as purchase_order_id,
        cast(StockItemID as int64) as stock_item_id,
        cast(OrderedOuters as int64) as ordered_outers,
        Description as description,
        cast(ReceivedOuters as int64) as received_outers,
        cast(PackageTypeID as int64) as package_type_id,
        cast(ExpectedUnitPricePerOuter as float64) as expected_unit_price_per_outer,
        {{ parse_timestamp('LastReceiptDate') }} as last_receipt_date,
        cast(IsOrderLineFinalized as int64) as is_order_line_finalized,
        cast(LastEditedBy as int64) as last_edited_by,
        {{ parse_timestamp('LastEditedWhen') }} as last_edited_when
    from source
    where PurchaseOrderLineID is not null
        and PurchaseOrderID is not null
        and StockItemID is not null
)

select * from renamed

