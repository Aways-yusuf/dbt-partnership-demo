{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'Orders') }}
),

renamed as (
    select
        cast(OrderID as int64) as order_id,
        cast(CustomerID as int64) as customer_id,
        cast(SalespersonPersonID as int64) as salesperson_person_id,
        safe_cast(PickedByPersonID as int64) as picked_by_person_id,
        cast(ContactPersonID as int64) as contact_person_id,
        safe_cast(BackorderOrderID as int64) as backorder_order_id,
        cast(OrderDate as date) as order_date,
        cast(ExpectedDeliveryDate as date) as expected_delivery_date,
        safe_cast(CustomerPurchaseOrderNumber as int64) as customer_purchase_order_number,
        cast(IsUndersupplyBackordered as int64) as is_undersupply_backordered,
        Comments as comments,
        DeliveryInstructions as delivery_instructions,
        InternalComments as internal_comments,
        {{ parse_timestamp('PickingCompletedWhen') }} as picking_completed_when,
        cast(LastEditedBy as int64) as last_edited_by,
        {{ parse_timestamp('LastEditedWhen') }} as last_edited_when
    from source
    where OrderID is not null
)

select * from renamed

