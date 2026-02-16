{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'PurchaseOrders') }}
),

renamed as (
    select
        cast(PurchaseOrderID as int64) as purchase_order_id,
        cast(SupplierID as int64) as supplier_id,
        cast(OrderDate as date) as order_date,
        cast(DeliveryMethodID as int64) as delivery_method_id,
        cast(ContactPersonID as int64) as contact_person_id,
        cast(ExpectedDeliveryDate as date) as expected_delivery_date,
        SupplierReference as supplier_reference,
        cast(IsOrderFinalized as int64) as is_order_finalized,
        Comments as comments,
        InternalComments as internal_comments,
        cast(LastEditedBy as int64) as last_edited_by,
        {{ parse_timestamp('LastEditedWhen') }} as last_edited_when
    from source
    where PurchaseOrderID is not null
)

select * from renamed

