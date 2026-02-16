{{ config(materialized='view') }}

with purchase_order_lines as (
    select * from {{ ref('stg_purchase_order_lines') }}
),

purchase_orders as (
    select * from {{ ref('stg_purchase_orders') }}
)

select
    pol.purchase_order_line_id,
    pol.purchase_order_id as wwi_purchase_order_id,
    pol.stock_item_id as wwi_stock_item_id,
    po.supplier_id as wwi_supplier_id,
    po.order_date,
    pol.ordered_outers,
    pol.received_outers,
    pol.description,
    pol.package_type_id,
    pol.expected_unit_price_per_outer,
    pol.last_receipt_date,
    pol.is_order_line_finalized,
    greatest(
        pol.last_edited_when,
        po.last_edited_when
    ) as last_modified_when
from purchase_order_lines pol
inner join purchase_orders po
    on pol.purchase_order_id = po.purchase_order_id

