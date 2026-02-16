{{ config(materialized='view') }}

with order_lines as (
    select * from {{ ref('stg_order_lines') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
)

select
    ol.order_line_id,
    ol.order_id as wwi_order_id,
    ol.stock_item_id as wwi_stock_item_id,
    o.customer_id as wwi_customer_id,
    c.delivery_city_id as wwi_city_id,
    o.salesperson_person_id as wwi_salesperson_id,
    o.picked_by_person_id as wwi_picker_id,
    o.order_date,
    ol.picking_completed_when,
    o.backorder_order_id as wwi_backorder_id,
    ol.description,
    ol.package_type_id,
    ol.quantity,
    ol.unit_price,
    ol.tax_rate,
    ol.quantity * ol.unit_price as total_excluding_tax,
    ol.quantity * ol.unit_price * ol.tax_rate / 100.0 as tax_amount,
    ol.quantity * ol.unit_price * (1 + ol.tax_rate / 100.0) as total_including_tax,
    greatest(
        ol.last_edited_when,
        o.last_edited_when
    ) as last_modified_when
from order_lines ol
inner join orders o
    on ol.order_id = o.order_id
inner join customers c
    on o.customer_id = c.wwi_customer_id

