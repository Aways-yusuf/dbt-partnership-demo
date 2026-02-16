-- Staging: Sales.Orders + OrderLines (source for Fact.Order). Replaces GetOrderUpdates → Order_Staging.
{{ config(materialized='view') }}
with orders as (select * from {{ source('wwi_oltp', 'Orders') }}),
     order_lines as (select * from {{ source('wwi_oltp', 'OrderLines') }}),
     customers as (select customer_id, delivery_city_id from {{ source('wwi_oltp', 'Customers') }}),
     package_types as (select package_type_id, package_type_name from {{ source('wwi_oltp', 'PackageTypes') }})
select
    o.order_id as wwi_order_id,
    ol.order_line_id as wwi_backorder_id,
    c.delivery_city_id as wwi_city_id,
    o.customer_id as wwi_customer_id,
    ol.stock_item_id as wwi_stock_item_id,
    cast(o.order_date as date) as order_date_key,
    cast(ol.picking_completed_when as date) as picked_date_key,
    o.salesperson_person_id as wwi_salesperson_id,
    ol.picked_by_person_id as wwi_picker_id,
    ol.description,
    coalesce(pt.package_type_name, 'Unknown') as package,
    ol.quantity,
    ol.unit_price,
    ol.tax_rate,
    ol.quantity * ol.unit_price as total_excluding_tax,
    (ol.quantity * ol.unit_price) * ol.tax_rate / 100 as tax_amount,
    (ol.quantity * ol.unit_price) * (1 + ol.tax_rate / 100) as total_including_tax,
    greatest(coalesce(ol.last_edited_when, o.last_edited_when), coalesce(o.last_edited_when, ol.last_edited_when)) as last_modified_when
from orders o
join order_lines ol on o.order_id = ol.order_id
join customers c on o.customer_id = c.customer_id
left join package_types pt on ol.package_type_id = pt.package_type_id