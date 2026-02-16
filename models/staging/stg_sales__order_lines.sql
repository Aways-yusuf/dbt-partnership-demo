-- Staging: Sales.Orders + OrderLines (source for Fact.Order). Replaces GetOrderUpdates → Order_Staging.
{{ config(materialized='view') }}
with orders as (select * from {{ source('wwi_oltp', 'Orders') }}),
     order_lines as (select * from {{ source('wwi_oltp', 'OrderLines') }}),
     customers as (select customerid, deliverycityid from {{ source('wwi_oltp', 'Customers') }}),
     package_types as (select packagetypeid, packagetypename from {{ source('wwi_oltp', 'PackageTypes') }})
select
    o.orderid as wwi_order_id,
    ol.orderlineid as wwi_backorder_id,
    c.deliverycityid as wwi_city_id,
    o.customerid as wwi_customer_id,
    ol.stockitemid as wwi_stock_item_id,
    cast(o.orderdate as date) as order_date_key,
    cast(ol.pickingcompletedwhen as date) as picked_date_key,
    o.salespersonpersonid as wwi_salesperson_id,
    o.pickedbypersonid as wwi_picker_id,
    ol.description,
    coalesce(pt.packagetypename, 'Unknown') as package,
    ol.quantity,
    ol.unitprice,
    ol.taxrate,
    ol.quantity * ol.unitprice as total_excluding_tax,
    (ol.quantity * ol.unitprice) * ol.taxrate / 100 as tax_amount,
    (ol.quantity * ol.unitprice) * (1 + ol.taxrate / 100) as total_including_tax,
    greatest(coalesce(ol.lasteditedwhen, o.lasteditedwhen), coalesce(o.lasteditedwhen, ol.lasteditedwhen)) as last_modified_when
from orders o
join order_lines ol on o.orderid = ol.orderid
join customers c on o.customerid = c.customerid
left join package_types pt on ol.packagetypeid = pt.packagetypeid