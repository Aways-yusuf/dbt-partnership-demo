-- Staging: Sales.InvoiceLines + Invoices (source for Fact.Sale). Replaces GetSaleUpdates → Sale_Staging.
{{ config(materialized='view') }}
with invoices as (select * from {{ source('wwi_oltp', 'Invoices') }}),
     invoice_lines as (select * from {{ source('wwi_oltp', 'InvoiceLines') }}),
     customers as (select customer_id, delivery_city_id from {{ source('wwi_oltp', 'Customers') }}),
     stock_items as (select stock_item_id, is_chiller_stock from {{ source('wwi_oltp', 'StockItems') }}),
     package_types as (select package_type_id, package_type_name from {{ source('wwi_oltp', 'PackageTypes') }})
select
    cast(i.invoice_date as date) as invoice_date_key,
    cast(i.confirmed_delivery_time as date) as delivery_date_key,
    i.invoice_id as wwi_invoice_id,
    il.description,
    coalesce(pt.package_type_name, 'Unknown') as package,
    il.quantity,
    il.unit_price,
    il.tax_rate,
    il.extended_price - il.tax_amount as total_excluding_tax,
    il.tax_amount,
    il.line_profit as profit,
    il.extended_price as total_including_tax,
    if(coalesce(si.is_chiller_stock, false) = false, il.quantity, 0) as total_dry_items,
    if(coalesce(si.is_chiller_stock, false), il.quantity, 0) as total_chiller_items,
    c.delivery_city_id as wwi_city_id,
    i.customer_id as wwi_customer_id,
    i.bill_to_customer_id as wwi_bill_to_customer_id,
    il.stock_item_id as wwi_stock_item_id,
    i.salesperson_person_id as wwi_salesperson_id,
    greatest(coalesce(il.last_edited_when, i.last_edited_when), coalesce(i.last_edited_when, il.last_edited_when)) as last_modified_when
from invoices i
join invoice_lines il on i.invoice_id = il.invoice_id
join customers c on i.customer_id = c.customer_id
left join stock_items si on il.stock_item_id = si.stock_item_id
left join package_types pt on il.package_type_id = pt.package_type_id