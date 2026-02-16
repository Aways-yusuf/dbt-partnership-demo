-- Staging: Sales.InvoiceLines + Invoices (source for Fact.Sale). Replaces GetSaleUpdates → Sale_Staging.
{{ config(materialized='view') }}
with invoices as (select * from {{ source('wwi_oltp', 'Invoices') }}),
     invoice_lines as (select * from {{ source('wwi_oltp', 'InvoiceLines') }}),
<<<<<<< HEAD
     customers as (select customerid, deliverycityid from {{ source('wwi_oltp', 'Customers') }}),
     stock_items as (select stockitemid, ischillerstock from {{ source('wwi_oltp', 'StockItems') }}),
     package_types as (select packagetypeid, packagetypename from {{ source('wwi_oltp', 'PackageTypes') }})
=======
     customers as (select customer_id, delivery_city_id from {{ source('wwi_oltp', 'Customers') }}),
     stock_items as (select stock_item_id, is_chiller_stock from {{ source('wwi_oltp', 'StockItems') }}),
     package_types as (select package_type_id, package_type_name from {{ source('wwi_oltp', 'PackageTypes') }})
>>>>>>> aa729c57a4de3ef4f25d7f5d8895df9672bb50dd
select
    cast(i.invoicedate as date) as invoice_date_key,
    cast(i.confirmeddeliverytime as date) as delivery_date_key,
    i.invoiceid as wwi_invoice_id,
    il.description,
    coalesce(pt.packagetypename, 'Unknown') as package,
    il.quantity,
    il.unitprice,
    il.taxrate,
    il.extendedprice - il.taxamount as total_excluding_tax,
    il.taxamount,
    il.lineprofit as profit,
    il.extendedprice as total_including_tax,
    if(coalesce(si.ischillerstock, 0) = 0, il.quantity, 0) as total_dry_items,
    if(coalesce(si.ischillerstock, 0) != 0, il.quantity, 0) as total_chiller_items,
    c.deliverycityid as wwi_city_id,
    i.customerid as wwi_customer_id,
    i.billtocustomerid as wwi_bill_to_customer_id,
    il.stockitemid as wwi_stock_item_id,
    i.salespersonpersonid as wwi_salesperson_id,
    greatest(coalesce(il.lasteditedwhen, i.lasteditedwhen), coalesce(i.lasteditedwhen, il.lasteditedwhen)) as last_modified_when
from invoices i
join invoice_lines il on i.invoiceid = il.invoiceid
join customers c on i.customerid = c.customerid
left join stock_items si on il.stockitemid = si.stockitemid
left join package_types pt on il.packagetypeid = pt.packagetypeid