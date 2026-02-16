{{ config(materialized='view') }}

with invoice_lines as (
    select * from {{ ref('stg_invoice_lines') }}
),

invoices as (
    select * from {{ ref('stg_invoices') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
)

select
    il.invoice_line_id,
    il.invoice_id as wwi_invoice_id,
    il.stock_item_id as wwi_stock_item_id,
    i.customer_id as wwi_customer_id,
    i.bill_to_customer_id as wwi_bill_to_customer_id,
    c.delivery_city_id as wwi_city_id,
    i.salesperson_person_id as wwi_salesperson_id,
    i.invoice_date,
    i.confirmed_delivery_time,
    il.description,
    il.package_type_id,
    il.quantity,
    il.unit_price,
    il.tax_rate,
    il.extended_price as total_excluding_tax,
    il.tax_amount,
    il.line_profit as profit,
    il.extended_price + il.tax_amount as total_including_tax,
    i.total_dry_items,
    i.total_chiller_items,
    greatest(
        il.last_edited_when,
        i.last_edited_when
    ) as last_modified_when
from invoice_lines il
inner join invoices i
    on il.invoice_id = i.invoice_id
inner join customers c
    on i.customer_id = c.wwi_customer_id

