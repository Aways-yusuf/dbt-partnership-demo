-- Fact Sale. Replaces MigrateStagedSaleData → Fact.Sale.
{{ config(materialized='table', schema='facts') }}
with stg as (select * from {{ ref('stg_sales__invoice_lines') }}),
     dc as (select * from {{ ref('dim_city') }}),
     dcu as (select * from {{ ref('dim_customer') }}),
     dsi as (select * from {{ ref('dim_stock_item') }}),
     de as (select * from {{ ref('dim_employee') }})
select
    coalesce((select city_key from dc where dc.wwi_city_id = stg.wwi_city_id and stg.last_modified_when > dc.valid_from and stg.last_modified_when <= dc.valid_to order by dc.valid_from desc limit 1), 0) as city_key,
    coalesce((select customer_key from dcu where dcu.wwi_customer_id = stg.wwi_customer_id and stg.last_modified_when > dcu.valid_from and stg.last_modified_when <= dcu.valid_to order by dcu.valid_from desc limit 1), 0) as customer_key,
    coalesce((select customer_key from dcu dcu2 where dcu2.wwi_customer_id = stg.wwi_bill_to_customer_id and stg.last_modified_when > dcu2.valid_from and stg.last_modified_when <= dcu2.valid_to order by dcu2.valid_from desc limit 1), 0) as bill_to_customer_key,
    coalesce((select stock_item_key from dsi where dsi.wwi_stock_item_id = stg.wwi_stock_item_id and stg.last_modified_when > dsi.valid_from and stg.last_modified_when <= dsi.valid_to order by dsi.valid_from desc limit 1), 0) as stock_item_key,
    stg.invoice_date_key,
    stg.delivery_date_key,
    coalesce((select employee_key from de where de.wwi_employee_id = stg.wwi_salesperson_id and stg.last_modified_when > de.valid_from and stg.last_modified_when <= de.valid_to order by de.valid_from desc limit 1), 0) as salesperson_key,
    stg.wwi_invoice_id,
    stg.description,
    stg.package,
    stg.quantity,
    stg.unit_price,
    stg.tax_rate,
    stg.total_excluding_tax,
    stg.tax_amount,
    stg.profit,
    stg.total_including_tax,
    stg.total_dry_items,
    stg.total_chiller_items
from stg