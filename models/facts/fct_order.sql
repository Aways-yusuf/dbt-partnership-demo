-- Fact Order. Replaces MigrateStagedOrderData → Fact.Order.
{{ config(materialized='table', schema='facts') }}
with stg as (select * from {{ ref('stg_sales__order_lines') }}),
     dc as (select * from {{ ref('dim_city') }}),
     dcu as (select * from {{ ref('dim_customer') }}),
     dsi as (select * from {{ ref('dim_stock_item') }}),
     de as (select * from {{ ref('dim_employee') }})
select
    coalesce((select city_key from dc where dc.wwi_city_id = stg.wwi_city_id and stg.last_modified_when > dc.valid_from and stg.last_modified_when <= dc.valid_to order by dc.valid_from desc limit 1), 0) as city_key,
    coalesce((select customer_key from dcu where dcu.wwi_customer_id = stg.wwi_customer_id and stg.last_modified_when > dcu.valid_from and stg.last_modified_when <= dcu.valid_to order by dcu.valid_from desc limit 1), 0) as customer_key,
    coalesce((select stock_item_key from dsi where dsi.wwi_stock_item_id = stg.wwi_stock_item_id and stg.last_modified_when > dsi.valid_from and stg.last_modified_when <= dsi.valid_to order by dsi.valid_from desc limit 1), 0) as stock_item_key,
    stg.order_date_key,
    stg.picked_date_key,
    coalesce((select employee_key from de where de.wwi_employee_id = stg.wwi_salesperson_id and stg.last_modified_when > de.valid_from and stg.last_modified_when <= de.valid_to order by de.valid_from desc limit 1), 0) as salesperson_key,
    coalesce((select employee_key from de e2 where e2.wwi_employee_id = stg.wwi_picker_id and stg.last_modified_when > e2.valid_from and stg.last_modified_when <= e2.valid_to order by e2.valid_from desc limit 1), 0) as picker_key,
    stg.wwi_order_id,
    stg.wwi_backorder_id,
    stg.description,
    stg.package,
    stg.quantity,
    stg.unit_price,
    stg.tax_rate,
    stg.total_excluding_tax,
    stg.tax_amount,
    stg.total_including_tax
from stg