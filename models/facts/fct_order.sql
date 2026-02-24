-- Fact Order. Replaces MigrateStagedOrderData → Fact.Order.
{{ config(materialized='table') }}
with stg as (select * from {{ ref('int_order') }}),
     dc as (select * from {{ ref('dim_city') }}),
     dcu as (select * from {{ ref('dim_customer') }}),
     dsi as (select * from {{ ref('dim_stock_item') }}),
     de as (select * from {{ ref('dim_employee') }}),

     city_match as (
       select
         stg.wwi_order_id,
         stg.wwi_backorder_id,
         dc.city_key,
         row_number() over (partition by stg.wwi_order_id, stg.wwi_backorder_id order by dc.valid_from desc) as rn
       from stg
       left join dc
         on safe_cast(dc.wwi_city_id as int64) = safe_cast(stg.wwi_city_id as int64)
         and stg.last_modified_when > dc.valid_from
         and stg.last_modified_when <= dc.valid_to
     ),
     customer_match as (
       select
         stg.wwi_order_id,
         stg.wwi_backorder_id,
         dcu.customer_key,
         row_number() over (partition by stg.wwi_order_id, stg.wwi_backorder_id order by dcu.valid_from desc) as rn
       from stg
       left join dcu
         on safe_cast(dcu.wwi_customer_id as int64) = safe_cast(stg.wwi_customer_id as int64)
         and stg.last_modified_when > dcu.valid_from
         and stg.last_modified_when <= dcu.valid_to
     ),
     stock_item_match as (
       select
         stg.wwi_order_id,
         stg.wwi_backorder_id,
         dsi.stock_item_key,
         row_number() over (partition by stg.wwi_order_id, stg.wwi_backorder_id order by dsi.valid_from desc) as rn
       from stg
       left join dsi
         on safe_cast(dsi.wwi_stock_item_id as int64) = safe_cast(stg.wwi_stock_item_id as int64)
         and stg.last_modified_when > dsi.valid_from
         and stg.last_modified_when <= dsi.valid_to
     ),
     salesperson_match as (
       select
         stg.wwi_order_id,
         stg.wwi_backorder_id,
         de.employee_key as salesperson_key,
         row_number() over (partition by stg.wwi_order_id, stg.wwi_backorder_id order by de.valid_from desc) as rn
       from stg
       left join de
         on safe_cast(de.wwi_employee_id as int64) = safe_cast(stg.wwi_salesperson_id as int64)
         and stg.last_modified_when > de.valid_from
         and stg.last_modified_when <= de.valid_to
     ),
     picker_match as (
       select
         stg.wwi_order_id,
         stg.wwi_backorder_id,
         de.employee_key as picker_key,
         row_number() over (partition by stg.wwi_order_id, stg.wwi_backorder_id order by de.valid_from desc) as rn
       from stg
       left join de
         on safe_cast(de.wwi_employee_id as int64) = safe_cast(stg.wwi_picker_id as int64)
         and stg.last_modified_when > de.valid_from
         and stg.last_modified_when <= de.valid_to
     )

select
    coalesce(ct.city_key, 0) as city_key,
    coalesce(cu.customer_key, 0) as customer_key,
    coalesce(si.stock_item_key, 0) as stock_item_key,
    stg.order_date_key,
    stg.picked_date_key,
    coalesce(sp.salesperson_key, 0) as salesperson_key,
    coalesce(pk.picker_key, 0) as picker_key,
    stg.wwi_order_id,
    stg.wwi_backorder_id,
    stg.description,
    stg.package,
    stg.quantity,
    stg.unitprice as unit_price,
    stg.taxrate as tax_rate,
    stg.total_excluding_tax,
    stg.tax_amount,
    stg.total_including_tax
from stg
left join (select wwi_order_id, wwi_backorder_id, city_key from city_match where rn = 1) ct
    on ct.wwi_order_id = stg.wwi_order_id and ct.wwi_backorder_id = stg.wwi_backorder_id
left join (select wwi_order_id, wwi_backorder_id, customer_key from customer_match where rn = 1) cu
    on cu.wwi_order_id = stg.wwi_order_id and cu.wwi_backorder_id = stg.wwi_backorder_id
left join (select wwi_order_id, wwi_backorder_id, stock_item_key from stock_item_match where rn = 1) si
    on si.wwi_order_id = stg.wwi_order_id and si.wwi_backorder_id = stg.wwi_backorder_id
left join (select wwi_order_id, wwi_backorder_id, salesperson_key from salesperson_match where rn = 1) sp
    on sp.wwi_order_id = stg.wwi_order_id and sp.wwi_backorder_id = stg.wwi_backorder_id
left join (select wwi_order_id, wwi_backorder_id, picker_key from picker_match where rn = 1) pk
    on pk.wwi_order_id = stg.wwi_order_id and pk.wwi_backorder_id = stg.wwi_backorder_id
