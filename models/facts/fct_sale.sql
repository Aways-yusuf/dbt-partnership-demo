-- Fact Sale. Replaces MigrateStagedSaleData → Fact.Sale.
{{ config(materialized='table') }}
with stg_base as (select * from {{ ref('int_sale') }}),
     stg as (
       select *, row_number() over (order by wwi_invoice_id, wwi_stock_item_id, quantity, last_modified_when) as _row_id
       from stg_base
     ),
     dc as (select * from {{ ref('dim_city') }}),
     dcu as (select * from {{ ref('dim_customer') }}),
     dsi as (select * from {{ ref('dim_stock_item') }}),
     de as (select * from {{ ref('dim_employee') }}),

     city_match as (
       select
         stg._row_id,
         dc.city_key,
         row_number() over (partition by stg._row_id order by dc.valid_from desc) as rn
       from stg
       left join dc
         on safe_cast(dc.wwi_city_id as int64) = safe_cast(stg.wwi_city_id as int64)
         and stg.last_modified_when > dc.valid_from
         and stg.last_modified_when <= dc.valid_to
     ),
     customer_match as (
       select
         stg._row_id,
         dcu.customer_key,
         row_number() over (partition by stg._row_id order by dcu.valid_from desc) as rn
       from stg
       left join dcu
         on safe_cast(dcu.wwi_customer_id as int64) = safe_cast(stg.wwi_customer_id as int64)
         and stg.last_modified_when > dcu.valid_from
         and stg.last_modified_when <= dcu.valid_to
     ),
     bill_to_customer_match as (
       select
         stg._row_id,
         dcu.customer_key as bill_to_customer_key,
         row_number() over (partition by stg._row_id order by dcu.valid_from desc) as rn
       from stg
       left join dcu
         on safe_cast(dcu.wwi_customer_id as int64) = safe_cast(stg.wwi_bill_to_customer_id as int64)
         and stg.last_modified_when > dcu.valid_from
         and stg.last_modified_when <= dcu.valid_to
     ),
     stock_item_match as (
       select
         stg._row_id,
         dsi.stock_item_key,
         row_number() over (partition by stg._row_id order by dsi.valid_from desc) as rn
       from stg
       left join dsi
         on safe_cast(dsi.wwi_stock_item_id as int64) = safe_cast(stg.wwi_stock_item_id as int64)
         and stg.last_modified_when > dsi.valid_from
         and stg.last_modified_when <= dsi.valid_to
     ),
     salesperson_match as (
       select
         stg._row_id,
         de.employee_key as salesperson_key,
         row_number() over (partition by stg._row_id order by de.valid_from desc) as rn
       from stg
       left join de
         on safe_cast(de.wwi_employee_id as int64) = safe_cast(stg.wwi_salesperson_id as int64)
         and stg.last_modified_when > de.valid_from
         and stg.last_modified_when <= de.valid_to
     )

select
    coalesce(ct.city_key, 0) as city_key,
    coalesce(cu.customer_key, 0) as customer_key,
    coalesce(bcu.bill_to_customer_key, 0) as bill_to_customer_key,
    coalesce(si.stock_item_key, 0) as stock_item_key,
    stg.invoice_date_key,
    stg.delivery_date_key,
    coalesce(sp.salesperson_key, 0) as salesperson_key,
    stg.wwi_invoice_id,
    stg.description,
    stg.package,
    stg.quantity,
    stg.unitprice as unit_price,
    stg.taxrate as tax_rate,
    stg.total_excluding_tax,
    stg.taxamount as tax_amount,
    stg.profit,
    stg.total_including_tax,
    stg.total_dry_items,
    stg.total_chiller_items
from stg
left join (select _row_id, city_key from city_match where rn = 1) ct on ct._row_id = stg._row_id
left join (select _row_id, customer_key from customer_match where rn = 1) cu on cu._row_id = stg._row_id
left join (select _row_id, bill_to_customer_key from bill_to_customer_match where rn = 1) bcu on bcu._row_id = stg._row_id
left join (select _row_id, stock_item_key from stock_item_match where rn = 1) si on si._row_id = stg._row_id
left join (select _row_id, salesperson_key from salesperson_match where rn = 1) sp on sp._row_id = stg._row_id
