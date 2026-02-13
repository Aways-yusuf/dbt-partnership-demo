-- Dimension Customer (SCD2). Replaces MigrateStagedCustomerData → Dimension.Customer.
{{ config(materialized='table', schema='dimensions') }}
with customers as (select * from {{ ref('stg_sales__customers') }}),
with_valid_to as (
    select wwi_customer_id, customer, bill_to_customer_id, postal_code, valid_from,
           coalesce(lead(valid_from) over (partition by wwi_customer_id order by valid_from), timestamp('9999-12-31 23:59:59.999999')) as valid_to
    from customers
)
select row_number() over (order by wwi_customer_id, valid_from) as customer_key,
       wwi_customer_id, customer, bill_to_customer_id, postal_code, valid_from, valid_to
from with_valid_to