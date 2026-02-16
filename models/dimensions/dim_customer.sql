-- Dimension Customer (SCD Type 2). Replaces MigrateStagedCustomerData → Dimension.Customer.
-- Dependency: City dimension (load order; build City before Customer when using same run).
-- Columns align with Integration.Customer_Staging / Dimension.Customer.
{{ config(materialized='table', schema='dimensions') }}
with customer_joined as (select * from {{ ref('int_customer__joined') }})
select
    row_number() over (order by wwi_customer_id, valid_from) as customer_key,
    wwi_customer_id,
    customer,
    bill_to_customer,
    category,
    buying_group,
    primary_contact,
    postal_code,
    valid_from,
    valid_to
from customer_joined