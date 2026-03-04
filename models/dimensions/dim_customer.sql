-- Dimension Customer (SCD Type 2). Replaces MigrateStagedCustomerData → Dimension.Customer.
-- Dependency: City dimension (load order; build City before Customer when using same run).
-- Columns align with Integration.Customer_Staging / Dimension.Customer.
{{ config(materialized='table') }}
with customer_joined as (select * from {{ ref('int_customer') }})
select
    {% if target.type == 'bigquery' %}
    cast(row_number() over (order by wwi_customer_id, valid_from) as int64) as customer_key,
    cast(wwi_customer_id as int64) as wwi_customer_id,
    {% else %}
    (row_number() over (order by coalesce(wwi_customer_id, 0), valid_from))::integer as customer_key,
    (case when wwi_customer_id is null or wwi_customer_id::text = 'NULL' then 0 else wwi_customer_id::integer end) as wwi_customer_id,
    {% endif %}
    customer,
    bill_to_customer,
    category,
    buying_group,
    primary_contact,
    postal_code,
    valid_from,
    valid_to
from customer_joined