-- Staging: Sales.Customers (source for Dimension.Customer). Replaces GetCustomerUpdates → Customer_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'customers') }}
),
renamed as (
    select
        customer_id as wwi_customer_id,
        customer_name as customer,
        bill_to_customer_id,
        customer_category_id,
        buying_group_id,
        primary_contact_person_id,
        delivery_postal_code as postal_code,
        valid_from,
        valid_to
    from source
)
select * from renamed