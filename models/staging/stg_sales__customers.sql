-- Staging: Sales.Customers (source for Dimension.Customer). Replaces GetCustomerUpdates → Customer_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'Customers') }}
),
renamed as (
    select
        customerid as wwi_customer_id,
        customername as customer,
        billtocustomerid as bill_to_customer_id,
        customercategoryid as customer_category_id,
        buyinggroupid as buying_group_id,
        primarycontactpersonid as primary_contact_person_id,
        deliverypostalcode as postal_code,
        validfrom,
        validto
    from source
)
select * from renamed