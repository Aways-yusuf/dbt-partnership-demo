-- Staging: Sales.Customers (source for Dimension.Customer). Replaces GetCustomerUpdates → Customer_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'Customers') }}
),
renamed as (
    select
        safe_cast(customerid as int64) as wwi_customer_id,
        customername as customer,
        safe_cast(billtocustomerid as int64) as bill_to_customer_id,
        safe_cast(customercategoryid as int64) as customer_category_id,
        safe_cast(buyinggroupid as int64) as buying_group_id,
        safe_cast(primarycontactpersonid as int64) as primary_contact_personid,
        deliverypostalcode as postal_code,
        safe_cast(substr(cast(validfrom as string), 1, 26) as timestamp) as valid_from,
        safe_cast(substr(cast(validto as string), 1, 26) as timestamp) as valid_to
    from source
)
select * from renamed