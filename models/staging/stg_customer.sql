-- Staging: Sales.Customers (source for Dimension.Customer). Replaces GetCustomerUpdates → Customer_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'Customers') }}
),
renamed as (
    select
        {{ cross_db_safe_cast_int('customerid') }} as wwi_customer_id,
        customername as customer,
        {{ cross_db_safe_cast_int('billtocustomerid') }} as bill_to_customer_id,
        {{ cross_db_safe_cast_int('customercategoryid') }} as customer_category_id,
        {{ cross_db_safe_cast_int('buyinggroupid') }} as buying_group_id,
        {{ cross_db_safe_cast_int('primarycontactpersonid') }} as primary_contact_personid,
        deliverypostalcode as postal_code,
        {{ cross_db_cast_timestamp('validfrom') }} as valid_from,
        {{ cross_db_cast_timestamp('validto') }} as valid_to
    from source
)
select * from renamed
