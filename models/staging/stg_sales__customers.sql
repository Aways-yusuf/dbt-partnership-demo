-- Staging: Sales.Customers (source for Dimension.Customer). Replaces GetCustomerUpdates → Customer_Staging.
{{ config(materialized='view') }}
with source as (
    select * from {{ source('wwi_oltp', 'Customers') }}
),
renamed as (
    select
        CustomerID as wwi_customer_id,
        CustomerName as customer,
        BillToCustomerID,
        CustomerCategoryID,
        BuyingGroupID,
        PrimaryContactPersonID,
        DeliveryPostalCode as postal_code,
        ValidFrom,
        ValidTo
    from source
)
select * from renamed