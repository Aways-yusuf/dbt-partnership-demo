-- Intermediate: Customer + Category + Buying Group + Primary Contact + Bill To name (point-in-time).
-- Replaces GetCustomerUpdates logic: SCD Type 2 with Category, Buying Group, Primary Contact from lookups.
-- Uses actual source column names: PascalCase (CustomerCategoryID, BuyingGroupName, FullName, etc.).
{{ config(materialized='view') }}
with customers as (select * from {{ ref('stg_sales__customers') }}),
     categories as (
         select
             safe_cast(CustomerCategoryID as int64) as CustomerCategoryID,
             CustomerCategoryName,
             safe_cast(ValidFrom as timestamp) as ValidFrom,
             safe_cast(ValidTo as timestamp) as ValidTo
         from {{ source('wwi_oltp', 'CustomerCategories') }}
     ),
     buying_groups as (
         select
             safe_cast(BuyingGroupID as int64) as BuyingGroupID,
             BuyingGroupName,
             safe_cast(ValidFrom as timestamp) as ValidFrom,
             safe_cast(ValidTo as timestamp) as ValidTo
         from {{ source('wwi_oltp', 'BuyingGroups') }}
     ),
     people as (
         select
             safe_cast(PersonID as int64) as PersonID,
             FullName,
             ValidFrom,
             ValidTo
         from {{ source('wwi_oltp', 'People') }}
     ),
     bill_to as (
         select
             safe_cast(CustomerID as int64) as CustomerID,
             CustomerName,
             ValidFrom,
             ValidTo
         from {{ source('wwi_oltp', 'Customers') }}
     ),
customer_enriched as (
    select
        c.wwi_customer_id,
        c.customer,
        c.postal_code,
        c.ValidFrom as valid_from,
        c.ValidTo as valid_to,
        coalesce(bt.CustomerName, c.customer) as bill_to_customer,
        coalesce(cc.CustomerCategoryName, 'Unknown') as category,
        coalesce(bg.BuyingGroupName, 'None') as buying_group,
        coalesce(p.FullName, '') as primary_contact
    from customers c
    left join categories cc
        on safe_cast(c.CustomerCategoryID as int64) = cc.CustomerCategoryID
        and cc.ValidFrom <= c.ValidFrom
        and (cc.ValidTo is null or cc.ValidTo > c.ValidFrom)
    left join buying_groups bg
        on safe_cast(c.BuyingGroupID as int64) = bg.BuyingGroupID
        and bg.ValidFrom <= c.ValidFrom
        and (bg.ValidTo is null or bg.ValidTo > c.ValidFrom)
    left join people p
        on safe_cast(c.PrimaryContactPersonID as int64) = p.PersonID
        and p.ValidFrom <= c.ValidFrom
        and (p.ValidTo is null or p.ValidTo > c.ValidFrom)
    left join bill_to bt
        on safe_cast(c.BillToCustomerID as int64) = bt.CustomerID
        and bt.ValidFrom <= c.ValidFrom
        and (bt.ValidTo is null or bt.ValidTo > c.ValidFrom)
),
with_valid_to as (
    select
        wwi_customer_id,
        customer,
        bill_to_customer,
        category,
        buying_group,
        primary_contact,
        postal_code,
        valid_from,
        coalesce(lead(valid_from) over (partition by wwi_customer_id order by valid_from), timestamp('9999-12-31 23:59:59.999999')) as valid_to
    from customer_enriched
)
select * from with_valid_to
