-- Intermediate: Customer + Category + Buying Group + Primary Contact + Bill To name (point-in-time).
-- Replaces GetCustomerUpdates logic: SCD Type 2 with Category, Buying Group, Primary Contact from lookups.
-- Dependency: City (load order only; no FK from Customer to City in DW).
{{ config(materialized='view') }}
with customers as (select * from {{ ref('stg_sales__customers') }}),
     categories as (
         select safe_cast(customercategoryid as int64) as customer_category_id, customercategoryname as customer_category_name,
                safe_cast(substr(cast(validfrom as string), 1, 26) as timestamp) as valid_from, safe_cast(substr(cast(validto as string), 1, 26) as timestamp) as valid_to
         from {{ source('wwi_oltp', 'CustomerCategories') }}
     ),
     buying_groups as (
         select safe_cast(buyinggroupid as int64) as buying_group_id, buyinggroupname as buying_group_name,
                safe_cast(substr(cast(validfrom as string), 1, 26) as timestamp) as valid_from, safe_cast(substr(cast(validto as string), 1, 26) as timestamp) as valid_to
         from {{ source('wwi_oltp', 'BuyingGroups') }}
     ),
     people as (
         select safe_cast(personid as int64) as person_id, fullname as full_name
         from {{ source('wwi_oltp', 'People') }}
     ),
     bill_to as (
         select safe_cast(customerid as int64) as customer_id, customername as customer_name,
                safe_cast(substr(cast(validfrom as string), 1, 26) as timestamp) as valid_from, safe_cast(substr(cast(validto as string), 1, 26) as timestamp) as valid_to
         from {{ source('wwi_oltp', 'Customers') }}
     ),
customer_enriched as (
    select
        c.wwi_customer_id,
        c.customer,
        c.postal_code,
        c.valid_from,
        c.valid_to,
        coalesce(bt.customer_name, c.customer) as bill_to_customer,
        coalesce(cc.customer_category_name, 'Unknown') as category,
        coalesce(bg.buying_group_name, 'None') as buying_group,
        coalesce(p.full_name, '') as primary_contact
    from customers c
    left join categories cc on c.customer_category_id = cc.customer_category_id
        and cc.valid_from <= c.valid_from and (cc.valid_to is null or cc.valid_to > c.valid_from)
    left join buying_groups bg on c.buying_group_id = bg.buying_group_id
        and bg.valid_from <= c.valid_from and (bg.valid_to is null or bg.valid_to > c.valid_from)
    left join people p on c.primary_contact_personid = p.person_id
    left join bill_to bt on c.bill_to_customer_id = bt.customer_id
        and bt.valid_from <= c.valid_from and (bt.valid_to is null or bt.valid_to > c.valid_from)
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