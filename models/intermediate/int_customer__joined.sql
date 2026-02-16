-- Intermediate: Customer + Category + Buying Group + Primary Contact + Bill To name (point-in-time).
-- Replaces GetCustomerUpdates logic: SCD Type 2 with Category, Buying Group, Primary Contact from lookups.
-- Dependency: City (load order only; no FK from Customer to City in DW).
{{ config(materialized='view', schema='intermediate') }}
with customers as (select * from {{ ref('stg_sales__customers') }}),
     categories as (select * from {{ source('wwi_oltp', 'CustomerCategories') }}),
     buying_groups as (select * from {{ source('wwi_oltp', 'BuyingGroups') }}),
     people as (select personid, fullname from {{ source('wwi_oltp', 'People') }}),
     bill_to as (select customerid, customername, validfrom, validto from {{ source('wwi_oltp', 'Customers') }}),
customer_enriched as (
    select
        c.wwicustomerid,
        c.customer,
        c.postalcode,
        c.validfrom,
        c.validto,
        coalesce(bt.customername, c.customer) as bill_to_customer,
        coalesce(cc.customercategoryname, 'Unknown') as category,
        coalesce(bg.buyinggroupname, 'None') as buying_group,
        coalesce(p.fullname, '') as primary_contact
    from customers c
    left join categories cc on c.customer_category_id = cc.customercategoryid
        and cc.validfrom <= c.validfrom and (cc.validto is null or cc.validto > c.validfrom)
    left join buying_groups bg on c.buyinggroupid = bg.buyinggroupid
        and bg.validfrom <= c.validfrom and (bg.validto is null or bg.validto > c.validfrom)
    left join people p on c.primarycontactpersonid = p.personid
    left join bill_to bt on c.billtocustomerid = bt.customerid
        and bt.validfrom <= c.validfrom and (bt.validto is null or bt.validto > c.validfrom)
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