{{ config(materialized='view') }}

with customers as (
    select * from {{ ref('stg_customers') }}
),

customer_categories as (
    select * from {{ ref('stg_customer_categories') }}
),

buying_groups as (
    select * from {{ ref('stg_buying_groups') }}
),

people as (
    select * from {{ ref('stg_people') }}
)

select
    c.wwi_customer_id,
    c.customer_name as customer,
    bt.customer_name as bill_to_customer,
    cat.customer_category_name as category,
    bg.buying_group_name as buying_group,
    p.full_name as primary_contact,
    c.delivery_postal_code as postal_code,
    c.valid_from,
    c.valid_to,
    c.last_edited_by
from customers c
left join customers bt
    on c.bill_to_customer_id = bt.wwi_customer_id
    and c.valid_from >= bt.valid_from
    and c.valid_from < coalesce(bt.valid_to, timestamp('9999-12-31 23:59:59.999999'))
left join customer_categories cat
    on c.customer_category_id = cat.customer_category_id
    and c.valid_from >= cat.valid_from
    and c.valid_from < coalesce(cat.valid_to, timestamp('9999-12-31 23:59:59.999999'))
left join buying_groups bg
    on c.buying_group_id = bg.buying_group_id
    and c.valid_from >= bg.valid_from
    and c.valid_from < coalesce(bg.valid_to, timestamp('9999-12-31 23:59:59.999999'))
left join people p
    on c.primary_contact_person_id = p.wwi_person_id
    and c.valid_from >= p.valid_from
    and c.valid_from < coalesce(p.valid_to, timestamp('9999-12-31 23:59:59.999999'))

