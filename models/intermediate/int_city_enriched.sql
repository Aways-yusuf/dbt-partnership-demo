{{ config(materialized='view') }}

with cities as (
    select * from {{ ref('stg_cities') }}
),

state_provinces as (
    select * from {{ ref('stg_state_provinces') }}
),

countries as (
    select * from {{ ref('stg_countries') }}
)

select
    c.wwi_city_id,
    c.city_name as city,
    sp.state_province_name as state_province,
    co.country_name as country,
    co.continent,
    sp.sales_territory,
    co.region,
    co.subregion,
    c.location,
    c.latest_recorded_population,
    c.valid_from,
    c.valid_to,
    c.last_edited_by
from cities c
left join state_provinces sp
    on c.state_province_id = sp.state_province_id
    and c.valid_from >= sp.valid_from
    and c.valid_from < coalesce(sp.valid_to, timestamp('9999-12-31 23:59:59.999999'))
left join countries co
    on sp.country_id = co.country_id
    and c.valid_from >= co.valid_from
    and c.valid_from < coalesce(co.valid_to, timestamp('9999-12-31 23:59:59.999999'))

