-- Intermediate: City + State Province + Country (point-in-time). Replaces GetCityUpdates temporal logic.
{{ config(materialized='view') }}
with cities as (select * from {{ ref('stg_city') }}),
     state_provinces as (
         select
             stateprovinceid as state_province_id,
             stateprovincename as state_province,
             countryid as country_id,
             salesterritory as sales_territory,
             safe_cast(substr(cast(validfrom as string), 1, 26) as timestamp) as valid_from,
             safe_cast(substr(cast(validto as string), 1, 26) as timestamp) as valid_to
         from {{ source('wwi_oltp', 'StateProvinces') }}
     ),
     countries as (
         select
             countryid as country_id,
             countryname as country,
             continent,
             region,
             subregion,
             safe_cast(substr(cast(validfrom as string), 1, 26) as timestamp) as valid_from,
             safe_cast(substr(cast(validto as string), 1, 26) as timestamp) as valid_to
         from {{ source('wwi_oltp', 'Countries') }}
     ),
city_sp as (
    select c.wwi_city_id, c.city, c.state_province_id, c.location, c.latest_recorded_population, c.valid_from, c.valid_to,
           sp.state_province, sp.country_id, sp.sales_territory
    from cities c
    inner join state_provinces sp on c.state_province_id = sp.state_province_id
        and sp.valid_from <= c.valid_from and (sp.valid_to is null or sp.valid_to > c.valid_from)
),
city_sp_co as (
    select c.wwi_city_id, c.city, c.state_province, c.country_id, c.sales_territory, c.location, c.latest_recorded_population, c.valid_from, c.valid_to,
           co.country, co.continent, co.region, co.subregion
    from city_sp c
    inner join countries co on c.country_id = co.country_id
        and co.valid_from <= c.valid_from and (co.valid_to is null or co.valid_to > c.valid_from)
),
with_valid_to as (
    select wwi_city_id, city, state_province, country, continent, sales_territory, region, subregion, location, latest_recorded_population, valid_from,
           coalesce(lead(valid_from) over (partition by wwi_city_id order by valid_from), timestamp('9999-12-31 23:59:59.999999')) as valid_to
    from city_sp_co
)
select * from with_valid_to
