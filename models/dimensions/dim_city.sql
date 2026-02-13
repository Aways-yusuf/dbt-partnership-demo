-- Dimension City (SCD2). Replaces MigrateStagedCityData → Dimension.City.
{{ config(materialized='table', schema='dimensions') }}
with city_joined as (select * from {{ ref('int_city__joined') }})
select
    row_number() over (order by wwi_city_id, valid_from) as city_key,
    wwi_city_id,
    city,
    state_province,
    country,
    continent,
    sales_territory,
    region,
    subregion,
    location,
    coalesce(latest_recorded_population, 0) as latest_recorded_population,
    valid_from,
    valid_to
from city_joined