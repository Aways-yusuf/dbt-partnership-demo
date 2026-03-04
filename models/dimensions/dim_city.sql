-- Dimension City (SCD2). Replaces MigrateStagedCityData → Dimension.City.
{{ config(
    materialized='incremental',
    unique_key=['wwi_city_id', 'valid_from'],
    incremental_strategy='merge'
) }}
with city_joined as (
    select * from {{ ref('int_city') }}
    {% if is_incremental() %}
    where valid_from > (select coalesce(max(valid_from), {{ cross_db_timestamp_min() }}) from {{ this }})
    {% endif %}
),
with_key as (
    select
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
        valid_to,
        row_number() over (order by wwi_city_id, valid_from) as rn
    from city_joined
)
select
    {% if is_incremental() %}
    (select coalesce(max(city_key), 0) from {{ this }}) + rn as city_key,
    {% else %}
    rn as city_key,
    {% endif %}
    wwi_city_id,
    city,
    state_province,
    country,
    continent,
    sales_territory,
    region,
    subregion,
    location,
    latest_recorded_population,
    valid_from,
    valid_to
from with_key