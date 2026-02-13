-- Date dimension. Replaces Integration.PopulateDateDimensionForYear + GenerateDateDimensionColumns.
{{ config(materialized='table', schema='dimensions') }}
with date_range as (
    select date_value from unnest(generate_date_array(date({{ var('date_dim_start_year') }}, 1, 1), date({{ var('date_dim_end_year') }}, 12, 31), interval 1 day)) as date_value
)
select
    d.date_value as date,
    extract(year from d.date_value) * 10000 + extract(month from d.date_value) * 100 + extract(day from d.date_value) as date_key,
    extract(day from d.date_value) as day_number,
    format_date('%A', d.date_value) as day_of_week,
    extract(dayofyear from d.date_value) as day_of_year_number,
    extract(week from d.date_value) as week_of_year,
    format_date('%B', d.date_value) as month_name,
    format_date('%b', d.date_value) as short_month,
    concat('Q', cast(extract(quarter from d.date_value) as string)) as quarter,
    concat('H', cast(if(extract(month from d.date_value) < 7, 1, 2) as string)) as half_of_year,
    date_trunc(d.date_value, month) as beginning_of_month,
    date_trunc(d.date_value, quarter) as beginning_of_quarter,
    extract(year from d.date_value) as calendar_year,
    concat('CY', cast(extract(year from d.date_value) as string)) as calendar_year_label,
    extract(quarter from d.date_value) as calendar_quarter_number,
    extract(month from d.date_value) as calendar_month_number,
    extract(isoweek from d.date_value) as iso_week_number,
    if(extract(month from d.date_value) > 6, extract(year from d.date_value) + 1, extract(year from d.date_value)) as fiscal_year,
    concat('FY', cast(if(extract(month from d.date_value) > 6, extract(year from d.date_value) + 1, extract(year from d.date_value)) as string)) as fiscal_year_label
from date_range d
order by date