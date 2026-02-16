{{ config(
    materialized='incremental',
    unique_key='stock_item_key',
    on_schema_change='fail'
) }}

{% if is_incremental() %}
    {% set end_of_time = "timestamp('9999-12-31 23:59:59.999999')" %}
{% else %}
    {% set end_of_time = "timestamp('9999-12-31 23:59:59.999999')" %}
{% endif %}

with source_data as (
    select * from {{ ref('int_stock_item_enriched') }}
),

{% if is_incremental() %}
existing_rows as (
    select * from {{ this }}
    where is_current = true
),
{% endif %}

new_rows as (
    select
        {{ surrogate_key(['wwi_stock_item_id']) }} as stock_item_key,
        cast(wwi_stock_item_id as int64) as wwi_stock_item_id,
        stock_item,
        color,
        selling_package,
        buying_package,
        brand,
        size,
        lead_time_days,
        quantity_per_outer,
        is_chiller_stock,
        barcode,
        tax_rate,
        unit_price,
        recommended_retail_price,
        typical_weight_per_unit,
        photo,
        valid_from,
        coalesce(valid_to, {{ end_of_time }}) as valid_to,
        {{ hash_diff(['stock_item', 'color', 'selling_package', 'buying_package', 'brand', 'size', 'lead_time_days', 'quantity_per_outer', 'is_chiller_stock', 'barcode', 'tax_rate', 'unit_price', 'recommended_retail_price', 'typical_weight_per_unit']) }} as hashdiff
    from source_data
    where wwi_stock_item_id is not null
),

{% if is_incremental() %}
rows_to_close as (
    select
        er.stock_item_key,
        er.wwi_stock_item_id,
        min(nr.valid_from) as new_valid_from
    from existing_rows er
    inner join new_rows nr
        on er.wwi_stock_item_id = nr.wwi_stock_item_id
    group by er.stock_item_key, er.wwi_stock_item_id
),

closed_rows as (
    select
        er.*,
        rtc.new_valid_from as valid_to_updated,
        case
            when rtc.new_valid_from is not null then false
            else er.is_current
        end as is_current_updated
    from existing_rows er
    left join rows_to_close rtc
        on er.stock_item_key = rtc.stock_item_key
),

final as (
    select
        stock_item_key,
        wwi_stock_item_id,
        stock_item,
        color,
        selling_package,
        buying_package,
        brand,
        size,
        lead_time_days,
        quantity_per_outer,
        is_chiller_stock,
        barcode,
        tax_rate,
        unit_price,
        recommended_retail_price,
        typical_weight_per_unit,
        photo,
        valid_from,
        coalesce(valid_to_updated, valid_to) as valid_to,
        hashdiff,
        is_current_updated as is_current
    from closed_rows
    
    union all
    
    select
        stock_item_key,
        wwi_stock_item_id,
        stock_item,
        color,
        selling_package,
        buying_package,
        brand,
        size,
        lead_time_days,
        quantity_per_outer,
        is_chiller_stock,
        barcode,
        tax_rate,
        unit_price,
        recommended_retail_price,
        typical_weight_per_unit,
        photo,
        valid_from,
        valid_to,
        hashdiff,
        case
            when valid_to = {{ end_of_time }} then true
            else false
        end as is_current
    from new_rows
)
{% else %}
final as (
    select
        stock_item_key,
        wwi_stock_item_id,
        stock_item,
        color,
        selling_package,
        buying_package,
        brand,
        size,
        lead_time_days,
        quantity_per_outer,
        is_chiller_stock,
        barcode,
        tax_rate,
        unit_price,
        recommended_retail_price,
        typical_weight_per_unit,
        photo,
        valid_from,
        valid_to,
        hashdiff,
        case
            when valid_to = {{ end_of_time }} then true
            else false
        end as is_current
    from new_rows
)
{% endif %}

select * from final

