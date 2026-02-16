{{ config(
    materialized='incremental',
    unique_key='supplier_key',
    on_schema_change='fail'
) }}

{% if is_incremental() %}
    {% set end_of_time = "timestamp('9999-12-31 23:59:59.999999')" %}
{% else %}
    {% set end_of_time = "timestamp('9999-12-31 23:59:59.999999')" %}
{% endif %}

with source_data as (
    select * from {{ ref('stg_suppliers') }}
),

{% if is_incremental() %}
existing_rows as (
    select * from {{ this }}
    where is_current = true
),
{% endif %}

new_rows as (
    select
        {{ surrogate_key(['wwi_supplier_id']) }} as supplier_key,
        cast(wwi_supplier_id as int64) as wwi_supplier_id,
        supplier_name as supplier,
        valid_from,
        coalesce(valid_to, {{ end_of_time }}) as valid_to,
        {{ hash_diff(['supplier_name']) }} as hashdiff
    from source_data
    where wwi_supplier_id is not null
),

{% if is_incremental() %}
rows_to_close as (
    select
        er.supplier_key,
        er.wwi_supplier_id,
        min(nr.valid_from) as new_valid_from
    from existing_rows er
    inner join new_rows nr
        on er.wwi_supplier_id = nr.wwi_supplier_id
    group by er.supplier_key, er.wwi_supplier_id
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
        on er.supplier_key = rtc.supplier_key
),

final as (
    select
        supplier_key,
        wwi_supplier_id,
        supplier,
        valid_from,
        coalesce(valid_to_updated, valid_to) as valid_to,
        hashdiff,
        is_current_updated as is_current
    from closed_rows
    
    union all
    
    select
        supplier_key,
        wwi_supplier_id,
        supplier,
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
        supplier_key,
        wwi_supplier_id,
        supplier,
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

