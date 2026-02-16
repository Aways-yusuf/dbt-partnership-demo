{{ config(
    materialized='incremental',
    unique_key='stock_holding_key',
    cluster_by=['stock_item_key']
) }}

{% if is_incremental() %}
    {% set cutoff_date = "date_sub(current_date(), interval 7 day)" %}
{% endif %}

with stock_item_holdings as (
    select * from {{ ref('stg_stock_item_holdings') }}
    {% if is_incremental() %}
    where last_edited_when >= timestamp({{ cutoff_date }})
    {% endif %}
),

dim_stock_item as (
    select * from {{ ref('dim_stock_item') }}
),

holding_with_keys as (
    select
        sih.*,
        coalesce((
            select stock_item_key
            from dim_stock_item dsi
            where dsi.wwi_stock_item_id = sih.stock_item_id
                and sih.last_edited_when > dsi.valid_from
                and sih.last_edited_when <= dsi.valid_to
            order by dsi.valid_from
            limit 1
        ), {{ surrogate_key(['sih.stock_item_id']) }}) as stock_item_key
    from stock_item_holdings sih
),

final as (
    select
        {{ surrogate_key(['stock_item_id']) }} as stock_holding_key,
        stock_item_key,
        cast(quantity_on_hand as int64) as quantity_on_hand,
        bin_location,
        cast(last_stocktake_quantity as int64) as last_stocktake_quantity,
        cast(last_cost_price as float64) as last_cost_price,
        cast(reorder_level as int64) as reorder_level,
        cast(target_stock_level as int64) as target_stock_level
    from holding_with_keys
)

{% if is_incremental() %}
select * from final
where not exists (
    select 1
    from {{ this }}
    where {{ this }}.stock_holding_key = final.stock_holding_key
)
{% else %}
select * from final
{% endif %}

