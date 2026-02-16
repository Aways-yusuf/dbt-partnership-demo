{{ config(
    materialized='incremental',
    unique_key='purchase_key',
    partition_by={'field': 'order_date_key', 'data_type': 'date'},
    cluster_by=['supplier_key', 'stock_item_key']
) }}

{% if is_incremental() %}
    {% set cutoff_date = "date_sub(current_date(), interval 7 day)" %}
{% endif %}

with purchase_prep as (
    select * from {{ ref('int_purchase_prep') }}
    {% if is_incremental() %}
    where last_modified_when >= timestamp({{ cutoff_date }})
    {% endif %}
),

dim_supplier as (
    select * from {{ ref('dim_supplier') }}
),

dim_stock_item as (
    select * from {{ ref('dim_stock_item') }}
),

purchase_with_keys as (
    select
        pp.*,
        coalesce((
            select supplier_key
            from dim_supplier ds
            where ds.wwi_supplier_id = pp.wwi_supplier_id
                and pp.last_modified_when > ds.valid_from
                and pp.last_modified_when <= ds.valid_to
            order by ds.valid_from
            limit 1
        ), {{ surrogate_key(['pp.wwi_supplier_id']) }}) as supplier_key,
        coalesce((
            select stock_item_key
            from dim_stock_item dsi
            where dsi.wwi_stock_item_id = pp.wwi_stock_item_id
                and pp.last_modified_when > dsi.valid_from
                and pp.last_modified_when <= dsi.valid_to
            order by dsi.valid_from
            limit 1
        ), {{ surrogate_key(['pp.wwi_stock_item_id']) }}) as stock_item_key
    from purchase_prep pp
),

package_types as (
    select * from {{ ref('stg_package_types') }}
),

final as (
    select
        {{ surrogate_key(['wwi_purchase_order_id', 'wwi_stock_item_id']) }} as purchase_key,
        supplier_key,
        stock_item_key,
        cast(order_date as date) as order_date_key,
        cast(wwi_purchase_order_id as int64) as wwi_purchase_order_id,
        cast(ordered_outers as int64) as ordered_outers,
        cast(received_outers as int64) as received_outers,
        description,
        pt.package_type_name as package,
        cast(expected_unit_price_per_outer as float64) as expected_unit_price_per_outer,
        cast(last_receipt_date as date) as last_receipt_date,
        cast(is_order_line_finalized as int64) as is_order_line_finalized
    from purchase_with_keys pwk
    left join package_types pt
        on pwk.package_type_id = pt.package_type_id
        and pwk.last_modified_when >= pt.valid_from
        and pwk.last_modified_when < coalesce(pt.valid_to, timestamp('9999-12-31 23:59:59.999999'))
)

{% if is_incremental() %}
select * from final
where not exists (
    select 1
    from {{ this }}
    where {{ this }}.purchase_key = final.purchase_key
)
{% else %}
select * from final
{% endif %}

