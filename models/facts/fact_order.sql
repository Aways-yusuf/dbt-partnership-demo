{{ config(
    materialized='incremental',
    unique_key='order_key',
    partition_by={'field': 'order_date_key', 'data_type': 'date'},
    cluster_by=['city_key', 'customer_key', 'stock_item_key']
) }}

{% if is_incremental() %}
    {% set cutoff_date = "date_sub(current_date(), interval 7 day)" %}
{% endif %}

with order_prep as (
    select * from {{ ref('int_order_prep') }}
    {% if is_incremental() %}
    where last_modified_when >= timestamp({{ cutoff_date }})
    {% endif %}
),

dim_city as (
    select * from {{ ref('dim_city') }}
),

dim_customer as (
    select * from {{ ref('dim_customer') }}
),

dim_stock_item as (
    select * from {{ ref('dim_stock_item') }}
),

dim_employee as (
    select * from {{ ref('dim_employee') }}
),

order_with_keys as (
    select
        op.*,
        coalesce((
            select city_key
            from dim_city dc
            where dc.wwi_city_id = op.wwi_city_id
                and op.last_modified_when > dc.valid_from
                and op.last_modified_when <= dc.valid_to
            order by dc.valid_from
            limit 1
        ), {{ surrogate_key(['op.wwi_city_id']) }}) as city_key,
        coalesce((
            select customer_key
            from dim_customer dc
            where dc.wwi_customer_id = op.wwi_customer_id
                and op.last_modified_when > dc.valid_from
                and op.last_modified_when <= dc.valid_to
            order by dc.valid_from
            limit 1
        ), {{ surrogate_key(['op.wwi_customer_id']) }}) as customer_key,
        coalesce((
            select stock_item_key
            from dim_stock_item dsi
            where dsi.wwi_stock_item_id = op.wwi_stock_item_id
                and op.last_modified_when > dsi.valid_from
                and op.last_modified_when <= dsi.valid_to
            order by dsi.valid_from
            limit 1
        ), {{ surrogate_key(['op.wwi_stock_item_id']) }}) as stock_item_key,
        coalesce((
            select employee_key
            from dim_employee de
            where de.wwi_employee_id = op.wwi_salesperson_id
                and op.last_modified_when > de.valid_from
                and op.last_modified_when <= de.valid_to
            order by de.valid_from
            limit 1
        ), {{ surrogate_key(['op.wwi_salesperson_id']) }}) as salesperson_key,
        coalesce((
            select employee_key
            from dim_employee de
            where de.wwi_employee_id = op.wwi_picker_id
                and op.last_modified_when > de.valid_from
                and op.last_modified_when <= de.valid_to
            order by de.valid_from
            limit 1
        ), {{ surrogate_key(['op.wwi_picker_id']) }}) as picker_key
    from order_prep op
),

package_types as (
    select * from {{ ref('stg_package_types') }}
),

final as (
    select
        {{ surrogate_key(['wwi_order_id', 'wwi_stock_item_id']) }} as order_key,
        city_key,
        customer_key,
        stock_item_key,
        cast(order_date as date) as order_date_key,
        cast(picking_completed_when as date) as picked_date_key,
        salesperson_key,
        picker_key,
        cast(wwi_order_id as int64) as wwi_order_id,
        safe_cast(wwi_backorder_id as int64) as wwi_backorder_id,
        description,
        pt.package_type_name as package,
        cast(quantity as int64) as quantity,
        cast(unit_price as float64) as unit_price,
        cast(tax_rate as float64) as tax_rate,
        cast(total_excluding_tax as float64) as total_excluding_tax,
        cast(tax_amount as float64) as tax_amount,
        cast(total_including_tax as float64) as total_including_tax
    from order_with_keys owk
    left join package_types pt
        on owk.package_type_id = pt.package_type_id
        and owk.last_modified_when >= pt.valid_from
        and owk.last_modified_when < coalesce(pt.valid_to, timestamp('9999-12-31 23:59:59.999999'))
)

{% if is_incremental() %}
select * from final
where not exists (
    select 1
    from {{ this }}
    where {{ this }}.order_key = final.order_key
)
{% else %}
select * from final
{% endif %}

