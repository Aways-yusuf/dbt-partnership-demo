{{ config(materialized='view') }}

with stock_items as (
    select * from {{ ref('stg_stock_items') }}
),

colors as (
    select * from {{ ref('stg_colors') }}
),

package_types_selling as (
    select * from {{ ref('stg_package_types') }}
),

package_types_buying as (
    select * from {{ ref('stg_package_types') }}
)

select
    si.wwi_stock_item_id,
    si.stock_item_name as stock_item,
    c.color_name as color,
    pt_selling.package_type_name as selling_package,
    pt_buying.package_type_name as buying_package,
    si.brand,
    si.size,
    si.lead_time_days,
    si.quantity_per_outer,
    si.is_chiller_stock,
    si.barcode,
    si.tax_rate,
    si.unit_price,
    si.recommended_retail_price,
    si.typical_weight_per_unit,
    si.photo,
    si.valid_from,
    si.valid_to,
    si.last_edited_by
from stock_items si
left join colors c
    on si.color_id = c.color_id
    and si.valid_from >= c.valid_from
    and si.valid_from < coalesce(c.valid_to, timestamp('9999-12-31 23:59:59.999999'))
left join package_types_selling pt_selling
    on si.unit_package_id = pt_selling.package_type_id
    and si.valid_from >= pt_selling.valid_from
    and si.valid_from < coalesce(pt_selling.valid_to, timestamp('9999-12-31 23:59:59.999999'))
left join package_types_buying pt_buying
    on si.outer_package_id = pt_buying.package_type_id
    and si.valid_from >= pt_buying.valid_from
    and si.valid_from < coalesce(pt_buying.valid_to, timestamp('9999-12-31 23:59:59.999999'))

