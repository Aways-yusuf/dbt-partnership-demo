{{ config(materialized='view') }}

with source as (
    select * from {{ source('wwi_source', 'StockItems') }}
),

renamed as (
    select
        cast(StockItemID as int64) as wwi_stock_item_id,
        StockItemName as stock_item_name,
        cast(SupplierID as int64) as supplier_id,
        safe_cast(ColorID as int64) as color_id,
        cast(UnitPackageID as int64) as unit_package_id,
        cast(OuterPackageID as int64) as outer_package_id,
        Brand as brand,
        Size as size,
        cast(LeadTimeDays as int64) as lead_time_days,
        cast(QuantityPerOuter as int64) as quantity_per_outer,
        cast(IsChillerStock as int64) as is_chiller_stock,
        Barcode as barcode,
        cast(TaxRate as float64) as tax_rate,
        cast(UnitPrice as float64) as unit_price,
        cast(RecommendedRetailPrice as float64) as recommended_retail_price,
        cast(TypicalWeightPerUnit as float64) as typical_weight_per_unit,
        MarketingComments as marketing_comments,
        InternalComments as internal_comments,
        Photo as photo,
        CustomFields as custom_fields,
        Tags as tags,
        SearchDetails as search_details,
        {{ parse_timestamp('ValidFrom') }} as valid_from,
        {{ parse_timestamp('ValidTo') }} as valid_to,
        cast(LastEditedBy as int64) as last_edited_by
    from source
    where StockItemID is not null
)

select * from renamed

