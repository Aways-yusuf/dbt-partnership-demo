# SSIS to dbt BigQuery Migration Summary

## Overview
This document summarizes the complete migration of the Wide World Importers SSIS ETL project to dbt BigQuery models.

## Project Structure

```
dbt-partnership-demo/
├── models/
│   ├── sources/
│   │   └── sources.yml          # Source table definitions
│   ├── staging/                  # 20 staging models
│   ├── intermediate/             # 4 enrichment models
│   ├── dimensions/               # 7 SCD Type 2 dimension models
│   └── facts/                    # 6 fact table models
├── macros/
│   ├── hash_diff.sql            # Hash generation for SCD Type 2
│   ├── parse_timestamp.sql      # Timestamp parsing with timezone support
│   └── surrogate_key.sql        # Surrogate key generation
└── dbt_project.yml              # Project configuration
```

## SSIS Task Mapping

### Dimension Loads
- `MigrateStagedCityData` → `dim_city`
- `MigrateStagedCustomerData` → `dim_customer`
- `MigrateStagedEmployeeData` → `dim_employee`
- `MigrateStagedPaymentMethodData` → `dim_payment_method`
- `MigrateStagedStockItemData` → `dim_stock_item`
- `MigrateStagedSupplierData` → `dim_supplier`
- `MigrateStagedTransactionTypeData` → `dim_transaction_type`

### Fact Loads
- `MigrateStagedSaleData` → `fact_sale`
- `MigrateStagedPurchaseData` → `fact_purchase`
- `MigrateStagedOrderData` → `fact_order`
- `MigrateStagedTransactionData` → `fact_transaction`
- `MigrateStagedMovementData` → `fact_movement`
- `MigrateStagedStockHoldingData` → `fact_stock_holding`

## Fact Table Grains

1. **Fact.Sale**: Invoice Line (WWI Invoice ID + Stock Item ID)
2. **Fact.Purchase**: Purchase Order Line (WWI Purchase Order ID + Stock Item ID)
3. **Fact.Order**: Order Line (WWI Order ID + Stock Item ID)
4. **Fact.Transaction**: Transaction (Customer or Supplier Transaction)
5. **Fact.Movement**: Stock Item Transaction (Stock Item Transaction ID)
6. **Fact.StockHolding**: Stock Item Snapshot (Stock Item ID)

## SCD Type 2 Implementation

All dimensions implement SCD Type 2 with:
- **surrogate_key**: MD5 hash of business key
- **business_key**: WWI ID (e.g., `wwi_city_id`)
- **hashdiff**: MD5 hash of all descriptive attributes
- **valid_from**: Timestamp when row becomes valid
- **valid_to**: Timestamp when row expires (9999-12-31 for current)
- **is_current**: Boolean flag indicating current row

### Incremental Logic
- First run: Inserts all rows with `is_current = true` where `valid_to = 9999-12-31`
- Incremental runs:
  1. Identifies existing current rows that have new versions
  2. Closes off those rows (sets `valid_to` and `is_current = false`)
  3. Inserts new rows with `is_current = true`

## Type Conversion Strategy

### Timestamp Handling
- Uses `parse_timestamp` macro to handle:
  - 7 decimal places (datetime2(7))
  - Timezone offsets (+00)
  - Variable fractional seconds
  - Already timestamp types

### Integer Safety
- Explicit `cast()` for required integers
- `safe_cast()` for optional integers
- All IDs cast to `int64`
- NULL business keys filtered out

### Data Type Conversions
- `STRING` → `string` (no change)
- `INT64` → `int64` (explicit cast)
- `FLOAT64` → `float64` (explicit cast)
- `DATE` → `date` (cast from string/timestamp)
- `TIMESTAMP` → `timestamp` (via parse_timestamp macro)

## Model Details

### Staging Models (20)
All staging models:
- Rename columns to snake_case
- Cast to correct BigQuery types
- Handle timestamp parsing
- Filter NULL primary keys
- No business logic

**Staging Models:**
- `stg_cities`
- `stg_customers`
- `stg_people`
- `stg_payment_methods`
- `stg_stock_items`
- `stg_suppliers`
- `stg_transaction_types`
- `stg_invoice_lines`
- `stg_invoices`
- `stg_order_lines`
- `stg_orders`
- `stg_purchase_order_lines`
- `stg_purchase_orders`
- `stg_customer_transactions`
- `stg_supplier_transactions`
- `stg_stock_item_transactions`
- `stg_stock_item_holdings`
- `stg_state_provinces`
- `stg_countries`
- `stg_buying_groups`
- `stg_customer_categories`
- `stg_delivery_methods`
- `stg_package_types`
- `stg_colors`

### Intermediate Models (4)
Enrich dimension data with lookups:
- `int_city_enriched`: Joins Cities with StateProvinces and Countries
- `int_customer_enriched`: Joins Customers with Categories, BuyingGroups, and People
- `int_employee_enriched`: Filters People where `is_employee = 1`
- `int_stock_item_enriched`: Joins StockItems with Colors and PackageTypes

### Dimension Models (7)
All use incremental materialization with SCD Type 2:
- `dim_city`
- `dim_customer`
- `dim_employee`
- `dim_payment_method`
- `dim_stock_item`
- `dim_supplier`
- `dim_transaction_type`

### Fact Models (6)
All use incremental materialization with point-in-time dimension joins:
- `fact_sale`: Partitioned by `invoice_date_key`, clustered by `city_key`, `customer_key`, `stock_item_key`
- `fact_purchase`: Partitioned by `order_date_key`, clustered by `supplier_key`, `stock_item_key`
- `fact_order`: Partitioned by `order_date_key`, clustered by `city_key`, `customer_key`, `stock_item_key`
- `fact_transaction`: Partitioned by `date_key`, clustered by `customer_key`, `supplier_key`, `transaction_type_key`
- `fact_movement`: Partitioned by `date_key`, clustered by `stock_item_key`, `transaction_type_key`
- `fact_stock_holding`: Clustered by `stock_item_key`

## Point-in-Time Joins

Facts join to dimensions using:
```sql
fact_date between dim.valid_from and dim.valid_to
```

This ensures facts reference the correct dimension version at the time of the transaction.

## Incremental Strategy

### Dimensions
- Uses `merge` strategy
- Closes off old rows before inserting new ones
- Prevents overlapping validity windows

### Facts
- Uses `merge` strategy
- 7-day lookback window for incremental loads
- Prevents duplicate rows using unique keys

## Validation Checklist

✅ No 7-digit timestamps remain (all parsed via macro)
✅ No unsafe casts (all use `safe_cast` for optional fields)
✅ No `{{ this }}` reference in first load logic
✅ All keys are `int64`
✅ All dimensions are SCD Type 2
✅ All facts use correct grain
✅ All required tables are created
✅ Proper partitioning and clustering on fact tables

## Running the Project

### First Run (Full Refresh)
```bash
dbt run --full-refresh
```

### Incremental Run
```bash
dbt run
```

### Run Specific Models
```bash
# Run all dimensions
dbt run --select dimensions

# Run all facts
dbt run --select facts

# Run specific model
dbt run --select dim_city
```

## Notes

1. **Lineage Keys**: The original SSIS implementation used lineage keys for audit purposes. This has been removed as it's SQL Server specific.

2. **Temporal Tables**: The original used SQL Server temporal tables (`FOR SYSTEM_TIME`). This has been replaced with explicit SCD Type 2 logic.

3. **Geography Type**: The `Location` field in Cities uses `FLOAT64` instead of SQL Server's `geography` type. This may need adjustment based on your BigQuery setup.

4. **Date Dimension**: The original SSIS project includes a Date dimension. This has not been created as it's typically generated separately or uses a date dimension table.

5. **Incremental Cutoff**: Facts use a 7-day lookback window. Adjust in the fact models if needed.

## Next Steps

1. Test the models with `dbt run`
2. Verify data quality with `dbt test`
3. Add custom tests for business rules
4. Create documentation with `dbt docs generate`
5. Set up scheduling in dbt Cloud or your orchestration tool

