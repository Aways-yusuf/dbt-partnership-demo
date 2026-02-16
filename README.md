# dbt-partnership-demo – WWI on BigQuery

dbt project that replicates the **Wide World Importers** data warehouse pipeline from the legacy **SSIS DailyETLMain.dtsx** package. All 13 entities are implemented with the same logic, transformations, and structural patterns as the SSIS/DTSX packages.

## Entities (SSIS → dbt)

| Entity           | Type      | SSIS flow                         | dbt models |
|------------------|-----------|-----------------------------------|------------|
| City             | Dimension | GetCityUpdates → City_Staging → MigrateStagedCityData | stg_application__cities, state_provinces, countries → int_city__joined → dim_city |
| Customer         | Dimension | GetCustomerUpdates → Customer_Staging → MigrateStagedCustomerData | stg_sales__customers → dim_customer |
| Employee         | Dimension | GetEmployeeUpdates → Employee_Staging → MigrateStagedEmployeeData | stg_application__people → dim_employee |
| Payment Method   | Dimension | GetPaymentMethodUpdates → PaymentMethod_Staging → MigrateStagedPaymentMethodData | stg_application__payment_methods → dim_payment_method |
| Stock Item       | Dimension | GetStockItemUpdates → StockItem_Staging → MigrateStagedStockItemData | stg_warehouse__stock_items → dim_stock_item |
| Supplier         | Dimension | GetSupplierUpdates → Supplier_Staging → MigrateStagedSupplierData | stg_purchasing__suppliers → dim_supplier |
| Transaction Type | Dimension | GetTransactionTypeUpdates → TransactionType_Staging → MigrateStagedTransactionTypeData | stg_application__transaction_types → dim_transaction_type |
| Date             | Dimension | PopulateDateDimensionForYear     | dim_date |
| Movement         | Fact      | GetMovementUpdates → Movement_Staging → MigrateStagedMovementData | stg_warehouse__stock_item_transactions → fct_movement |
| Order            | Fact      | GetOrderUpdates → Order_Staging → MigrateStagedOrderData | stg_sales__order_lines → fct_order |
| Purchase         | Fact      | GetPurchaseUpdates → Purchase_Staging → MigrateStagedPurchaseData | stg_purchasing__purchase_order_lines → fct_purchase |
| Sale             | Fact      | GetSaleUpdates → Sale_Staging → MigrateStagedSaleData | stg_sales__invoice_lines → fct_sale |
| Stock Holding    | Fact      | GetStockHoldingUpdates → StockHolding_Staging → MigrateStagedStockHoldingData | stg_warehouse__stock_item_holdings → fct_stock_holding |
| Transaction      | Fact      | GetTransactionUpdates → Transaction_Staging → MigrateStagedTransactionData | stg_sales__customer_transactions + stg_purchasing__supplier_transactions → int_transaction__union → fct_transaction |

## Naming and structure

- **Staging**: `stg_<schema>__<table>` (views), aligned to Integration.*_Staging column sets.
- **Intermediate**: `int_<entity>__<suffix>` (e.g. `int_city__joined`, `int_transaction__union`).
- **Dimensions**: `dim_<entity>` (tables), SCD2 with `valid_from`/`valid_to`, surrogate `*_key`.
- **Facts**: `fct_<entity>` (tables), dimension keys resolved by effective date (`last_modified_when`).
- **Columns**: snake_case; legacy names mapped in model comments/sources.

## Setup

1. Install: `pip install -r requirements.txt`
2. Set env: `BQ_PROJECT`, `WWI_SOURCE_DATASET`, `WWI_DW_DATASET`
3. Align `models/sources.yml` with your replicated OLTP table identifiers
4. Run: `dbt debug` then `dbt run`

## Run order

Dimensions before facts (same as SSIS):

```bash
dbt run --select dim_* int_*    # dimensions + intermediates
dbt run --select fct_*          # facts
# or
dbt run
```

## Source reference

Legacy definitions: `sql-server-samples-master/samples/databases/wide-world-importers/` (wwi-ssis, wwi-dw-ssdt, wwi-ssdt).
