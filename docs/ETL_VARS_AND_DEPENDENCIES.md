# ETL Variables and Build Dependencies

## SSIS package variables → dbt equivalent

| SSIS variable | Purpose | dbt approach |
|---------------|---------|--------------|
| **LastETLCutoffTime** | Lower bound for change extract (previous load cutoff) | **Incremental**: filter in staging/intermediate with `where last_modified_when > var('etl_cutoff_start')`. Or use `dbt run --vars '{"etl_cutoff_start": "2024-01-01 00:00:00"}'` and reference in models. |
| **TargetETLCutoffTime** | Upper bound for this run; stored after success | **Incremental**: `etl_cutoff_end` var; optionally write to a small state table after run (e.g. `insert into etl_cutoff (table_name, cutoff_time) values ('Customer', '...')` in a post-hook). |
| **LineageKey** | Batch key for this load; stored in Dimension/Fact rows | **Optional**: (1) Add a `lineage_key` or `run_id` column to dim/fct and set via `var('run_id')` or `invocation_id` in dbt; (2) Or omit and use dbt `run_results` / metadata for lineage. |
| **TableName** | Entity name for GetLastETLCutoffTime / GetLineageKey | Not needed in dbt; each model is one entity. |

### Using cutoff vars in a model (example)

```sql
-- In a staging or incremental model:
where 1=1
  and (nullif('{{ var("etl_cutoff_start") }}', '') = '' or last_edited_when > timestamp('{{ var("etl_cutoff_start") }}'))
  and (nullif('{{ var("etl_cutoff_end") }}', '') = '' or last_edited_when <= timestamp('{{ var("etl_cutoff_end") }}'))
```

For **full refresh** (default): leave vars empty; all rows are processed.

---

## Build order (SSIS load order → dbt DAG)

dbt builds models in **dependency order**. The following order matches SSIS and ensures dimensions exist before facts that reference them:

| Order | Entity | Type | Depends on |
|-------|--------|------|------------|
| 1 | Date | Dimension | None |
| 2 | City | Dimension | None |
| 3 | Customer | Dimension | City (load order only) |
| 4 | Employee | Dimension | Customer (load order only) |
| 5 | Payment Method | Dimension | Employee (load order only) |
| 6 | Stock Item | Dimension | Payment Method (load order only) |
| 7 | Supplier | Dimension | Stock Item (load order only) |
| 8 | Transaction Type | Dimension | Supplier (load order only) |
| 9 | Sale, Order, Movement, Purchase, Stock Holding, Transaction | Facts | Dimensions above |

**Run all in correct order:**

```bash
dbt run
```

dbt will run dimensions before facts because facts `ref()` dimensions. The order among dimensions is determined by the DAG (e.g. `dim_customer` refs `int_customer__joined` which refs `stg_sales__customers`; no ref to `dim_city`, so City and Customer can run in any order unless you add a ref).

To **enforce** SSIS-like order explicitly (optional):

```bash
dbt run --select dim_date dim_city int_customer__joined dim_customer int_supplier__joined dim_supplier dim_employee dim_payment_method dim_stock_item dim_supplier dim_transaction_type
dbt run --select fct_sale fct_order fct_movement fct_purchase fct_stock_holding fct_transaction
```

---

## SCD Type 2 (History tracking)

Dimensions that use **SCD Type 2** in this project:

- City, Customer, Employee, Payment Method, Stock Item, Supplier, Transaction Type

Each has:

- **valid_from** / **valid_to**: row effective period; `valid_to = 9999-12-31` for current row.
- **Surrogate key** (e.g. `customer_key`): stable FK for facts.
- **Natural key** (e.g. `wwi_customer_id`): source system id; facts resolve dimension key by matching on natural key and `last_modified_when` between `valid_from` and `valid_to`.

Transaction Type is SCD Type 1 in SSIS; we still use valid_from/valid_to here for consistency.
