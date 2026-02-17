# Data flow and BigQuery datasets

## Dataset layout (project: data-platforms-66d-demos)

| Dataset        | Purpose                         | Created by   | Contents                                      |
|----------------|----------------------------------|-------------|-----------------------------------------------|
| **dbt_source** | Source (raw) tables              | You / load  | Customers, People, BuyingGroups, etc.         |
| **dbt_staging**| Staging + intermediate views     | dbt         | stg_* views, int_* views                      |
| **dbt_target**| Final dimensions and facts      | dbt         | dim_* tables, fct_* tables                   |

No extra datasets like `dbt_target_dbt_staging` or `dbt_target_intermediate` are created. The project uses a custom `generate_schema_name` macro so the configured schema name is the actual BigQuery dataset name.

## Flow

```
dbt_source (raw tables)
       │
       ▼
  dbt_staging
  ├── stg_*           (staging views: one per source table / entity)
  └── int_*           (intermediate views: joins, SCD2 prep)
       │
       ▼
  dbt_target
  ├── dim_*           (dimension tables, SCD2)
  └── fct_*           (fact tables)
```

## Run order (examples)

- Customer: staging → intermediate → dimension
  ```bash
  export WWI_SOURCE_DATASET=dbt_source
  dbt run --select +dim_customer
  ```
- Supplier: staging → intermediate → dimension
  ```bash
  export WWI_SOURCE_DATASET=dbt_source
  dbt run --select +dim_supplier
  ```
- Only staging and intermediate:
  ```bash
  dbt run --select staging.* intermediate.*
  ```
- Only dimensions and facts:
  ```bash
  dbt run --select dimensions.* facts.*
  ```
- Full pipeline:
  ```bash
  dbt run
  ```

## Cleaning up old datasets

If you previously ran without the macro, you may have:

- `dbt_target_dbt_staging`
- `dbt_target_intermediate`
- `dbt_target_dimensions`

You can drop those in BigQuery Console (or via `bq rm -r -d ...`) and re-run dbt; new objects will be created in **dbt_staging** and **dbt_target** only.
