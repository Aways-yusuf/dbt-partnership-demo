# dbt-partnership-demo – WWI on BigQuery and AlloyDB

dbt project that replicates the **Wide World Importers** data warehouse pipeline from the legacy **SSIS DailyETLMain.dtsx** package. All 13 entities are implemented with the same logic, transformations, and structural patterns as the SSIS/DTSX packages.

**Warehouses:** The project runs on **BigQuery** (default) and optionally on **AlloyDB**. Use `--target dev` for BigQuery or `--target alloydb_dev` for AlloyDB. See [AlloyDB setup](#alloydb-optional) below.

## Entities (SSIS → dbt)

| Entity           | Type      | SSIS flow                         | dbt models |
|------------------|-----------|-----------------------------------|------------|
| City             | Dimension | GetCityUpdates → City_Staging → MigrateStagedCityData | stg_application__cities, state_provinces, countries → int_city__joined → dim_city |
| Customer         | Dimension | GetCustomerUpdates → Customer_Staging → MigrateStagedCustomerData (SCD2; dep: City) | stg_sales__customers + categories, buying_groups, people → int_customer__joined → dim_customer |
| Employee         | Dimension | GetEmployeeUpdates → Employee_Staging → MigrateStagedEmployeeData (SCD2; dep: Customer) | stg_application__people → dim_employee |
| Payment Method   | Dimension | GetPaymentMethodUpdates → PaymentMethod_Staging → MigrateStagedPaymentMethodData (SCD2; dep: Employee) | stg_application__payment_methods → dim_payment_method |
| Stock Item       | Dimension | GetStockItemUpdates → StockItem_Staging → MigrateStagedStockItemData (SCD2; dep: Payment Method) | stg_warehouse__stock_items → dim_stock_item |
| Supplier         | Dimension | GetSupplierUpdates → Supplier_Staging → MigrateStagedSupplierData (SCD2; dep: Stock Item) | stg_purchasing__suppliers + supplier_categories, people → int_supplier__joined → dim_supplier |
| Transaction Type | Dimension | GetTransactionTypeUpdates → TransactionType_Staging → MigrateStagedTransactionTypeData | stg_application__transaction_types → dim_transaction_type |
| Date             | Dimension | PopulateDateDimensionForYear     | dim_date |
| Movement         | Fact      | GetMovementUpdates → Movement_Staging → MigrateStagedMovementData | stg_warehouse__stock_item_transactions → fct_movement |
| Order            | Fact      | GetOrderUpdates → Order_Staging → MigrateStagedOrderData | stg_sales__order_lines → fct_order |
| Purchase         | Fact      | GetPurchaseUpdates → Purchase_Staging → MigrateStagedPurchaseData (Key mapping; dep: Supplier, Stock Item) | stg_purchasing__purchase_order_lines → fct_purchase |
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

### BigQuery (default)

1. Install: `pip install -r requirements.txt` (includes `dbt-bigquery`; add `dbt-postgres` if you also want AlloyDB).
2. Set env: `BQ_PROJECT`, `WWI_SOURCE_DATASET`, `WWI_DW_DATASET`
3. Align `models/sources.yml` with your replicated OLTP table identifiers (database/schema for your BQ project/dataset).
4. Run: `dbt debug` then `dbt run` (uses target `dev` = BigQuery).

### AlloyDB (optional)

You can run the **same project** against AlloyDB so that all dbt models are built in AlloyDB (e.g. for a copy of the warehouse or for migration).

1. Install the Postgres adapter: `pip install dbt-postgres` (already listed in `requirements.txt`).
2. In `profiles.yml`, the targets `alloydb_dev` and `alloydb_prod` are configured. Set environment variables (or override in the profile):
   - `ALLOYDB_HOST` – AlloyDB instance host (e.g. from AlloyDB connection name).
   - `ALLOYDB_PORT` – usually `5432`.
   - `ALLOYDB_USER` – database user.
   - `ALLOYDB_PASSWORD` – password (or use IAM if supported).
   - `ALLOYDB_DATABASE` – database name (e.g. `wide_world_importers`).
3. Ensure the same OLTP source data is available in AlloyDB (replicate from your source or from BigQuery). Point the **sources** in `models/sources.yml` to the AlloyDB database/schema when running with `--target alloydb_*` (sources use `database` and `schema`; for Postgres/AlloyDB these map to the DB and schema containing your tables).
4. Run against AlloyDB:
   - `dbt debug --target alloydb_dev`
   - `dbt run --target alloydb_dev`
   - `dbt build --target alloydb_prod` for production.

#### AlloyDB in dbt Cloud

In dbt Cloud, the profile and targets come from the **Connection** and **Environment** you configure in the UI, not from the repo’s `profiles.yml`. So `--target alloydb_dev` fails with “profile 'user' does not have a target named 'alloydb_dev'” because that target only exists in the repo profile.

**Fix: use a separate AlloyDB connection and environment**

1. **Add a PostgreSQL (AlloyDB) connection**
   - In dbt Cloud: **Project** → **Settings** (gear) → **Connections** (or **Profile**).
   - Click **Add connection** (or **New connection**).
   - Choose **PostgreSQL** (AlloyDB is Postgres-compatible).
   - Set:
     - **Host:** `35.224.103.243`
     - **Port:** `5432`
     - **Database:** your database name (e.g. `postgres`)
     - **User / Password:** your AlloyDB user and password (or use IAM and token as password if your setup supports it).
   - Name the connection (e.g. “AlloyDB”) and save.

2. **Create an environment that uses that connection**
   - **Deploy** → **Environments** → **Create environment**.
   - **Name:** e.g. “AlloyDB Dev”.
   - **Connection:** select the AlloyDB connection you created.
   - **dbt version:** choose a version (e.g. same as your BigQuery env).
   - Save.

3. **Run dbt against AlloyDB**
   - **Create a job** (or edit an existing job): **Deploy** → **Jobs** → **Create job** (or pick a job).
   - Set **Environment** to the AlloyDB environment (e.g. “AlloyDB Dev”).
   - **Commands:** use `dbt run` (no `--target` needed; the environment’s connection is the target).
   - Run the job.

4. **Where AlloyDB source tables live**  
   Sources point to BigQuery when the target is BigQuery, and to Postgres/AlloyDB when the target is Postgres (see `models/sources.yml`). By default the project uses **database** `postgres` and **schema** `public` for AlloyDB. If your WWI source tables are in a different database or schema, set **Environment variables** or **Variables** for the AlloyDB environment in dbt Cloud:
   - `source_database`: e.g. `postgres` (or your DB name).
   - `source_schema`: e.g. `public` (or the schema where `People`, `Cities`, etc. live).

So in dbt Cloud you don’t use `--target alloydb_dev`. You run `dbt run` in a job (or IDE) that uses the AlloyDB environment; that environment’s connection is the target.

**If you see:** `panic: not yet implemented: PostgreSQL's list_relations_schemas` — the Rust adapter in dbt 1.8+ doesn't implement this for Postgres. **Fix:** pin both `dbt-core` and `dbt-postgres` to &lt; 1.8 (see `requirements.txt`), then reinstall so nothing is 1.8+:

  ```bash
  pip uninstall dbt-core dbt-postgres dbt-bigquery -y
  pip install -r requirements.txt
  ```

  Confirm versions (all should be 1.7.x or 1.6.x, not 1.8+):

  ```bash
  pip show dbt-core dbt-postgres
  ```

  Then run again: `dbt run --target alloydb_dev_iam`.

#### AlloyDB with IAM (OAuth)

To use IAM instead of a static password (token as password), do the following.

**1. Enable IAM authentication on the AlloyDB instance**

- In Google Cloud Console: AlloyDB → your instance → Edit → Database flags → add `alloydb.iam_authentication` = `on` (or set it via [Configure database flags](https://cloud.google.com/alloydb/docs/instance-configure-database-flags)).

**2. Grant IAM roles to the user or service account**

The principal (your user or service account) needs:

- `alloydb.databaseUser` – connect to the instance
- `serviceusage.serviceUsageConsumer` – permission-checking APIs

Console: IAM & Admin → select principal → Grant access → add role **AlloyDB Database User** (and ensure Service Usage Consumer is present).  
Or with gcloud: [Grant access to other users](https://cloud.google.com/alloydb/docs/user-grant-access).

**3. Create the IAM database user on the cluster**

- **Console:** AlloyDB → Clusters → your cluster → **Users** → **Add user account** → **Cloud IAM** → enter principal (see username format below) → Add.
- **gcloud:** Replace `USERNAME`, `CLUSTER`, `REGION` (e.g. `us-central1`):

  ```bash
  gcloud alloydb users create USERNAME \
    --cluster=CLUSTER \
    --region=REGION \
    --type=IAM_BASED
  ```

  **USERNAME format:**  
  - IAM user: full email (e.g. `you@company.com`).  
  - Service account: email **without** `.gserviceaccount.com` (e.g. `my-sa@my-project.iam`).

**4. Grant DB privileges (optional)**

Connect as `postgres` and grant access to objects the IAM user needs (e.g. for dbt: schema/database access). Example:

```sql
GRANT USAGE ON SCHEMA dbt_target TO "you@company.com";
GRANT CREATE ON SCHEMA dbt_target TO "you@company.com";
-- grant on source schema/tables as needed
```

**5. Set the username in `profiles.yml`**

Under `alloydb_dev_iam` (and `alloydb_prod_iam` if used), set `user` to the same value you used as USERNAME in step 3 (e.g. `you@company.com` or `my-sa@my-project.iam`).

**6. Get a token and run dbt**

In the same terminal where you run dbt:

```bash
# Use your current gcloud user/application-default credentials
export PGPASSWORD=$(gcloud auth print-access-token)

# Optional: restrict token to AlloyDB only
# export PGPASSWORD=$(gcloud auth application-default print-access-token --scopes=https://www.googleapis.com/auth/alloydb.login)

dbt debug --target alloydb_dev_iam
dbt run --target alloydb_dev_iam
```

Tokens expire; if a run fails with auth errors, run `export PGPASSWORD=$(gcloud auth print-access-token)` again and retry.

---

**Summary:** BigQuery remains the default (`dev` / `prod`). Use `--target alloydb_dev` or `--target alloydb_prod` to build the same models in AlloyDB. Use `--target alloydb_dev_iam` (and step 6 above) for IAM/OAuth. The project uses cross-database macros in `macros/cross_db_utils.sql` so one codebase compiles to both BigQuery and Postgres/AlloyDB SQL.

## Run order

Dimensions before facts (same as SSIS):

```bash
dbt run --select dim_* int_*    # dimensions + intermediates
dbt run --select fct_*          # facts
# or
dbt run
```

## ETL variables and build order

SSIS package variables (LastETLCutoffTime, TargetETLCutoffTime, LineageKey, TableName) and how to implement them in dbt (vars, incremental, run_id), plus build-order dependencies: see **[docs/ETL_VARS_AND_DEPENDENCIES.md](docs/ETL_VARS_AND_DEPENDENCIES.md)**.

## Documentation

- **Model and column descriptions** are in the schema YAML under `models/`:
  - `models/facts/_facts_models.yml` — fact tables (grain, FKs, key columns)
  - `models/dimensions/_dimensions_models.yml` — dimension tables
  - `models/staging/_staging_models.yml` — staging views and source mapping
  - `models/intermediate/_intermediate_models.yml` — intermediate models
- **Source definitions** (tables, descriptions): `models/sources.yml`
- **Reference docs** (dbt-fusion): `dbt man` (writes catalog/artifacts with `--write-json` / `--write-catalog`)

## Source reference

Legacy definitions: `sql-server-samples-master/samples/databases/wide-world-importers/` (wwi-ssis, wwi-dw-ssdt, wwi-ssdt).
