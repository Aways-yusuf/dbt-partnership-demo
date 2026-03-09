# ssis-dbt

Convert **SSIS packages** into a **dbt project** using **Vertex AI (Gemini)**. The service reads an SSIS package (and related SQL artifacts) from Google Cloud Storage, sends them to Gemini with strict conversion rules, and uploads the generated dbt models, schemas, and macros back to GCS.

## Overview

This script implements an HTTP-triggered pipeline that:

1. **Reads from GCS**: The SSIS package at the given `file_path`, plus fixed reference files (`WWI-store-procedures.sql`, `WWI_BQ_ddl.sql`) from the same bucket.
2. **Converts via Vertex AI**: Builds a long, deterministic prompt (BigQuery or AlloyDB) and calls Gemini to produce a single JSON object: keys = file paths, values = file contents.
3. **Writes back to GCS**: Parses the model output (cleaning markdown/code fences and escaping control characters), then uploads each file under a configurable parent directory in the same bucket.

It is intended to run as a **Google Cloud Function** or **Cloud Run** service using `functions_framework`.

## Setup

### Dependencies

```bash
pip install -r requirements.txt
```

- `functions-framework` – HTTP server for Cloud Run / Cloud Functions
- `requests` – HTTP client (if needed elsewhere)
- `google-cloud-storage` – read/write GCS
- `google-cloud-aiplatform` – Vertex AI (Gemini)
- `google-api-core` – core Google API client

### Configuration (in code)

These are currently hardcoded in `cloud_run.py`; you may want to move them to environment variables for deployment:

| Variable        | Default / value           | Description                          |
|----------------|---------------------------|--------------------------------------|
| `PROJECT_ID`   | `data-platforms-66d-demos` | Google Cloud project for Vertex AI   |
| `LOCATION`     | `us-central1`            | Vertex AI region                     |
| `MODEL_NAME`   | `gemini-2.5-pro`         | Gemini model name                    |
| Bucket name    | `wwi-ssis-dbt-demo`      | GCS bucket (read + write)            |
| Parent dir     | `DBT/dbt-partnership-demo` | GCS prefix for uploaded dbt files   |

### GCS layout (expected)

- **Input**: SSIS package at `gs://<bucket>/<file_path>` (provided in the request). The script also always reads:
  - `gs://<bucket>/WWI-store-procedures.sql`
  - `gs://<bucket>/WWI_BQ_ddl.sql`
- **Output**: Generated files are written to `gs://<bucket>/<parent_dir>/<file_path>` for each entry in the AI response (e.g. `models/staging/stg_*.sql`, `models/sources.yml`, `macros/generate_schema_name.sql`, etc.).

### Permissions

The service account used to run the function needs:

- **Storage**: read objects from the bucket (SSIS + SQL files), write objects (uploaded dbt files).
- **Vertex AI**: use the Gemini model in the given project and region.

## API

### POST – convert SSIS to dbt

**Request body (JSON)**

| Field        | Required | Description |
|-------------|----------|-------------|
| `file_path` | Yes      | GCS path (relative to bucket) of the SSIS package file to convert, e.g. `path/to/package.dtsx`. |
| `warehouse` | No       | Target warehouse: `"bigquery"` (default) or `"alloydb"`. Drives BigQuery vs AlloyDB/PostgreSQL SQL and patterns in the prompt. |

**Response**

- **Success (200)**: The response body is the **cleaned JSON** returned by the model (file paths → file contents). The same content is also uploaded to GCS under the parent directory.
- **Error (400)**: Missing `file_path` or invalid `warehouse`; body is a short error message.

**Example**

```bash
curl -X POST https://YOUR_SERVICE_URL \
  -H "Content-Type: application/json" \
  -d '{"file_path": "path/to/your/package.dtsx", "warehouse": "bigquery"}'

curl -X POST https://YOUR_SERVICE_URL \
  -H "Content-Type: application/json" \
  -d '{"file_path": "path/to/your/package.dtsx", "warehouse": "alloydb"}'
```

## Conversion rules (prompt design)

The Gemini prompt enforces:

- **Deterministic output**: Same inputs → same structure, filenames, ordering, formatting. No optional/extra models, no timestamps or random suffixes.
- **One entity** → one staging model, one intermediate model, one final model (dimension or fact). File names use source table name in lowercase snake_case.
- **Strict JSON**: Single JSON object only; keys = full file paths, values = full file content. No markdown or explanations.
- **Required structure**: `models/staging/`, `models/intermediate/`, `models/dimensions/`, `models/facts/`, `models/sources.yml`, `macros/generate_schema_name.sql`, plus schema YAML for dimensions/facts.
- **Macro**: A fixed `generate_schema_name` macro is required (uses `+schema` from `dbt_project`, fallback to `target.schema`).
- **Architecture**:
  - **BigQuery**: Staging (snake_case, SAFE_CAST, SAFE.PARSE_TIMESTAMP), intermediate (temporal joins), dimensions (SCD Type 2, incremental), facts (surrogate keys, partition/cluster, 7-day incremental). BigQuery SQL only.
  - **AlloyDB**: Same layering and logic, but PostgreSQL/AlloyDB syntax (CAST, TO_TIMESTAMP, BIGINT/DOUBLE PRECISION, no BigQuery-specific functions).

The model is instructed to generate models for **all** entities detected in the SSIS package (no skipping).

## Implementation details

- **Cleaning AI output**: `clean_ai_response()` strips leading/trailing ` ```json ` and ` ``` ` so the rest can be parsed as JSON.
- **Control characters**: `_escape_control_chars_in_json_strings()` escapes raw newlines/tabs inside JSON string values to avoid `Invalid control character` when parsing.
- **Upload**: Each key in the parsed JSON is prefixed with `parent_dir` and uploaded as a blob with `content_type="text/plain"`.

## Files

- **`cloud_run.py`** – HTTP handler `main()`, GCS read/write, prompt construction for BigQuery and AlloyDB, `convert_ssis_to_dbt()`, and response cleaning/upload logic.
- **`requirements.txt`** – Python dependencies listed above.

## Running locally

```bash
# Set GOOGLE_APPLICATION_CREDENTIALS if not using default credentials
functions_framework --target=main --port=8080
```

Then send POST requests to `http://localhost:8080` with a JSON body containing `file_path` and optionally `warehouse`.
