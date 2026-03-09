# dbt-run

Trigger a **dbt Cloud job** (e.g. dbt run) from an HTTP API. Designed to be called from external clients such as Figma, curl, or any service that can send POST/GET requests.

## Overview

This script exposes a small HTTP API that:

- **POST**: Triggers a dbt Cloud job run (BigQuery or AlloyDB). Optionally waits for the run to finish and returns a summary of results (models run, success/error counts, elapsed time).
- **GET** `?run_id=XXXXX`: Fetches the status and run result for an existing run (useful when you triggered with `"wait": false`).
- **OPTIONS**: Handles CORS preflight for browser clients.

It uses the [dbt Cloud API v2](https://docs.getdbt.com/dbt-cloud/api-v2) and can run as a **Google Cloud Run** or **Cloud Functions** service (via `functions_framework`) or locally for testing.

## Setup

### Dependencies

```bash
pip install -r requirements.txt
```

- `functions-framework` – for Cloud Run / Cloud Functions (optional when running locally)
- `requests` – for calling the dbt Cloud API

### Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DBT_CLOUD_ACCOUNT_ID` | Yes | Account ID from the deploy URL (e.g. `https://yh400.us1.dbt.com/deploy/**70471823514028**/projects/...`). |
| `DBT_CLOUD_JOB_ID` | Yes | **Job** ID for the BigQuery job (from **Jobs** → job URL: `.../jobs/XXXXX`). **Not** the run ID from `.../runs/YYYYY`. |
| `DBT_CLOUD_JOB_ID_2` | Yes (for AlloyDB) | Job ID for the AlloyDB job (e.g. `70471823569441`). |
| `DBT_CLOUD_API_TOKEN` | Yes | dbt Cloud API token (Account → API Access). |
| `DBT_CLOUD_PROJECT_ID` | No | Project ID from the URL; used so the response can include a `run_url` to open the run in the UI. |
| `DBT_CLOUD_BASE` | No | Base URL for dbt Cloud (default: `https://yh400.us1.dbt.com`). |
| `DBT_CLOUD_AUTH_TYPE` | No | If set and contains `"Token"`, uses `Authorization: Token <token>`; otherwise `Authorization: Bearer <token>`. |

**Finding IDs**

- **Account ID**: In a run URL, the number after `/deploy/` (e.g. `70471823514028`).
- **Job ID**: In dbt Cloud go to **Jobs** → open the job → URL has `.../jobs/XXXXX`; use that `XXXXX`. Do **not** use the run ID from `.../runs/YYYYY`.

### Recommended dbt Cloud setup (two environments, two jobs)

A typical setup for this project is:

1. **Two environments** (each with its own connection type):
   - **BigQuery environment** — connection uses the **service account** method (authenticate with a Google Cloud service account in dbt Cloud). Jobs that use this environment run against BigQuery.
   - **AlloyDB environment** — connection uses **username and password** for a database user, plus **host**, **port**, **database**, and other connection details. Jobs that use this environment run against AlloyDB.

2. **Two predefined jobs**
   - **BigQuery job** — set **Environment** to the BigQuery environment. Put this job’s ID in `DBT_CLOUD_JOB_ID`. When the API receives `{"job": "bigquery"}`, it triggers this job.
   - **AlloyDB job** — set **Environment** to the AlloyDB environment. Put this job’s ID in `DBT_CLOUD_JOB_ID_2`. When the API receives `{"job": "alloydb"}`, it triggers this job.

Each job runs `dbt run` (no `--target` needed); the environment’s connection determines the warehouse. The script’s `job` parameter simply selects which of the two jobs to trigger.

## API

### POST – trigger a run

**Request**

- **Body** (JSON, all optional):
  - `job`: `"bigquery"` (default) or `"alloydb"` – which dbt Cloud job to run.
  - `cause`: string (e.g. `"Triggered from Figma"`) – reason for the run.
  - `wait`: boolean (default `true`) – if `true`, wait for the run to finish and include `run_result` in the response.
  - `wait_timeout_seconds`: number (default `600`, max `3600`) – how long to wait when `wait` is true.

**Response (success)**

- `ok`: `true`
- `job_id`, `run_id`, `run_url` (if `DBT_CLOUD_PROJECT_ID` is set)
- `job_run`: raw run payload from the API
- If `wait` was true: `run_status`, `run_status_human`, and `run_result` with:
  - `models_total`, `models_success`, `models_error`, `models_skipped`
  - `elapsed_time_seconds`
  - `run_status_summary` (`"success"` or `"error"`)
  - `models_detail`: list of per-model status, execution time, relation name, rows affected, message

**Example**

```bash
# Trigger BigQuery job and wait for result
curl -X POST https://YOUR_SERVICE_URL \
  -H "Content-Type: application/json" \
  -d '{"job": "bigquery"}'

# Trigger AlloyDB job
curl -X POST https://YOUR_SERVICE_URL \
  -H "Content-Type: application/json" \
  -d '{"job": "alloydb"}'

# Trigger and don't wait (poll later with GET)
curl -X POST https://YOUR_SERVICE_URL \
  -H "Content-Type: application/json" \
  -d '{"job": "bigquery", "wait": false}'
```

### GET – run status and result

**Query**

- `run_id`: integer – the run ID returned from a previous POST.

**Response**

- `run_id`, `run_status`, `run_status_human`
- If the run finished successfully: `run_result` (same shape as in POST response).

**Example**

```bash
curl "https://YOUR_SERVICE_URL?run_id=70471867338954"
```

## Running

### Local (no functions_framework required for trigger)

Without deploying, you can trigger a run and wait for the result:

```bash
export DBT_CLOUD_ACCOUNT_ID=...
export DBT_CLOUD_JOB_ID=...
export DBT_CLOUD_API_TOKEN=...
python main.py
```

This uses the default BigQuery job and `cause="Triggered from Figma"`.

### Cloud Run / Cloud Functions

With `functions_framework` installed, run the HTTP server locally:

```bash
functions_framework --target=main --port=8080
```

Then send POST/GET requests to `http://localhost:8080`. Deploy the same entrypoint (`main.main`) to Cloud Run or Cloud Functions and set the environment variables there.

## Run status codes

The script maps dbt Cloud run statuses as follows:

- `1` – Queued  
- `2` – Starting  
- `3` – Running  
- `10` – Success  
- `20` – Error  
- `30` – Cancelled  

When waiting for a run, the script polls until status is 10, 20, or 30 (or until `wait_timeout_seconds`).

## Files

- **`main.py`** – HTTP handler (`main`), trigger logic (`trigger_dbt_cloud_run`), polling (`poll_run_until_done`), and run result summarization from `run_results.json`.
- **`requirements.txt`** – Python dependencies (`functions-framework`, `requests`).
