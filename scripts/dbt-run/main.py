"""
Trigger a dbt Cloud job (e.g. dbt run) from an API call.
Call this from Figma (or any client) to run dbt in dbt Cloud.

URL mapping (from a run URL like):
  https://yh400.us1.dbt.com/deploy/70471823514028/projects/70471823552417/runs/70471867338954
  -> base: https://yh400.us1.dbt.com
  -> account_id (deploy): 70471823514028  -> set DBT_CLOUD_ACCOUNT_ID
  -> project_id: 70471823552417
  -> run_id: 70471867338954 (one execution; do NOT use this as job_id)

  JOB ID is different: go to dbt Cloud -> Jobs -> click your job -> URL has /jobs/XXXXX; that XXXXX is DBT_CLOUD_JOB_ID.
  Run URL has /runs/YYYYY; YYYY is run_id only. Use job_id (from Jobs), not run_id.

Env vars (required):
  DBT_CLOUD_ACCOUNT_ID  - from deploy segment above (e.g. 70471823514028)
  DBT_CLOUD_JOB_ID      - JOB id for BigQuery job from Jobs page URL (.../jobs/XXXXX), NOT the run_id from .../runs/YYYYY
  DBT_CLOUD_JOB_ID_2    - JOB id for AlloyDB job (e.g. 70471823569441)
  DBT_CLOUD_API_TOKEN   - dbt Cloud API token (Account -> API Access)

Env vars (optional, for run_url in response):
  DBT_CLOUD_PROJECT_ID  - project ID from URL (e.g. 70471823552417) so response includes run_url to open the run

Example from Figma / curl:
  POST: trigger run and wait for completion. Body can include:
    "job": "bigquery" | "alloydb" - which DBT Cloud job to run (default: "bigquery")
    "cause": "...", "wait": false, "wait_timeout_seconds": 600
  GET ?run_id=XXXXX: get run status and run_result for an existing run (e.g. if you triggered with "wait": false).

Curl examples:
  curl -X POST https://YOUR_SERVICE_URL -H "Content-Type: application/json" -d '{"job": "bigquery"}'
  curl -X POST https://YOUR_SERVICE_URL -H "Content-Type: application/json" -d '{"job": "alloydb"}'
"""

import os
import time
import requests

# Optional: use functions_framework for Cloud Run/Cloud Functions
try:
    import functions_framework
except ImportError:
    functions_framework = None

# Use your region URL if needed, e.g. https://yh400.us1.dbt.com
DBT_CLOUD_BASE = os.environ.get("DBT_CLOUD_BASE", "https://yh400.us1.dbt.com")
RUN_ENDPOINT = "/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
RUN_GET_ENDPOINT = "/api/v2/accounts/{account_id}/runs/{run_id}/"
RUN_ARTIFACTS_ENDPOINT = "/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/run_results.json"

# Run status codes from dbt Cloud API
STATUS_QUEUED, STATUS_STARTING, STATUS_RUNNING = 1, 2, 3
STATUS_SUCCESS, STATUS_ERROR, STATUS_CANCELLED = 10, 20, 30
FINAL_STATUSES = (STATUS_SUCCESS, STATUS_ERROR, STATUS_CANCELLED)


def _headers(api_token: str):
    h = {"Content-Type": "application/json"}
    if api_token.startswith("dbts_") or "Token" in os.environ.get("DBT_CLOUD_AUTH_TYPE", ""):
        h["Authorization"] = f"Token {api_token}"
    else:
        h["Authorization"] = f"Bearer {api_token}"
    return h


def get_run_status(account_id: str, run_id: int, api_token: str) -> tuple[int, dict]:
    """GET run status. Returns (status_code, response_data)."""
    url = f"{DBT_CLOUD_BASE.rstrip('/')}{RUN_GET_ENDPOINT.format(account_id=account_id, run_id=run_id)}"
    try:
        r = requests.get(url, headers=_headers(api_token), timeout=30)
        return r.status_code, r.json() if r.text else {}
    except requests.RequestException:
        return 0, {}


def get_run_results_artifact(account_id: str, run_id: int, api_token: str) -> dict | None:
    """GET run_results.json artifact. Returns parsed JSON or None."""
    url = f"{DBT_CLOUD_BASE.rstrip('/')}{RUN_ARTIFACTS_ENDPOINT.format(account_id=account_id, run_id=run_id)}"
    try:
        r = requests.get(url, headers=_headers(api_token), timeout=30)
        if r.ok and r.text:
            return r.json()
        return None
    except requests.RequestException:
        return None


def summarize_run_results(artifact: dict) -> dict:
    """Build summary from run_results.json: counts, elapsed time, and per-model details."""
    results = artifact.get("results") or []
    success = sum(1 for n in results if (n.get("status") or "").upper() == "SUCCESS")
    error = sum(1 for n in results if (n.get("status") or "").upper() in ("ERROR", "FAIL"))
    skipped = sum(1 for n in results if (n.get("status") or "").upper() == "SKIP")
    total = len(results)
    elapsed = artifact.get("elapsed_time")

    # Per-model details (each node in the run)
    models_detail = []
    for n in results:
        adapter = n.get("adapter_response") or {}
        models_detail.append({
            "unique_id": n.get("unique_id"),
            "status": n.get("status"),
            "execution_time": n.get("execution_time"),
            "relation_name": n.get("relation_name"),  # e.g. database.schema.table
            "rows_affected": adapter.get("rows_affected"),
            "message": n.get("message"),  # often set on error
        })

    return {
        "models_total": total,
        "models_success": success,
        "models_error": error,
        "models_skipped": skipped,
        "elapsed_time_seconds": elapsed,
        "run_status_summary": "success" if error == 0 else "error",
        "models_detail": models_detail,
    }


def poll_run_until_done(
    account_id: str, run_id: int, api_token: str, timeout_seconds: int = 600, poll_interval: int = 10
) -> tuple[int | None, dict]:
    """Poll run status until SUCCESS/ERROR/CANCELLED. Returns (status_code, run_data)."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        sc, data = get_run_status(account_id, run_id, api_token)
        if sc != 200:
            return None, {}
        run_data = (data.get("data") or {}) if isinstance(data.get("data"), dict) else {}
        status = run_data.get("status")
        if status is not None and status in FINAL_STATUSES:
            return status, run_data
        time.sleep(poll_interval)
    return None, {}


def trigger_dbt_cloud_run(cause: str = "Triggered from API", wait_for_result: bool = True, wait_timeout_seconds: int = 600, job_id_override: str | None = None) -> tuple[dict, int]:
    """
    Trigger a dbt Cloud job run. Returns (response_data, status_code).
    job_id_override: if set, use this job id; otherwise use DBT_CLOUD_JOB_ID (BigQuery).
    """
    account_id = os.environ.get("DBT_CLOUD_ACCOUNT_ID", "").strip()
    job_id = (job_id_override or os.environ.get("DBT_CLOUD_JOB_ID", "")).strip()
    api_token = os.environ.get("DBT_CLOUD_API_TOKEN", "").strip()

    if not account_id or not job_id or not api_token:
        return {
            "error": "Missing config",
            "hint": "Set DBT_CLOUD_ACCOUNT_ID, DBT_CLOUD_JOB_ID (and DBT_CLOUD_JOB_ID_2 for alloydb), DBT_CLOUD_API_TOKEN",
        }, 500

    url = f"{DBT_CLOUD_BASE.rstrip('/')}{RUN_ENDPOINT.format(account_id=account_id, job_id=job_id)}"
    try:
        r = requests.post(url, headers=_headers(api_token), json={"cause": cause}, timeout=30)
        data = r.json() if r.text else {}
        if not r.ok:
            hint = ""
            if r.status_code == 404:
                hint = "DBT_CLOUD_JOB_ID must be the JOB id from Jobs page URL (.../jobs/XXXXX), not the RUN id from .../runs/YYYYY. Check account_id and job_id."
            return {"error": "dbt Cloud API error", "details": data, "status": r.status_code, "hint": hint or None}, r.status_code
        run_data = data.get("data") or {}
        run_id = run_data.get("id")
        project_id = run_data.get("project_id") or os.environ.get("DBT_CLOUD_PROJECT_ID", "")
        base = DBT_CLOUD_BASE.rstrip("/")
        run_url = f"{base}/deploy/{account_id}/projects/{project_id}/runs/{run_id}" if (run_id and account_id and project_id) else None
        out = {"ok": True, "job_id": job_id, "run_id": run_id, "run_url": run_url, "job_run": run_data}

        # By default: wait for run to finish, then fetch and return run_result (models_total, models_success, etc.)
        if wait_for_result and run_id:
            status, final_run = poll_run_until_done(account_id, run_id, api_token, timeout_seconds=wait_timeout_seconds)
            out["run_status"] = final_run.get("status")
            out["run_status_human"] = {STATUS_SUCCESS: "success", STATUS_ERROR: "error", STATUS_CANCELLED: "cancelled"}.get(status, "timeout_or_unknown")
            if status == STATUS_SUCCESS:
                artifact = get_run_results_artifact(account_id, run_id, api_token)
                out["run_result"] = summarize_run_results(artifact) if artifact else None
            else:
                out["run_result"] = None
        return out, 200
    except requests.RequestException as e:
        return {"error": "Request failed", "details": str(e)}, 502


if functions_framework:

    @functions_framework.http
    def main(request):
        if request.method == "OPTIONS":
            return ("", 204, {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type"})

        if request.method == "GET":
            run_id = request.args.get("run_id", "").strip()
            if not run_id:
                return {"error": "Missing run_id query param"}, 400
            try:
                run_id = int(run_id)
            except ValueError:
                return {"error": "run_id must be an integer"}, 400
            account_id = os.environ.get("DBT_CLOUD_ACCOUNT_ID", "").strip()
            api_token = os.environ.get("DBT_CLOUD_API_TOKEN", "").strip()
            if not account_id or not api_token:
                return {"error": "Missing config"}, 500
            sc, data = get_run_status(account_id, run_id, api_token)
            if sc != 200:
                return {"error": "Failed to get run", "details": data}, sc if sc else 502
            run_data = (data.get("data") or {}) if isinstance(data.get("data"), dict) else {}
            status_code = run_data.get("status")
            out = {"run_id": run_id, "run_status": status_code, "run_status_human": {STATUS_SUCCESS: "success", STATUS_ERROR: "error", STATUS_CANCELLED: "cancelled"}.get(status_code, "running")}
            if status_code == STATUS_SUCCESS:
                artifact = get_run_results_artifact(account_id, run_id, api_token)
                out["run_result"] = summarize_run_results(artifact) if artifact else None
            else:
                out["run_result"] = None
            return out, 200

        if request.method != "POST":
            return {"error": "Use POST to trigger dbt run or GET ?run_id= to get result"}, 405

        try:
            body = request.get_json(silent=True) or {}
            print("Try block body:", body)
        except Exception:
            body = {}
            print("Excpet block body:", body)
        cause = (body.get("cause") or "Triggered from Figma").strip()
        # Resolve job: "bigquery" -> BigQuery job, "alloydb" -> AlloyDB job
        job_key = (body.get("job") or "bigquery").strip().lower()
        if job_key == "alloydb":
            job_id = os.environ.get("DBT_CLOUD_JOB_ID_2", "").strip()
        else:
            job_id = os.environ.get("DBT_CLOUD_JOB_ID", "").strip()
        if not job_id:
            return {"error": "Missing job config", "hint": "Set DBT_CLOUD_JOB_ID for bigquery and DBT_CLOUD_JOB_ID_2 for alloydb"}, 500
        # Default True: one POST = trigger + wait + return run_result in same response
        wait_for_result = body.get("wait", True) is not False
        print("Wait for result:", wait_for_result)
        wait_timeout_seconds = min(int(body.get("wait_timeout_seconds", 600)), 3600)
        print("Wait timeout seconds:", wait_timeout_seconds)
        data, status = trigger_dbt_cloud_run(cause=cause, wait_for_result=wait_for_result, wait_timeout_seconds=wait_timeout_seconds, job_id_override=job_id)
        print("Data:", data)
        print("Status", status)
        return data, status

else:
    # Run locally for testing (waits and returns run_result by default)
    if __name__ == "__main__":
        data, status = trigger_dbt_cloud_run(cause="Triggered from Figma", wait_for_result=True)
        print("Status:", status)
        print("Response:", data)
