import functions_framework
import os
import re
import json

from google.cloud import storage
import vertexai
from vertexai.generative_models import GenerativeModel
# ==============================
# CONFIG
# ==============================
PROJECT_ID = "data-platforms-66d-demos"
LOCATION = "us-central1"
MODEL_NAME = "gemini-2.5-pro"

# ==============================
# 1️⃣ Read SSIS package from GCS
# ==============================
def read_from_gcs(bucket_name, file_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    return blob.download_as_text()


def clean_ai_response(response_text: str) -> str:
    """
    Removes ```json ... ``` wrapper if present
    """
    # Remove markdown code fences
    cleaned = re.sub(r"^```json\s*", "", response_text.strip())
    cleaned = re.sub(r"^```", "", cleaned)
    cleaned = re.sub(r"```$", "", cleaned)
    return cleaned.strip()


def _escape_control_chars_in_json_strings(s: str) -> str:
    """
    Escape unescaped control characters (\\n, \\r, \\t) only inside JSON string values.
    Fixes 'Invalid control character' when the model returns literal newlines inside values.
    """
    result = []
    i = 0
    in_string = False
    escape_next = False
    while i < len(s):
        c = s[i]
        if escape_next:
            result.append(c)
            escape_next = False
            i += 1
            continue
        if not in_string:
            result.append(c)
            if c == '"':
                in_string = True
            i += 1
            continue
        # Inside a string value
        if c == "\\":
            result.append(c)
            escape_next = True
            i += 1
        elif c == '"':
            result.append(c)
            in_string = False
            i += 1
        elif c == "\n":
            result.append("\\n")
            i += 1
        elif c == "\r":
            result.append("\\r")
            i += 1
        elif c == "\t":
            result.append("\\t")
            i += 1
        elif ord(c) < 32:
            # Other ASCII control characters
            result.append(f"\\u{ord(c):04x}")
            i += 1
        else:
            result.append(c)
            i += 1
    return "".join(result)


def upload_files_to_gcs(bucket_name: str, json_response: str, parent_dir: str):
    """
    Parses JSON and uploads files to GCS
    """

    # 1️⃣ Clean response
    cleaned_json = clean_ai_response(json_response)

    # 2️⃣ Escape control chars inside JSON string values (fixes model output with raw newlines)
    cleaned_json = _escape_control_chars_in_json_strings(cleaned_json)

    # 3️⃣ Parse JSON safely
    try:
        files_dict = json.loads(cleaned_json)
    except json.JSONDecodeError as e:
        print("❌ Invalid JSON returned from model")
        print(e)
        return

    # 4️⃣ Initialize GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # 4️⃣ Iterate and upload
    for file_path, file_content in files_dict.items():
        print(f"Uploading: {parent_dir}/{file_path}")

        blob = bucket.blob(parent_dir + "/" + file_path)
        blob.upload_from_string(file_content, content_type="text/plain")

    print("✅ All files uploaded successfully!")


# ==============================
# 2️⃣ Convert SSIS → DBT using Vertex AI
# ==============================

# Shared rules (same for BigQuery and AlloyDB) — no rule or logic is omitted for either warehouse.
DETERMINISTIC_RULES = """
DETERMINISTIC EXECUTION RULES
1. Output must be COMPLETELY DETERMINISTIC.
2. For identical inputs, the output structure, filenames, ordering, and formatting MUST be identical.
3. Do NOT infer optional models.
4. Do NOT create extra helper models.
5. Do NOT skip detected entities.
6. One entity = exactly: - 1 staging model
   - 1 intermediate model
   - 1 final model (dimension OR fact)
7. File names must use ONLY the exact source table name in lowercase snake_case.
8. Sort all generated files alphabetically by filepath before returning JSON.
9. Sort columns alphabetically inside every SELECT statement.
10. Use consistent indentation (2 spaces).
11. Do NOT add timestamps, random aliases, or variable suffixes.
12. Do NOT pluralize or singularize names. Use exact detected table names.
13. If no primary key exists, use the first column alphabetically as deterministic key.
14. If entity type cannot be determined, classify as dimension.
15. Don't need date models.
16. Do NOT change naming conventions across runs.
17. DO NOT create additional models in different runs.
18. DO NOT rename models in different runs.
19. DO NOT invent alternative naming patterns.
"""

STRICT_OUTPUT_RULES = """
STRICT OUTPUT RULES:
1. Return ONLY valid JSON.
2. Do NOT include explanations.
3. Do NOT include markdown.
4. Do NOT wrap output in ```json```.
5. The response must be a single valid JSON object.
6. Each key must be the full filepath.
7. Each value must contain the full file content as a string.
"""

REQUIRED_STRUCTURE = """
REQUIRED PROJECT STRUCTURE (NO DEVIATION ALLOWED):
models/staging/stg_<entity_name>.sql
models/intermediate/int_<entity_name>.sql
models/dimensions/dim_<entity_name>.sql (if dimension)
models/facts/fct_<entity_name>.sql (if fact)
models/dimensions/schema.yml
models/facts/schema.yml
models/sources.yml
macros/generate_schema_name.sql
"""

MACRO_REQUIRED = """
MACRO REQUIRED - Create this exact file (macros/generate_schema_name.sql):
Use +schema from dbt_project (e.g. dbt_staging for staging/intermediate, dbt_target for dimensions/facts).
Only fall back to target.schema when no custom schema is set.
Include the following macro verbatim in macros/generate_schema_name.sql:

{{# Use +schema from dbt_project (dbt_staging for staging/intermediate, dbt_target for dimensions/facts).
   Only fall back to target.schema when no custom schema is set. #}}
{{% macro generate_schema_name(custom_schema_name, node) -%}}
    {{%- set default_schema = target.schema -%}}
    {{%- if custom_schema_name is not none and custom_schema_name | trim != '' -%}}
        {{{{ custom_schema_name | trim }}}}
    {{%- else -%}}
        {{{{ default_schema }}}}
    {{%- endif -%}}
{{%- endmacro %}}
"""

JSON_FORMAT_REQUIRED = """
JSON FORMAT REQUIRED:
{{
  "models/staging/file_name.sql": "file content here",
  "models/intermediate/file_name.sql": "file content here",
  "models/dimensions/file_name.sql": "file content here",
  "models/sources.yml": "file content here",
  "models/facts/file_name.sql": "file content here",
  "macros/generate_schema_name.sql": "file content here"
}}
"""

ARCHITECTURE_BIGQUERY = """
ARCHITECTURE REQUIREMENTS:

1. Follow layered dbt architecture:
   - models/staging/
   - models/intermediate/
   - models/dimensions/
   - models/facts/

2. All staging models:
   - Rename columns to snake_case
   - Explicitly cast numeric columns to int64 or float64
   - Use SAFE_CAST
   - Parse timestamps using:
     COALESCE(
       SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S%Ez', CAST(col AS STRING)),
       SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', CAST(col AS STRING)),
       SAFE_CAST(col AS TIMESTAMP)
     )
   - Remove NULL primary keys
   - Use SAFE_CAST for integer columns

3. Intermediate models:
   - Join entities using temporal logic
   - Use valid_from and valid_to logic
   - Use GREATEST() when combining change timestamps

4. Dimension models:
   - Must implement SCD Type 2
   - Use:
        is_current
        valid_from
        valid_to
   - Use incremental materialization
   - Separate first run vs incremental logic
   - Wrap all this references inside {% if is_incremental() %}
   - Use dbt_utils.generate_surrogate_key()
   - Prevent NULL surrogate key inputs

5. Fact models:
   - Join to dimensions using:
       fact_timestamp > dim.valid_from
       AND fact_timestamp <= dim.valid_to
   - Use surrogate keys
   - Partition by date key
   - Cluster by major dimension keys
   - Support incremental loads (7 day rolling window)

6. Generate models for ALL entities detected in SSIS:
   - All dimensions
   - All facts
   - Do not skip any

7. Use BigQuery SQL syntax only.

8. Assume source tables are already loaded into BigQuery.
"""

ARCHITECTURE_ALLOYDB = """
ARCHITECTURE REQUIREMENTS (AlloyDB / PostgreSQL-compatible):

1. Follow layered dbt architecture:
   - models/staging/
   - models/intermediate/
   - models/dimensions/
   - models/facts/

2. All staging models:
   - Rename columns to snake_case
   - Explicitly cast numeric columns to BIGINT or DOUBLE PRECISION (use CAST or ::)
   - Use CAST for type conversion (PostgreSQL/AlloyDB does not have SAFE_CAST; use CAST and handle nulls in COALESCE if needed)
   - Parse timestamps using PostgreSQL/AlloyDB syntax, e.g.:
     COALESCE(
       TO_TIMESTAMP(CAST(col AS TEXT), 'YYYY-MM-DD HH24:MI:SS.MS TZ'),
       TO_TIMESTAMP(CAST(col AS TEXT), 'YYYY-MM-DD HH24:MI:SS.MS'),
       CAST(col AS TIMESTAMP)
     )
     Or use TO_TIMESTAMP with appropriate format; for TIMESTAMPTZ use AT TIME ZONE if needed.
   - Remove NULL primary keys
   - Use CAST for integer columns (e.g. CAST(col AS BIGINT))

3. Intermediate models:
   - Join entities using temporal logic
   - Use valid_from and valid_to logic
   - Use GREATEST() when combining change timestamps (PostgreSQL supports GREATEST)

4. Dimension models:
   - Must implement SCD Type 2
   - Use:
        is_current
        valid_from
        valid_to
   - Use incremental materialization
   - Separate first run vs incremental logic
   - Wrap all this references inside {% if is_incremental() %}
   - Use dbt_utils.generate_surrogate_key()
   - Prevent NULL surrogate key inputs

5. Fact models:
   - Join to dimensions using:
       fact_timestamp > dim.valid_from
       AND fact_timestamp <= dim.valid_to
   - Use surrogate keys
   - Use PostgreSQL/AlloyDB partitioning if needed (PARTITION BY RANGE, etc.); otherwise standard tables
   - Support incremental loads (7 day rolling window)

6. Generate models for ALL entities detected in SSIS:
   - All dimensions
   - All facts
   - Do not skip any

7. Use AlloyDB/PostgreSQL SQL syntax only (no BigQuery-specific functions).

8. Assume source tables are already loaded into AlloyDB.
"""


def _build_bigquery_prompt(ssis_content: str, store_procedures_content: str, database_schema_content: str) -> str:
    return f"""
You are a senior and a deterministic Data Engineer specializing in converting SSIS packages into dbt models for BigQuery.
Your task:
Convert the provided SSIS package XML and SQL stored procedures into a complete dbt project structure.

{DETERMINISTIC_RULES}
{STRICT_OUTPUT_RULES}
{REQUIRED_STRUCTURE}
{MACRO_REQUIRED}
{JSON_FORMAT_REQUIRED}
{ARCHITECTURE_BIGQUERY}

Now convert the following SSIS package and stored procedures:

SSIS PACKAGE CONTENT:
======================
{ssis_content}
======================

Store procedures:
==================
{store_procedures_content}
==================

DDL
=================
{database_schema_content}
=================

Return only JSON.
"""


def _build_alloydb_prompt(ssis_content: str, store_procedures_content: str, database_schema_content: str) -> str:
    return f"""
You are a senior and a deterministic Data Engineer specializing in converting SSIS packages into dbt models for AlloyDB (PostgreSQL-compatible).
Your task:
Convert the provided SSIS package XML and SQL stored procedures into a complete dbt project structure for AlloyDB.

{DETERMINISTIC_RULES}
{STRICT_OUTPUT_RULES}
{REQUIRED_STRUCTURE}
{MACRO_REQUIRED}
{JSON_FORMAT_REQUIRED}
{ARCHITECTURE_ALLOYDB}

Now convert the following SSIS package and stored procedures:

SSIS PACKAGE CONTENT:
======================
{ssis_content}
======================

Store procedures:
==================
{store_procedures_content}
==================

DDL
=================
{database_schema_content}
=================

Return only JSON.
"""


def convert_ssis_to_dbt(ssis_content, store_procedures_content, database_schema_content, warehouse: str = "bigquery"):
    """
    Convert SSIS to dbt. warehouse must be "bigquery" or "alloydb".
    """
    if warehouse not in ("bigquery", "alloydb"):
        raise ValueError(f'warehouse must be "bigquery" or "alloydb", got: {warehouse!r}')

    vertexai.init(project=PROJECT_ID, location=LOCATION)
    model = GenerativeModel(MODEL_NAME)

    if warehouse == "alloydb":
        prompt = _build_alloydb_prompt(ssis_content, store_procedures_content, database_schema_content)
    else:
        prompt = _build_bigquery_prompt(ssis_content, store_procedures_content, database_schema_content)

    response = model.generate_content(prompt)
    return response.text

# ==============================
# 3️⃣ Main Function
# ==============================
@functions_framework.http
def main(request):
    bucket_name = "wwi-ssis-dbt-demo"

    request_json = request.get_json(silent=True) or {}

    file_path = request_json.get("file_path")
    if not file_path:
        return "Missing file_path parameter", 400

    warehouse = (request_json.get("warehouse") or "bigquery").strip().lower()
    if warehouse not in ("bigquery", "alloydb"):
        return f'Invalid warehouse: use "bigquery" or "alloydb", got: {warehouse!r}', 400

    parent_dir = "DBT/dbt-partnership-demo"

    print("Reading SSIS package from GCS")
    ssis_content = read_from_gcs(bucket_name, file_path)
    print("Reading store procedures from GCS")
    store_procedures_content = read_from_gcs(bucket_name, "WWI-store-procedures.sql")
    print("Reading database schema from GCS")
    database_schema_content = read_from_gcs(bucket_name, "WWI_BQ_ddl.sql")

    print(f"Converting SSIS package to dbt models (target: {warehouse})")
    dbt_output = convert_ssis_to_dbt(ssis_content, store_procedures_content, database_schema_content, warehouse=warehouse)
    print("Uploading dbt models to GCS")
    cleaned_json = clean_ai_response(dbt_output)
    upload_files_to_gcs(bucket_name, dbt_output, parent_dir)
    
    return cleaned_json
    

