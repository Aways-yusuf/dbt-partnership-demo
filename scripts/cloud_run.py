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


def upload_files_to_gcs(bucket_name: str, json_response: str, parent_dir: str):
    """
    Parses JSON and uploads files to GCS
    """

    # 1️⃣ Clean response
    cleaned_json = clean_ai_response(json_response)

    # 2️⃣ Parse JSON safely
    try:
        files_dict = json.loads(cleaned_json)
    except json.JSONDecodeError as e:
        print("❌ Invalid JSON returned from model")
        print(e)
        return

    # 3️⃣ Initialize GCS client
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
def convert_ssis_to_dbt(ssis_content, store_procedures_content, database_schema_content):
    vertexai.init(project=PROJECT_ID, location=LOCATION)

    model = GenerativeModel(MODEL_NAME)

    prompt = f"""
You are a senior Data Engineer specializing in converting SSIS packages into dbt models for BigQuery.
Your task:
Convert the provided SSIS package XML and SQL stored procedures into a complete dbt project structure.
STRICT OUTPUT RULES:
1. Return ONLY valid JSON.
2. Do NOT include explanations.
3. Do NOT include markdown.
4. Do NOT wrap output in ```json```.
5. The response must be a single valid JSON object.
6. Each key must be the full filepath.
7. Each value must contain the full file content as a string.

JSON FORMAT REQUIRED:
{{
  "models/staging/stg_cities.sql": "file content here",
  "models/intermediate/int_city_prep.sql": "file content here",
  "models/dimensions/dim_city.sql": "file content here",
  "models/dimensions/schema.yml": "file content here",
  "models/sources.yml": "file content here",
  "models/facts/fact_sale.sql": "file content here"
}}

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
   - use safe_cast for integer columns

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
   - Wrap all  this  references inside % if is_incremental() %
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

    response = model.generate_content(prompt)

    return response.text

# ==============================
# 3️⃣ Main Function
# ==============================
@functions_framework.http
def main(request):
    bucket_name = "wwi-ssis-dbt-demo"
    #file_path = "DailyETLMain.dtsx"

    request_json = request.get_json()

    file_path = request_json["file_path"]
    if not file_path:
        return "Missing file_path parameter", 400
        

    parent_dir = "DBT/dbt-partnership-demo"

    print("Reading SSIS package from GCS")
    ssis_content = read_from_gcs(bucket_name, file_path)
    print("Reading store procedures from GCS")
    store_procedures_content = read_from_gcs(bucket_name, "WWI-store-procedures.sql")
    print("Reading database schema from GCS")
    database_schema_content = read_from_gcs(bucket_name, "WWI_BQ_ddl.sql")

    print("Converting SSIS package to dbt models")
    dbt_output = convert_ssis_to_dbt(ssis_content, store_procedures_content, database_schema_content)
    print("Uploading dbt models to GCS")
    cleaned_json = clean_ai_response(dbt_output)
    upload_files_to_gcs(bucket_name, dbt_output, parent_dir)
    
    return cleaned_json
    

