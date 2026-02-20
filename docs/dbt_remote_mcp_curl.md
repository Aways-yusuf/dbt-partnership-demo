# Using dbt MCP Remote with curl

The dbt Cloud remote MCP server speaks **JSON-RPC 2.0** over HTTP. You can call it with `curl` using the same base URL and headers as in your Cursor `mcp.json`.

## Troubleshooting

### `curl: option : blank argument where content is expected`

This means one of the header values is **empty** — i.e. an environment variable is unset. Either:

1. **Run the export commands (section 1) in the same terminal** before running curl, then run the curl in that same shell.
2. **Check that all vars are set:**  
   `echo "URL:$DBT_MCP_URL AUTH:${DBT_MCP_AUTH:0:20}... PROD:$DBT_PROD_ENV_ID USER:$DBT_USER_ID DEV:$DBT_DEV_ENV_ID"`  
   If any value is missing, re-run the exports from section 1.

### `jq: parse error: Invalid numeric literal at line 1, column 6`

That error means the server returned **non-JSON** (usually an HTML error page). Common causes:

- **401 Unauthorized** – Wrong or expired token, or missing/incorrect headers.
- **403 Forbidden** – Token valid but no access to this environment.
- **500 / 502** – Server error (HTML error page).

**Debug:** Run the same `curl` command **without** `| jq .` to see the raw response. If you see `<html>` or `401 Authorization Required`, fix your auth (see section 1). Use the **exact** `Authorization` value from `mcp.json` (it usually already includes `Bearer `).

**Use the safe pipe below** so that when the server returns HTML (e.g. 401), you see the real response instead of a jq error. All examples in this doc use it:

```bash
# Safe: pretty-print JSON, or show raw response if not JSON (e.g. 401 HTML)
... | tee /tmp/dbt_mcp_resp.txt | (jq . 2>/dev/null || cat /tmp/dbt_mcp_resp.txt)
```

## 1. Set your credentials (from `~/.cursor/mcp.json`)

```bash
# Copy these from the "dbt-mcp-remote" -> "headers" section of mcp.json
# Use the Authorization value exactly as in mcp.json (it usually already includes "Bearer ")
export DBT_MCP_URL="https://yh400.us1.dbt.com/api/ai/v1/mcp/"
export DBT_MCP_AUTH="Bearer dbtu_GhXMfi4aaPoRfnKunbrtpcjkjRjbAQKvEdGpsubAZe3i3zImgo" 
export DBT_PROD_ENV_ID="70471823520349"    
export DBT_USER_ID="70471823501502"                    
export DBT_DEV_ENV_ID="70471823519177"     
```

**Alternative – one-off curl without env vars:**  
If you prefer not to use exports, replace the placeholders in this single command and run it (no spaces around `=` in the `-H` values):

```bash
curl -s -X POST "https://yh400.us1.dbt.com/api/ai/v1/mcp/" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "x-dbt-prod-environment-id: YOUR_PROD_ENV_ID" \
  -H "x-dbt-user-id: YOUR_USER_ID" \
  -H "x-dbt-dev-environment-id: YOUR_DEV_ENV_ID" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | tee /tmp/dbt_mcp_resp.txt | (jq . 2>/dev/null || cat /tmp/dbt_mcp_resp.txt)
```

## 2. List available tools

```bash
curl -s -X POST "$DBT_MCP_URL" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "Authorization: $DBT_MCP_AUTH" \
  -H "x-dbt-prod-environment-id: $DBT_PROD_ENV_ID" \
  -H "x-dbt-user-id: $DBT_USER_ID" \
  -H "x-dbt-dev-environment-id: $DBT_DEV_ENV_ID" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/list"
  }' | tee /tmp/dbt_mcp_resp.txt | (jq . 2>/dev/null || cat /tmp/dbt_mcp_resp.txt)
```

## 3. Call a tool (e.g. get all models)

```bash
curl -s -X POST "$DBT_MCP_URL" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "Authorization: $DBT_MCP_AUTH" \
  -H "x-dbt-prod-environment-id: $DBT_PROD_ENV_ID" \
  -H "x-dbt-user-id: $DBT_USER_ID" \
  -H "x-dbt-dev-environment-id: $DBT_DEV_ENV_ID" \
  -d '{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/call",
    "params": {
      "name": "get_all_models",
      "arguments": {}
    }
  }' | tee /tmp/dbt_mcp_resp.txt | (jq . 2>/dev/null || cat /tmp/dbt_mcp_resp.txt)
```

## 4. Call a tool with arguments (e.g. get model details)

```bash
curl -s -X POST "$DBT_MCP_URL" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "Authorization: $DBT_MCP_AUTH" \
  -H "x-dbt-prod-environment-id: $DBT_PROD_ENV_ID" \
  -H "x-dbt-user-id: $DBT_USER_ID" \
  -H "x-dbt-dev-environment-id: $DBT_DEV_ENV_ID" \
  -d '{
    "jsonrpc": "2.0",
    "id": 3,
    "method": "tools/call",
    "params": {
      "name": "get_model_details",
      "arguments": {
        "name": "fct_order"
      }
    }
  }' | tee /tmp/dbt_mcp_resp.txt | (jq . 2>/dev/null || cat /tmp/dbt_mcp_resp.txt)
```

## 5. Other useful tools (same pattern, different `name` and `arguments`)

| Tool               | Arguments example |
|--------------------|-------------------|
| `get_mart_models`  | `{}` |
| `get_lineage`      | `{"unique_id": "model.your_project.model_name"}` |
| `get_model_health` | `{"name": "model_name"}` or `{"unique_id": "model.project.model"}` |
| `list_metrics`     | `{}` or `{"search": "revenue"}` |
| `get_all_sources`  | `{}` |
| `get_exposures`    | `{}` |
| `query_metrics`    | `{"metrics": ["metric_name"], "group_by": [...], "limit": 10}` |
| `execute_sql`      | `{"sql": "SELECT ..."}` (SQL string to run; remote MCP requires PAT + x-dbt-user-id) |

Request format is always:

- **Method:** `tools/call`
- **params.name:** tool name (e.g. `get_all_models`, `get_lineage`)
- **params.arguments:** object of arguments (use `{}` when none required)

Response is JSON-RPC 2.0: look at `result.content[].text` for the tool output (or `result` for structured data, depending on the tool).

## 6. Optional: initialize first (if the server requires it)

Some MCP servers expect an `initialize` handshake before other calls:

```bash
# Initialize
curl -s -X POST "$DBT_MCP_URL" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "Authorization: $DBT_MCP_AUTH" \
  -H "x-dbt-prod-environment-id: $DBT_PROD_ENV_ID" \
  -H "x-dbt-user-id: $DBT_USER_ID" \
  -H "x-dbt-dev-environment-id: $DBT_DEV_ENV_ID" \
  -d '{
    "jsonrpc": "2.0",
    "id": 0,
    "method": "initialize",
    "params": {
      "protocolVersion": "2024-11-05",
      "capabilities": {},
      "clientInfo": { "name": "curl", "version": "1.0" }
    }
  }' | tee /tmp/dbt_mcp_resp.txt | (jq . 2>/dev/null || cat /tmp/dbt_mcp_resp.txt)
```

Then send an `initialized` notification (no `id`, no response expected):

```bash
curl -s -X POST "$DBT_MCP_URL" \
  -H "Content-Type: application/json" \
  -H "Authorization: $DBT_MCP_AUTH" \
  -H "x-dbt-prod-environment-id: $DBT_PROD_ENV_ID" \
  -H "x-dbt-user-id: $DBT_USER_ID" \
  -H "x-dbt-dev-environment-id: $DBT_DEV_ENV_ID" \
  -d '{"jsonrpc":"2.0","method":"initialized"}' | tee /tmp/dbt_mcp_resp.txt | (jq . 2>/dev/null || cat /tmp/dbt_mcp_resp.txt)
```

After that, use `tools/list` and `tools/call` as above.

---

**Security:** Do not commit real tokens. Use env vars (as in section 1) and add `mcp.json` to `.gitignore` if it lives in a repo.

---

## Python helper for formatted output

Use the script in `scripts/dbt_mcp_call.py` to call the remote MCP and get nicely formatted output (no jq, no curl by hand).

**Setup:** `pip install -r scripts/requirements.txt` (or `pip install requests`). Config is read from the same env vars as above or from `~/.cursor/mcp.json` (dbt-mcp-remote).

**Environment variables when calling:**

You can pass env vars in two ways:

1. **Inline** (only for that single command; no need to export):
   ```bash
   DBT_MCP_URL="https://yh400.us1.dbt.com/api/ai/v1/mcp/" \
   DBT_MCP_AUTH="Bearer YOUR_TOKEN" \
   DBT_PROD_ENV_ID="70471823520349" \
   DBT_USER_ID="70471823501502" \
   DBT_DEV_ENV_ID="70471823519177" \
   python scripts/dbt_mcp_call.py get_all_models
   ```

2. **Export in the same shell**, then run the script:
   ```bash
   export DBT_MCP_URL="https://yh400.us1.dbt.com/api/ai/v1/mcp/"
   export DBT_MCP_AUTH="Bearer YOUR_TOKEN"
   export DBT_PROD_ENV_ID="70471823520349"
   export DBT_USER_ID="70471823501502"
   export DBT_DEV_ENV_ID="70471823519177"
   python scripts/dbt_mcp_call.py get_all_models
   ```

If you don't set these, the script uses `~/.cursor/mcp.json` (dbt-mcp-remote section).

**Examples:**

```bash
# List available tools
python scripts/dbt_mcp_call.py --list-tools

# Call a tool (no arguments)
python scripts/dbt_mcp_call.py get_all_models

# Call a tool with arguments (JSON string)
python scripts/dbt_mcp_call.py get_model_details '{"name": "my_model"}'
python scripts/dbt_mcp_call.py list_metrics '{"search": "revenue"}'
python scripts/dbt_mcp_call.py get_lineage '{"unique_id": "model.my_project.my_model"}'

# From a curl command (paste the full curl; script parses -d body and uses configured URL/headers)
python scripts/dbt_mcp_call.py --curl 'curl -s -X POST "https://..." -H "Authorization: Bearer TOKEN" ... -d '"'"'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_all_models","arguments":{}}}'"'"''

# Raw JSON response (no formatting)
python scripts/dbt_mcp_call.py get_all_models --raw
```

### execute_sql – run SQL on the warehouse

The `execute_sql` tool runs SQL on dbt’s backend (your warehouse). It expects one argument:

| Argument | Type   | Required | Description                    |
|----------|--------|----------|--------------------------------|
| `sql`    | string | Yes      | The SQL statement to execute.  |

**Requirements for remote MCP:** You must use a **personal access token (PAT)** in `Authorization`, not a service token, and you must set the `x-dbt-user-id` header (from `~/.cursor/mcp.json` or env).

**Examples:**

```bash
# One-line SQL (pass JSON with "sql" key)
python scripts/dbt_mcp_call.py execute_sql '{"sql": "SELECT 1 AS num"}'
python scripts/dbt_mcp_call.py execute_sql '{"sql": "SELECT * FROM my_schema.dim_customer LIMIT 5"}'

# fct_purchase has: date_key, supplier_key, stock_item_key, wwi_purchase_order_id, ordered_outers, ordered_quantity, received_outers, package, is_order_finalized (no "id" column)
python scripts/dbt_mcp_call.py execute_sql '{"sql": "SELECT * FROM dbt_target.fct_purchase WHERE wwi_purchase_order_id = 1"}'
python scripts/dbt_mcp_call.py execute_sql '{"sql": "SELECT * FROM dbt_target.fct_purchase LIMIT 5"}'

# Multi-line SQL: pass a JSON string (escape double quotes inside the SQL)
python scripts/dbt_mcp_call.py execute_sql '{"sql": "SELECT id, name FROM my_schema.my_table WHERE id = 1"}'
```

**Multi-line SQL from a file (bash):**

```bash
# Build JSON with sql from a file
SQL=$(cat my_query.sql | jq -Rs '{sql: .}')
python scripts/dbt_mcp_call.py execute_sql "$SQL"
```

Or inline with newlines (escape or use $'...'):

```bash
python scripts/dbt_mcp_call.py execute_sql $'{"sql": "SELECT id\nFROM my_table\nLIMIT 10"}'
```



```bash

DBT remote command, MCP Curl command

python3 scripts/dbt_mcp_call.py get_all_models
python3 scripts/dbt_mcp_call.py get_all_macros
python3 scripts/dbt_mcp_call.py get_model_details '{"name": "fct_purchase"}'
python3 scripts/dbt_mcp_call.py get_model_parents '{"name": "fct_order"}'
python3 scripts/dbt_mcp_call.py get_model_parents '{"name": "fct_transaction"}'
python3 scripts/dbt_mcp_call.py get_lineage '{"unique_id": "model.dbt_partnership_demo.fct_purchase"}'
python3 scripts/dbt_mcp_call.py execute_sql '{"sql": "SELECT * FROM dbt_target.fct_purchase LIMIT 5"}'


DBT Local commands

get_lineage of fct_order
list
run
test
list_job_runs
get_job_run_details 70471866414705

```