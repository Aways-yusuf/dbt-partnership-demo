"""
ADK agent that connects to the dbt remote MCP server via Streamable HTTP.

Uses McpToolset with StreamableHTTPServerParams so the agent can use
all tools exposed by your dbt MCP server (models, metrics, lineage, etc.).

Authentication (no API key for the LLM):
  - Agent model (Gemini): set GOOGLE_GENAI_USE_VERTEXAI=TRUE and GOOGLE_CLOUD_PROJECT;
    run gcloud auth application-default login. No GOOGLE_API_KEY needed.
  - dbt MCP: dbt Cloud only supports token auth. Use DBT_API_TOKEN or DBT_API_TOKEN_FILE
    (path to a file containing the token) so the token is not in .env.

Setup:
  1. python3 -m venv .venv && source .venv/bin/activate
  2. pip install -r requirements.txt
  3. Copy .env.example to .env and set DBT_MCP_URL, DBT_PROD_ENVIRONMENT_ID,
     (DBT_API_TOKEN or DBT_API_TOKEN_FILE), and for Vertex: GOOGLE_GENAI_USE_VERTEXAI=TRUE, GOOGLE_CLOUD_PROJECT
  4. adk run .   or   adk web (from parent folder)

Ref: https://google.github.io/adk-docs/get-started/python/
     https://google.github.io/adk-docs/tools-custom/mcp-tools/
"""

import os

from google.genai import types
from google.adk.agents import LlmAgent
from google.adk.tools.mcp_tool import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StreamableHTTPServerParams

# Connection to your dbt remote MCP server (Streamable HTTP).
# Required: DBT_MCP_URL, (DBT_API_TOKEN or DBT_API_TOKEN_FILE), DBT_PROD_ENVIRONMENT_ID
# Optional (for execute_sql / Fusion): DBT_DEV_ENVIRONMENT_ID, DBT_USER_ID
# See: https://docs.getdbt.com/docs/dbt-ai/setup-remote-mcp
DBT_MCP_URL = os.environ.get("DBT_MCP_URL", "https://yh400.us1.dbt.com/api/ai/v1/mcp/")


def _get_dbt_token() -> str:
    """dbt MCP auth: token from env or from file (no token in .env)."""
    token = os.environ.get("DBT_API_TOKEN", "").strip()
    if token:
        return token
    token_file = os.environ.get("DBT_API_TOKEN_FILE")
    if token_file and os.path.isfile(token_file):
        with open(token_file, encoding="utf-8") as f:
            return f.read().strip()
    return "your-auth-token"  # placeholder so agent loads; MCP calls will fail until set


DBT_API_TOKEN = _get_dbt_token()
DBT_PROD_ENVIRONMENT_ID = os.environ.get("DBT_PROD_ENVIRONMENT_ID", "")
DBT_DEV_ENVIRONMENT_ID = os.environ.get("DBT_DEV_ENVIRONMENT_ID", "")
DBT_USER_ID = os.environ.get("DBT_USER_ID", "")

# dbt remote MCP requires Authorization + x-dbt-prod-environment-id at minimum.
# Use "Token" or "Bearer"; dbt accepts both.
_headers = {
    "Authorization": f"Bearer {DBT_API_TOKEN}",
    "x-dbt-prod-environment-id": DBT_PROD_ENVIRONMENT_ID,
}
if DBT_DEV_ENVIRONMENT_ID:
    _headers["x-dbt-dev-environment-id"] = DBT_DEV_ENVIRONMENT_ID
if DBT_USER_ID:
    _headers["x-dbt-user-id"] = DBT_USER_ID

dbt_mcp_toolset = McpToolset(
    connection_params=StreamableHTTPServerParams(
        url=DBT_MCP_URL.rstrip("/") + "/",  # ensure trailing slash
        headers=_headers,
        timeout=30.0,  # longer timeout for remote connection
    ),
)

# Model: default gemini-1.5-flash (good availability; override with ADK_MCP_MODEL).
# Other options: gemini-2.0-flash, gemini-1.5-pro, gemini-2.5-flash
_model = os.environ.get("ADK_MCP_MODEL", "gemini-2.0-flash")
_max_tokens = int(os.environ.get("ADK_MCP_MAX_OUTPUT_TOKENS", "1024"))

root_agent = LlmAgent(
    model=_model,
    name="dbt_assistant_agent",
    description="Assistant that can query dbt models, metrics, lineage, and run SQL via the dbt MCP server.",
    instruction=(
        "You help users explore and analyze their dbt project. "
        "Use the tools from the dbt MCP server to list models, get model details, "
        "query metrics, check lineage, run SQL, and answer questions about their data. "
        "Prefer MCP tools over guessing; if a tool fails, suggest what the user should check."
    ),
    tools=[dbt_mcp_toolset],
    generate_content_config=types.GenerateContentConfig(
        max_output_tokens=_max_tokens,
        temperature=0.2,  # lower = more deterministic, can reduce load
    ),
)
