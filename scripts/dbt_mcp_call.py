#!/usr/bin/env python3
"""
Call dbt remote MCP server and print formatted output.

Usage:
  # By tool name and arguments (uses env vars or ~/.cursor/mcp.json):
  python dbt_mcp_call.py get_all_models
  python dbt_mcp_call.py get_model_details '{"name": "my_model"}'
  python dbt_mcp_call.py list_metrics '{"search": "revenue"}'
  python dbt_mcp_call.py execute_sql '{"sql": "SELECT 1 AS num"}'

  # From a curl command (parses -d body, uses configured URL/headers):
  python dbt_mcp_call.py --curl 'curl -s -X POST "https://..." -H "..." -d '"'"'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_all_models","arguments":{}}}'"'"''
  echo 'curl ...' | python dbt_mcp_call.py --curl

Config: Set DBT_MCP_URL, DBT_MCP_AUTH, DBT_PROD_ENV_ID, DBT_USER_ID, DBT_DEV_ENV_ID,
  or the script will try to load from ~/.cursor/mcp.json (dbt-mcp-remote).

Environment variables when calling:
  # Inline (only for this one command):
  DBT_MCP_URL="https://..." DBT_MCP_AUTH="Bearer ..." DBT_PROD_ENV_ID="..." DBT_USER_ID="..." DBT_DEV_ENV_ID="..." python dbt_mcp_call.py get_all_models

  # Or export first in the same shell, then run:
  export DBT_MCP_URL="https://..." DBT_MCP_AUTH="Bearer ..." DBT_PROD_ENV_ID="..." DBT_USER_ID="..." DBT_DEV_ENV_ID="..."
  python dbt_mcp_call.py get_all_models
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("Install requests: pip install requests", file=sys.stderr)
    sys.exit(1)


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

def load_config():
    """Load URL and headers from env or ~/.cursor/mcp.json."""
    url = os.environ.get("DBT_MCP_URL")
    headers = {}
    if os.environ.get("DBT_MCP_AUTH"):
        headers["Authorization"] = os.environ.get("DBT_MCP_AUTH")
    if os.environ.get("DBT_PROD_ENV_ID"):
        headers["x-dbt-prod-environment-id"] = os.environ.get("DBT_PROD_ENV_ID")
    if os.environ.get("DBT_USER_ID"):
        headers["x-dbt-user-id"] = os.environ.get("DBT_USER_ID")
    if os.environ.get("DBT_DEV_ENV_ID"):
        headers["x-dbt-dev-environment-id"] = os.environ.get("DBT_DEV_ENV_ID")

    if url and headers:
        return url.strip("/") + "/", headers

    mcp_path = Path.home() / ".cursor" / "mcp.json"
    if not mcp_path.exists():
        return None, None

    try:
        with open(mcp_path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None, None

    servers = data.get("mcpServers") or {}
    remote = servers.get("dbt-mcp-remote") or {}
    if not remote:
        return None, None

    url = remote.get("url")
    headers = dict(remote.get("headers") or {})
    if url and headers:
        return url.strip("/") + "/" if not url.endswith("/") else url, headers
    return None, None


# -----------------------------------------------------------------------------
# Parse curl command
# -----------------------------------------------------------------------------

def parse_curl_body(curl_str: str) -> dict | None:
    """Extract JSON body from a curl command (-d or --data)."""
    # Match -d '...' or -d "..." or --data '...' (single line or multi-line)
    for pattern in [
        r"-d\s+'([^']*(?:\\'[^']*)*)'",
        r'-d\s+"([^"]*(?:\\."[^"]*)*)"',
        r"--data\s+'([^']*(?:\\'[^']*)*)'",
        r'--data\s+"([^"]*(?:\\."[^"]*)*)"',
        r"-d\s+(\{[^}]+\})",  # -d {...} unquoted
    ]:
        m = re.search(pattern, curl_str, re.DOTALL)
        if m:
            raw = m.group(1).replace("\\'", "'").replace('\\"', '"')
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                pass
    return None


# -----------------------------------------------------------------------------
# MCP request/response
# -----------------------------------------------------------------------------

def call_mcp(url: str, headers: dict, body: dict) -> dict | str:
    """POST JSON-RPC body to MCP endpoint; return parsed JSON or raw string."""
    req_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        **headers,
    }
    try:
        r = requests.post(url, json=body, headers=req_headers, timeout=60)
        text = r.text
    except requests.RequestException as e:
        return {"_error": str(e)}

    if r.status_code != 200:
        return {"_error": f"HTTP {r.status_code}", "_body": text}

    # Handle Server-Sent Events (SSE): "data: {...}\n" or "data: {...}\n\n"
    text = _strip_sse(text)

    if not text.strip().startswith("{"):
        return {"_error": "Response is not JSON (e.g. 401 HTML)", "_body": text[:500]}

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"_error": "Invalid JSON response", "_body": text[:500]}


def _strip_sse(text: str) -> str:
    """If response is SSE (lines starting with 'data: '), extract JSON payload."""
    stripped = text.strip()
    if not stripped.startswith("data:") and "\ndata:" not in stripped:
        return text
    lines = text.split("\n")
    payload_lines = []
    for line in lines:
        if line.startswith("data: "):
            payload_lines.append(line[6:])
        elif line.startswith("data:"):
            payload_lines.append(line[5:].lstrip())
        elif payload_lines and line.strip() != "":
            payload_lines.append(line)
    return "\n".join(payload_lines)


def format_response(data: dict) -> str:
    """Turn MCP JSON-RPC response into nice formatted output."""
    if isinstance(data, dict) and data.get("_error"):
        err = data["_error"]
        body = data.get("_body", "")
        return f"Error: {err}\n\n{body}" if body else f"Error: {err}"

    # JSON-RPC error
    if "error" in data:
        err = data["error"]
        msg = err.get("message", "Unknown error")
        code = err.get("code", "")
        return f"JSON-RPC Error ({code}): {msg}"

    # Result: MCP tools/call returns result.content[].text
    result = data.get("result")
    if result is None:
        return json.dumps(data, indent=2)

    content = result.get("content")
    if isinstance(content, list):
        parsed = []
        for block in content:
            if isinstance(block, dict):
                text = block.get("text")
                if text is not None:
                    text = text.strip()
                    if (
                        text.startswith("{")
                        and text.endswith("}")
                    ) or (
                        text.startswith("[")
                        and text.endswith("]")
                    ):
                        try:
                            parsed.append(json.loads(text))
                        except json.JSONDecodeError:
                            parsed.append(text)
                    else:
                        parsed.append(text)
        if parsed:
            # If all items are dicts (e.g. list of models), output as one formatted array
            if all(isinstance(p, dict) for p in parsed):
                return json.dumps(parsed, indent=2)
            return "\n\n".join(
                json.dumps(p, indent=2) if isinstance(p, dict) else str(p)
                for p in parsed
            )

    # tools/list returns result.tools
    if "tools" in result:
        tools = result["tools"]
        lines = ["Available tools:", ""]
        for t in tools:
            name = t.get("name", "?")
            desc = (t.get("description") or "").strip()
            lines.append(f"  • {name}")
            if desc:
                lines.append(f"    {desc}")
        return "\n".join(lines)

    return json.dumps(result, indent=2)


def _format_text(text: str) -> str:
    """Pretty-print text; if it's JSON, indent it."""
    text = text.strip()
    if not text:
        return text
    if (text.startswith("{") and text.endswith("}")) or (
        text.startswith("[") and text.endswith("]")
    ):
        try:
            obj = json.loads(text)
            return json.dumps(obj, indent=2)
        except json.JSONDecodeError:
            pass
    return text


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Call dbt remote MCP and print formatted output.",
        epilog=__doc__,
    )
    parser.add_argument(
        "tool",
        nargs="?",
        default=None,
        help="Tool name (e.g. get_all_models, get_model_details, list_metrics)",
    )
    parser.add_argument(
        "arguments",
        nargs="?",
        default="{}",
        help="JSON object of arguments (default: {})",
    )
    parser.add_argument(
        "--curl",
        metavar="CMD",
        nargs="?",
        const="<stdin>",
        default=None,
        help="Parse curl command from arg or stdin and use its -d body",
    )
    parser.add_argument(
        "--list-tools",
        action="store_true",
        help="Call tools/list and list available tools",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print raw JSON response, no formatting",
    )
    args = parser.parse_args()

    url, headers = load_config()
    if not url or not headers:
        print(
            "Config missing. Set DBT_MCP_URL, DBT_MCP_AUTH, DBT_PROD_ENV_ID, "
            "DBT_USER_ID, DBT_DEV_ENV_ID, or use ~/.cursor/mcp.json with dbt-mcp-remote.",
            file=sys.stderr,
        )
        sys.exit(1)

    body = None

    if args.curl is not None:
        curl_src = sys.stdin.read() if args.curl == "<stdin>" else args.curl
        body = parse_curl_body(curl_src)
        if not body:
            print("Could not parse JSON body from curl command.", file=sys.stderr)
            sys.exit(1)
    elif args.list_tools:
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list",
        }
    elif args.tool:
        try:
            arguments = json.loads(args.arguments)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON arguments: {e}", file=sys.stderr)
            sys.exit(1)
        body = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": args.tool, "arguments": arguments},
        }
    else:
        parser.print_help()
        sys.exit(0)

    response = call_mcp(url, headers, body)
    if isinstance(response, dict) and response.get("_error") and not args.raw:
        print(format_response(response))
        sys.exit(1)

    if args.raw:
        if isinstance(response, dict):
            print(json.dumps(response, indent=2))
        else:
            print(response)
    else:
        print(format_response(response))


if __name__ == "__main__":
    main()
