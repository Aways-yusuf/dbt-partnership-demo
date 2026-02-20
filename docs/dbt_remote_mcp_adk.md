# Beginner's Guide: ADK + dbt MCP (adk_mcp folder)

**Folder name is `adk_mcp` (underscore)** — ADK requires app names to be valid identifiers (letters, digits, underscores only; no hyphens).

This folder is an **AI agent** that talks to your **dbt** project over the internet using **MCP** (Model Context Protocol).

---

## What we built

- **Agent** = A chatbot that can use “tools” (e.g. ask dbt for models, run SQL).
- **dbt** = Tool for data transformations (SQL, models, metrics).
- **MCP** = Standard way for AI apps to connect to services like dbt and use their tools.

**Flow:** You chat → Agent (Gemini) calls your dbt MCP server → dbt does the work → Agent answers you.

---

## Files in this folder

| File | Purpose |
|------|--------|
| **agent.py** | Defines the agent (Gemini + dbt MCP tools). |
| **__init__.py** | Lets ADK find `root_agent` from `agent.py`. |
| **requirements.txt** | Lists `google-adk` so `pip install` installs the ADK and `adk` CLI. |
| **.env.example** | Template for URL and secrets. Copy to `.env` and fill in real values. |
| **.env** | Your real secrets (create from .env.example). **Do not commit to git.** |
| **cloud_run_proxy.py** | Optional: local proxy to open Cloud Run Web UI in browser when the service requires auth. |

---

## How to run (Mac)

### One-time setup (do this once)

```bash
cd /Users/gaurav/Downloads/Python/adk_mcp

# 1. Create virtual environment
python3 -m venv .venv

# 2. Activate it
source .venv/bin/activate

# 3. Install ADK (installs the `adk` command)
pip install -r requirements.txt

# 4. Configure secrets: create .env with at least:
#    DBT_MCP_URL, DBT_API_TOKEN, DBT_PROD_ENVIRONMENT_ID, GOOGLE_API_KEY
#    (Get prod environment ID from dbt Cloud: Orchestration → environment ID)
```

### Run the agent

**Option A – Terminal chat (run from inside adk_mcp):**

```bash
cd /Users/gaurav/Downloads/Python/adk_mcp
source .venv/bin/activate
adk run .
```

**Option B – Web UI (run from the PARENT folder that contains adk_mcp):**

```bash
cd /Users/gaurav/Downloads/Python
source adk_mcp/.venv/bin/activate
adk web --port 8000
```

Then open http://localhost:8000 and **select the "adk_mcp" agent** in the dropdown.

Important: `adk web` must be run from the folder that **contains** your agent folder (here: `Python`), not from inside `adk_mcp`. The agent folder name must use only letters, digits, and underscores (e.g. `adk_mcp`, not `adk-mcp`).

---

## How to use the ADK Web UI

1. **Start the server** (from the parent folder, with venv active):
   ```bash
   cd /Users/gaurav/Downloads/Python
   source adk_mcp/.venv/bin/activate
   adk web --port 8000
   ```
   Leave this terminal open while you use the UI.

2. **Open in browser:**  
   Go to **http://localhost:8000**.

3. **Pick your agent:**  
   At the top of the page there’s a dropdown (or agent selector). Choose **adk_mcp** (your dbt assistant).

4. **Chat:**  
   Type your message in the input at the bottom and press Enter (or click Send).  
   Examples:
   - “What models do I have?”
   - “List my dbt models.”
   - “Run a query for me.”

5. **Read the reply:**  
   The agent will call the dbt MCP tools when needed and show you the answer in the chat. You can keep asking follow-up questions in the same conversation.

6. **Stop the server:**  
   In the terminal where `adk web` is running, press **Ctrl+C**.

---

## How to host remotely

Right now `adk web` runs only on your machine (`localhost`). To use the same Web UI from another device or over the internet you have two main options.

### Option 1: Deploy to Google Cloud Run (recommended)

[Cloud Run](https://cloud.google.com/run) is the supported way to run ADK in production. You get a public (or private) URL and the Web UI can be included.

**Prerequisites**

- Google Cloud project with billing enabled.
- [gcloud CLI](https://cloud.google.com/sdk/docs/install) installed and logged in:  
  `gcloud auth login` and `gcloud config set project YOUR_PROJECT_ID`.

**1. Set environment variables**

```bash
export GOOGLE_CLOUD_PROJECT="data-platforms-66d-demos"
export GOOGLE_CLOUD_LOCATION="us-central1"
```

**2. Deploy the agent (with Web UI)**

Run from the **parent** folder that contains `adk_mcp` (e.g. `Python`), with the path to your agent folder:

```bash
cd /Users/gaurav/Downloads/Python
source adk_mcp/.venv/bin/activate

adk deploy cloud_run \
  --project=$GOOGLE_CLOUD_PROJECT \
  --region=$GOOGLE_CLOUD_LOCATION \
  --with_ui \
  ./adk_mcp
```

`--with_ui` deploys the ADK Web UI so you can chat in the browser at the Cloud Run URL.

**3. Pass dbt and Gemini secrets to Cloud Run**

Your agent needs `GOOGLE_API_KEY`, `DBT_MCP_URL`, `DBT_API_TOKEN`, `DBT_PROD_ENVIRONMENT_ID`, and optionally `DBT_DEV_ENVIRONMENT_ID`, `DBT_USER_ID`. You can:

- **Secrets (recommended):** Create secrets in [Secret Manager](https://console.cloud.google.com/security/secret-manager), then when deploying (or in the Cloud Run console) map them as environment variables so the container sees `GOOGLE_API_KEY`, `DBT_MCP_URL`, etc.
- **Or env vars:** Use `gcloud run services update` or the Cloud Run UI to set these env vars (avoid putting tokens in scripts).

After deployment, Cloud Run prints a URL like `https://adk-default-service-name-xxxxx.a.run.app`. Open it in a browser and select the **adk_mcp** agent to use the Web UI remotely.

**4. Authentication**

During deploy you may be asked: *Allow unauthenticated invocations?*  
- **y** – anyone with the URL can open the UI (fine for internal/testing).  
- **N** – only authenticated users (e.g. with `gcloud auth print-identity-token`) can call the service.

---

### Open Cloud Run Web UI in the browser and interact

**If you allowed unauthenticated access (answered `y` above):**

1. **Open the URL** in any browser:  
   `https://adk-default-service-name-952777511008.us-central1.run.app/`  
   (use the URL Cloud Run gave you after deploy.)
2. **Select your agent:** At the top of the page, use the dropdown and choose **adk_mcp**.
3. **Chat:** Type a message in the box at the bottom and press Enter (or click Send).  
   Examples: “What models do I have?”, “List my dbt models.”
4. **Read the reply** and keep asking follow-up questions in the same conversation.

**If the service requires authentication (you answered `N`):**

You can’t send a gcloud token from the browser directly. Use this **local proxy** so the browser talks to Cloud Run via your machine with a token:

1. **Install httpx** (if needed):  
   `pip install httpx`
2. **Set your Cloud Run URL** (edit the script or set env):  
   `export CLOUD_RUN_URL="https://adk-default-service-name-952777511008.us-central1.run.app"`
3. **Run the proxy** (from the `adk_mcp` folder):  
   `python cloud_run_proxy.py`
4. **Open in browser:** Go to **http://localhost:8080**.
5. **Select the adk_mcp agent** in the dropdown and chat as above.

The proxy adds your gcloud identity token to every request. Keep the proxy running while you use the UI. If you get “Forbidden” or “401” after a while, restart the proxy (tokens expire). Stop the proxy with **Ctrl+C**.

---

### Option 2: Run on a VPS and expose the port

For a quick remote setup without Cloud Run:

1. **Rent a small Linux server** (e.g. a VM on GCP, AWS, or any VPS).
2. **Copy your project** (e.g. `adk_mcp` and its parent so `adk web` can find the agent), and create a `.env` with the same variables as locally.
3. **Install Python, venv, and dependencies** on the server, then run:

   ```bash
   adk web --host 0.0.0.0 --port 8000
   ```

   `--host 0.0.0.0` makes the server listen on all interfaces so it’s reachable from the internet.
4. **Secure it:** Put the app behind HTTPS (e.g. nginx + Let’s Encrypt) and restrict access (firewall, VPN, or auth) so only you or your team can reach it. ADK Web is intended for development; use this only if you accept that and harden the server.

---

## "Failed to create MCP session" / "unhandled errors in a TaskGroup"

1. **Set required dbt headers**  
   dbt remote MCP needs more than the token. In `.env` set:
   - **DBT_PROD_ENVIRONMENT_ID** (required) – from dbt Cloud: **Orchestration** → your job/environment → use the production environment ID.
   - Optionally **DBT_DEV_ENVIRONMENT_ID** and **DBT_USER_ID** if you use `execute_sql` or Fusion tools.

2. **Check URL and token**  
   - `DBT_MCP_URL` must end with `/` (e.g. `https://yh400.us1.dbt.com/api/ai/v1/mcp/`).
   - Use the same token and URL that work in Cursor’s mcp.json.

3. **Known ADK issue**  
   Some ADK versions can show "unhandled errors in a TaskGroup" with Streamable HTTP MCP. If the connection still fails after setting the headers, try updating: `pip install -U google-adk`.

---

## If you get "command not found: adk"

- Make sure the venv is active (prompt shows `(.venv)`).
- Or call the CLI directly:  
  `.venv/bin/adk run .`

---

## What agent.py does (short)

- Reads **DBT_MCP_URL** and **DBT_API_TOKEN** from the environment.
- Creates **McpToolset** with **StreamableHTTPServerParams** to connect to your dbt MCP server over HTTP.
- Creates **root_agent** (LlmAgent) with Gemini and that toolset, so the agent can use dbt’s tools to answer you.

---

## Glossary

| Term | Meaning |
|------|--------|
| **ADK** | Agent Development Kit — Google’s library to build AI agents. |
| **Agent** | Program that uses an LLM and can call tools (here: dbt via MCP). |
| **MCP** | Model Context Protocol — standard for AI apps to use external tools. |
| **McpToolset** | ADK object that connects to one MCP server and exposes its tools to the agent. |
| **Streamable HTTP** | Way to talk to an MCP server over the web (used for remote dbt). |
| **root_agent** | The single agent ADK runs when you use `adk run` or `adk web`. |



```bash

Local ADK Setup

cd /Users/gaurav/Downloads/Python
source adk_mcp/.venv/bin/activate
adk web --port 8000

Local and Remote ADK Commands

get_all_models
get details about dbt sources
get_lineage of model.dbt_partnership_demo.fct_purchase
get_model_details of fct_purchase
get_all_macros
last run status of dim_city
execute the sql SELECT * FROM dbt_target.fct_purchase LIMIT 5

```