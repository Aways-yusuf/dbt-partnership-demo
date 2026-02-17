# Local setup: gcloud auth and dbt

## Problem

- **`gcloud auth application-default login`** fails in Cursor's terminal with:
  ```text
  /bin/sh: .../google-cloud-sdk/bin/gcloud: Operation not permitted
  ```
- **dbt debug** then fails with "Unable to generate access token" / "Reauthentication is needed" because no Application Default Credentials (ADC) exist.

## Fix: run gcloud outside Cursor

Use **Terminal.app** (or iTerm), not Cursor’s integrated terminal. The integrated terminal can block running binaries in `~/Downloads`.

### Step 1: Fix gcloud (in Terminal.app)

If `gcloud` gives "Operation not permitted", clear quarantine and ensure the binary is executable. On macOS, if your `xattr` does **not** support `-r`, run:

```bash
# Clear extended attributes (quarantine) on the SDK directory and everything under it
find /Users/deepthi/Downloads/google-cloud-sdk -print0 | xargs -0 xattr -c 2>/dev/null || true

# Make gcloud executable
chmod +x /Users/deepthi/Downloads/google-cloud-sdk/bin/gcloud
```

If your `xattr` **does** support recursive (e.g. `xattr -cr` works), you can use:

```bash
xattr -cr /Users/deepthi/Downloads/google-cloud-sdk
chmod +x /Users/deepthi/Downloads/google-cloud-sdk/bin/gcloud
```

### Step 2: Log in (in Terminal.app)

```bash
/Users/deepthi/Downloads/google-cloud-sdk/bin/gcloud auth application-default login
```

Complete the browser flow. This writes ADC to `~/.config/gcloud/application_default_credentials.json`.

### Step 3: Set default project (optional)

```bash
/Users/deepthi/Downloads/google-cloud-sdk/bin/gcloud config set project data-platforms-66d-demos
```

### Step 4: Use dbt (any terminal, including Cursor)

ADC are shared for your user, so dbt can run from Cursor:

```bash
cd /Users/deepthi/Feature_3/dbt-partnership-demo
source dbt-env/bin/activate
dbt debug
dbt run --select dim_customer   # or any model
```

## Alternative: install gcloud via Homebrew

If the Downloads SDK keeps causing issues, install the CLI via Homebrew so it lives in a normal path:

```bash
brew install --cask google-cloud-sdk
```

Then in Terminal.app:

```bash
gcloud auth application-default login
gcloud config set project data-platforms-66d-demos
```

After that, dbt will work from any terminal (including Cursor).
