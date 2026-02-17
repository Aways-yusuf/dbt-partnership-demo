#!/usr/bin/env bash
# Inspect target/run_results.json after a dbt run/build/test.
# Usage: ./scripts/check_run_results.sh [summary|failures]
# Requires: jq

set -e
RESULTS="${DBT_RUN_RESULTS:-target/run_results.json}"
MODE="${1:-summary}"
if [[ ! -f "$RESULTS" ]]; then
  echo "Run results not found: $RESULTS. Run dbt build/run/test first." >&2
  exit 1
fi

case "$MODE" in
  summary)
    jq -r '.results[] | "\(.status)\t\(.unique_id)\t\(.execution_time)s"' "$RESULTS" | column -t -s $'\t'
    ;;
  failures)
    jq '.results[] | select(.status != "success" and .status != "skipped")' "$RESULTS"
    ;;
  *)
    echo "Usage: $0 [summary|failures]" >&2
    echo "  summary (default): status, unique_id, execution_time per node" >&2
    echo "  failures: only non-success, non-skipped results" >&2
    exit 1
    ;;
esac
