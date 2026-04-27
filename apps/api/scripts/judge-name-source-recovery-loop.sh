#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
API_BASE_URL="${API_BASE_URL:-http://127.0.0.1:8787}"
JUDGE_SOURCE_RECOVERY_LIMIT="${JUDGE_SOURCE_RECOVERY_LIMIT:-250}"
JUDGE_SOURCE_RECOVERY_MAX_ROUNDS="${JUDGE_SOURCE_RECOVERY_MAX_ROUNDS:-20}"
D1_DB_PATH="${D1_DB_PATH:-}"

cd "$ROOT_DIR"

if [ -z "$D1_DB_PATH" ]; then
  D1_DB_PATH="$(find "$ROOT_DIR/.wrangler/state/v3/d1/miniflare-D1DatabaseObject" -name '*.sqlite' -print 2>/dev/null | head -n 1 || true)"
fi

if [ -z "$D1_DB_PATH" ] || [ ! -f "$D1_DB_PATH" ]; then
  echo "Unable to resolve D1_DB_PATH under $ROOT_DIR/.wrangler/state/v3/d1/miniflare-D1DatabaseObject" >&2
  exit 1
fi

for ROUND in $(seq 1 "$JUDGE_SOURCE_RECOVERY_MAX_ROUNDS"); do
  echo
  echo "== Judge Source Recovery Round $ROUND =="
  curl -fsS "$API_BASE_URL/health" > /dev/null

  API_BASE_URL="$API_BASE_URL" \
  D1_DB_PATH="$D1_DB_PATH" \
  JUDGE_SOURCE_RECOVERY_LIMIT="$JUDGE_SOURCE_RECOVERY_LIMIT" \
  pnpm write:judge-source-recovery

  CANDIDATES=$(node -e "const fs=require('fs'); const j=JSON.parse(fs.readFileSync('reports/judge-name-source-recovery-report.json','utf8')); console.log(j.summary.candidateRepairCount || 0);")
  FAILURES=$(node -e "const fs=require('fs'); const j=JSON.parse(fs.readFileSync('reports/judge-name-source-recovery-report.json','utf8')); console.log(j.summary.fetchFailureCount || 0);")

  echo "Candidate repairs this round: $CANDIDATES"
  echo "Fetch failures this round: $FAILURES"

  if [ "$CANDIDATES" = "0" ]; then
    echo "No more candidate repairs in the current batch window. Stopping."
    break
  fi
done
