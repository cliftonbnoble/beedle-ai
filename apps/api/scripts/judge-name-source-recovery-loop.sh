#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/Users/cliftonnoble/Documents/Beedle AI App/apps/api"
API_BASE_URL="${API_BASE_URL:-http://127.0.0.1:8787}"
JUDGE_SOURCE_RECOVERY_LIMIT="${JUDGE_SOURCE_RECOVERY_LIMIT:-250}"
JUDGE_SOURCE_RECOVERY_MAX_ROUNDS="${JUDGE_SOURCE_RECOVERY_MAX_ROUNDS:-20}"

cd "$ROOT_DIR"

for ROUND in $(seq 1 "$JUDGE_SOURCE_RECOVERY_MAX_ROUNDS"); do
  echo
  echo "== Judge Source Recovery Round $ROUND =="
  curl -fsS "$API_BASE_URL/health" > /dev/null

  API_BASE_URL="$API_BASE_URL" \
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
