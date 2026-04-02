#!/usr/bin/env bash
set -euo pipefail

MAX_ROUNDS="${CONCLUSIONS_SEARCHABILITY_MAX_ROUNDS:-120}"
SLEEP_SECONDS="${CONCLUSIONS_SEARCHABILITY_SLEEP_SECONDS:-8}"
REPORT_JSON="${CONCLUSIONS_SEARCHABILITY_REPORT_JSON:-./reports/conclusions-searchability-report.json}"

for ROUND in $(seq 1 "$MAX_ROUNDS"); do
  echo
  echo "== Conclusions Searchability Round $ROUND =="
  CONCLUSIONS_SEARCHABILITY_APPLY=1 node ./scripts/conclusions-searchability-backfill.mjs

  CANDIDATE_DOCS="$(node -e 'const fs=require("fs"); const report=JSON.parse(fs.readFileSync(process.argv[1],"utf8")); console.log(Number(report.candidateDocCount||0));' "$REPORT_JSON")"
  APPLIED_DOCS="$(node -e 'const fs=require("fs"); const report=JSON.parse(fs.readFileSync(process.argv[1],"utf8")); console.log(Number(report.appliedDocCount||0));' "$REPORT_JSON")"
  MISSING_AFTER="$(node -e 'const fs=require("fs"); const report=JSON.parse(fs.readFileSync(process.argv[1],"utf8")); console.log(Number(report.corpusAfter?.docsMissingTrustedConclusionChunks||0));' "$REPORT_JSON")"

  echo "Candidate docs this round: $CANDIDATE_DOCS"
  echo "Applied docs this round: $APPLIED_DOCS"
  echo "Docs still missing trusted conclusion chunks: $MISSING_AFTER"

  if [ "$CANDIDATE_DOCS" -eq 0 ]; then
    echo "No more candidate conclusions docs in the current batch window. Stopping."
    break
  fi

  if [ "$ROUND" -lt "$MAX_ROUNDS" ]; then
    echo "Sleeping ${SLEEP_SECONDS}s before the next round..."
    sleep "$SLEEP_SECONDS"
  fi
done
