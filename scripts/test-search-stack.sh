#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/Users/cliftonnoble/Documents/Beedle AI App"
API_DIR="$ROOT_DIR/apps/api"
WEB_DIR="$ROOT_DIR/apps/web"
API_BASE_URL="${API_BASE_URL:-http://127.0.0.1:8787}"
WEB_URL="${WEB_URL:-http://localhost:5555/search}"

echo
echo "== Beedle Search Stack Test =="
echo "API: $API_BASE_URL"
echo "Web: $WEB_URL"
echo

echo "-- Health check"
curl -sS "$API_BASE_URL/health"
echo
echo

echo "-- Index code targeted verification"
(
  cd "$API_DIR"
  API_BASE_URL="$API_BASE_URL" pnpm report:index-code-targeted-verification
)
echo

echo "-- Judge name search report"
(
  cd "$API_DIR"
  API_BASE_URL="$API_BASE_URL" pnpm report:judge-name-search
)
echo

echo "-- Judge search smoke test"
(
  cd "$API_DIR"
  API_BASE_URL="$API_BASE_URL" pnpm report:judge-search-smoke
)
echo

echo "== Manual front-end checks =="
echo "1. Open $WEB_URL"
echo "2. Search: rent reduction"
echo "3. Add judge filter: Erin E. Katayama"
echo "4. Add an index code filter if needed"
echo "5. Confirm top 12 ranked decisions load"
echo "6. Click 'Load next 12 decisions' and confirm the next page loads cleanly"
echo "7. Open a decision and verify:"
echo "   - full decision text renders"
echo "   - matched chunk controls expand/collapse"
echo "   - jumping to a chunk does not hide the top of the chunk"
echo "8. Repeat with a judge-only search and an index-code-filtered search"
echo
echo "Reports written under:"
echo "  $API_DIR/reports"
