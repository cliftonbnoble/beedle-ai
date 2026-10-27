#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
API_DIR="$ROOT_DIR/apps/api"
API_BASE_URL="${API_BASE_URL:-http://127.0.0.1:8787}"
WEB_URL="${WEB_URL:-http://localhost:5555/search}"

if [[ "${1:-}" == "--help" ]]; then
  echo 'Usage: [API_BASE_URL=http://127.0.0.1:8787] [WEB_URL=http://localhost:5555/search] bash scripts/test-search-stack.sh'
  echo 'Loopback targets only. Requires local servers and synthetic data. Writes reports, never starts servers or migrates a database.'
  exit 0
fi
if [[ "$#" -ne 0 ]]; then echo 'Unknown argument; use --help' >&2; exit 2; fi
node - "$API_BASE_URL" "$WEB_URL" <<'JS'
const [api, web] = process.argv.slice(2).map(value => new URL(value));
for (const url of [api, web]) {
  if (url.protocol !== 'http:' || !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname) || url.username || url.password || url.search || url.hash)
    throw new Error('Search-stack checks require plain HTTP loopback targets without credentials/query/hash. Use an isolated local fixture.');
}
if (api.pathname !== '/') throw new Error('API_BASE_URL must be an origin.');
JS
API_BASE_URL="${API_BASE_URL%/}"

echo
echo "== Beedle Search Stack Test =="
echo "API: $API_BASE_URL"
echo "Web: $WEB_URL"
echo

echo "-- Health check"
curl --fail --silent --show-error --max-time 15 "$API_BASE_URL/health"
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
