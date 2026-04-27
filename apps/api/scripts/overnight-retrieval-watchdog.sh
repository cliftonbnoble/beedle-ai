#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
API_DIR="$ROOT_DIR/apps/api"
API_BASE_URL="${API_BASE_URL:-http://127.0.0.1:8787}"
API_PORT="${API_PORT:-8787}"
RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
RUN_DIR="${RUN_DIR:-$API_DIR/reports/background-jobs/$RUN_ID}"
D1_DB_PATH="${D1_DB_PATH:-}"
MAX_ROUNDS="${OVERNIGHT_RETRIEVAL_MAX_ROUNDS:-5000}"
ACTIVATION_LIMIT="${OVERNIGHT_RETRIEVAL_ACTIVATION_LIMIT:-2}"
VECTOR_LIMIT="${OVERNIGHT_RETRIEVAL_VECTOR_LIMIT:-50}"
VECTOR_BATCH_SIZE="${OVERNIGHT_RETRIEVAL_VECTOR_BATCH_SIZE:-10}"
RUN_VECTOR="${OVERNIGHT_RETRIEVAL_RUN_VECTOR:-1}"
ROUND_SLEEP_SECONDS="${OVERNIGHT_RETRIEVAL_ROUND_SLEEP_SECONDS:-10}"
API_BOOT_SLEEP_SECONDS="${OVERNIGHT_RETRIEVAL_API_BOOT_SLEEP_SECONDS:-12}"
HEALTH_TIMEOUT_SECONDS="${OVERNIGHT_RETRIEVAL_HEALTH_TIMEOUT_SECONDS:-10}"

mkdir -p "$RUN_DIR"

if [ "${OVERNIGHT_RETRIEVAL_INTERNAL_LOG:-1}" = "1" ]; then
  exec > >(tee -a "$RUN_DIR/watchdog.log") 2>&1
fi

if [ -z "$D1_DB_PATH" ]; then
  D1_DB_PATH="$(
    find "$API_DIR/.wrangler/state/v3/d1/miniflare-D1DatabaseObject" -name '*.sqlite' -print 2>/dev/null \
      | grep -v '/backup-before-' \
      | head -n 1 || true
  )"
fi

if [ -z "$D1_DB_PATH" ] || [ ! -f "$D1_DB_PATH" ]; then
  echo "Unable to resolve D1_DB_PATH under $API_DIR/.wrangler/state/v3/d1/miniflare-D1DatabaseObject" >&2
  exit 1
fi

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

health_ok() {
  curl -fsS --max-time "$HEALTH_TIMEOUT_SECONDS" "$API_BASE_URL/health" >/dev/null 2>&1
}

start_api() {
  log "API health check failed; attempting restart"
  if lsof -nP -iTCP:"$API_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    log "Port $API_PORT is occupied while unhealthy; stopping wrangler/workerd before restart"
    pkill -f "wrangler dev" >/dev/null 2>&1 || true
    pkill -f "workerd" >/dev/null 2>&1 || true
    sleep 4
  fi

  (
    cd "$ROOT_DIR"
    NODE_OPTIONS=--max-old-space-size=4096 pnpm dev:api:local
  ) >> "$RUN_DIR/api.log" 2>&1 &
  echo "$!" > "$RUN_DIR/api.pid"
  log "Started API restart candidate pid=$(cat "$RUN_DIR/api.pid")"
  sleep "$API_BOOT_SLEEP_SECONDS"
}

ensure_api() {
  if health_ok; then
    return 0
  fi
  start_api
  if health_ok; then
    log "API is healthy after restart"
    return 0
  fi
  log "API is still unhealthy after restart attempt"
  return 1
}

snapshot() {
  sqlite3 -json "$D1_DB_PATH" "
    WITH active_docs AS (
      SELECT DISTINCT document_id FROM retrieval_search_chunks WHERE active = 1
    )
    SELECT
      (SELECT COUNT(*) FROM documents d WHERE d.file_type='decision_docx' AND d.searchable_at IS NOT NULL) AS searchableDecisionDocs,
      (SELECT COUNT(*) FROM documents d JOIN active_docs a ON a.document_id=d.id WHERE d.file_type='decision_docx' AND d.rejected_at IS NULL AND d.searchable_at IS NOT NULL) AS activeRetrievalDecisionCount,
      (SELECT COUNT(*) FROM documents d LEFT JOIN active_docs a ON a.document_id=d.id WHERE d.file_type='decision_docx' AND d.rejected_at IS NULL AND d.searchable_at IS NOT NULL AND a.document_id IS NULL) AS searchableButNotActiveCount;
  "
}

cd "$API_DIR"
log "Starting overnight retrieval watchdog"
log "Run dir: $RUN_DIR"
log "DB: $D1_DB_PATH"
log "Initial snapshot: $(snapshot)"

for ROUND in $(seq 1 "$MAX_ROUNDS"); do
  log "Round $ROUND starting"
  if ! ensure_api; then
    log "Skipping round $ROUND because API is not healthy"
    sleep "$ROUND_SLEEP_SECONDS"
    continue
  fi

  if ! API_BASE_URL="$API_BASE_URL" \
    D1_DB_PATH="$D1_DB_PATH" \
    SEARCHABLE_RETRIEVAL_ACTIVATION_LIMIT="$ACTIVATION_LIMIT" \
    SEARCHABLE_RETRIEVAL_ACTIVATION_OFFSET=0 \
    SEARCHABLE_RETRIEVAL_ACTIVATION_ORDER=decision_like_searchable_asc \
    SEARCHABLE_RETRIEVAL_ACTIVATION_PERFORM_VECTOR_UPSERT=0 \
    SEARCHABLE_RETRIEVAL_ACTIVATION_OUTPUT_DIR="$RUN_DIR/activation-round-$ROUND" \
    node ./scripts/searchable-retrieval-activation-batch.mjs >> "$RUN_DIR/activation.log" 2>&1; then
    log "Activation round $ROUND failed; will retry after sleep"
    sleep "$ROUND_SLEEP_SECONDS"
    continue
  fi

  if [ "$RUN_VECTOR" != "1" ]; then
    log "Skipping vector round $ROUND because OVERNIGHT_RETRIEVAL_RUN_VECTOR=$RUN_VECTOR"
    CURRENT_SNAPSHOT="$(snapshot)"
    log "Round $ROUND complete: $CURRENT_SNAPSHOT"
    if printf '%s' "$CURRENT_SNAPSHOT" | grep -q '"searchableButNotActiveCount":0'; then
      log "All searchable docs are active. Stopping."
      break
    fi
    sleep "$ROUND_SLEEP_SECONDS"
    continue
  fi

  if ! ensure_api; then
    log "Skipping vector round $ROUND because API is not healthy"
    sleep "$ROUND_SLEEP_SECONDS"
    continue
  fi

  TS="$(date +%Y%m%d-%H%M%S)"
  if ! API_BASE_URL="$API_BASE_URL" \
    RETRIEVAL_VECTOR_BACKFILL_LIMIT="$VECTOR_LIMIT" \
    RETRIEVAL_VECTOR_BACKFILL_BATCH_SIZE="$VECTOR_BATCH_SIZE" \
    RETRIEVAL_VECTOR_BACKFILL_REPORT_NAME="retrieval-vector-backfill-watchdog-${ROUND}-${TS}.json" \
    RETRIEVAL_VECTOR_BACKFILL_MARKDOWN_NAME="retrieval-vector-backfill-watchdog-${ROUND}-${TS}.md" \
    node ./scripts/retrieval-vector-backfill.mjs >> "$RUN_DIR/vector.log" 2>&1; then
    log "Vector round $ROUND failed; continuing"
  fi

  CURRENT_SNAPSHOT="$(snapshot)"
  log "Round $ROUND complete: $CURRENT_SNAPSHOT"
  if printf '%s' "$CURRENT_SNAPSHOT" | grep -q '"searchableButNotActiveCount":0'; then
    log "All searchable docs are active. Stopping."
    break
  fi
  sleep "$ROUND_SLEEP_SECONDS"
done

log "Overnight retrieval watchdog finished"
