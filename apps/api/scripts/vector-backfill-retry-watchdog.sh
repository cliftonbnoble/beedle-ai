#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
API_DIR="$ROOT_DIR/apps/api"
API_BASE_URL="${API_BASE_URL:-http://127.0.0.1:8787}"
API_PORT="${API_PORT:-8787}"
RUN_ID="${RUN_ID:-vector-retry-$(date +%Y%m%d-%H%M%S)}"
RUN_DIR="${RUN_DIR:-$API_DIR/reports/background-jobs/$RUN_ID}"
MAX_ROUNDS="${VECTOR_RETRY_MAX_ROUNDS:-1000}"
VECTOR_LIMIT="${VECTOR_RETRY_LIMIT:-100}"
VECTOR_BATCH_SIZE="${VECTOR_RETRY_BATCH_SIZE:-20}"
VECTOR_OFFSET_START="${VECTOR_RETRY_OFFSET_START:-0}"
VECTOR_OFFSET_STRIDE="${VECTOR_RETRY_OFFSET_STRIDE:-$VECTOR_LIMIT}"
STATE_FILE="${VECTOR_RETRY_STATE_FILE:-$RUN_DIR/vector-backfill-state.json}"
RESUME_FROM_STATE="${VECTOR_RETRY_RESUME_FROM_STATE:-1}"
INCLUDE_DOCUMENT_CHUNKS="${VECTOR_RETRY_INCLUDE_DOCUMENT_CHUNKS:-0}"
INCLUDE_TRUSTED_CHUNKS="${VECTOR_RETRY_INCLUDE_TRUSTED_CHUNKS:-1}"
SLEEP_SECONDS="${VECTOR_RETRY_SLEEP_SECONDS:-120}"
PROBE_TIMEOUT_SECONDS="${VECTOR_RETRY_PROBE_TIMEOUT_SECONDS:-30}"
HEALTH_TIMEOUT_SECONDS="${VECTOR_RETRY_HEALTH_TIMEOUT_SECONDS:-10}"
API_BOOT_SLEEP_SECONDS="${VECTOR_RETRY_API_BOOT_SLEEP_SECONDS:-15}"
MAX_CONSECUTIVE_PROBE_FAILURES="${VECTOR_RETRY_MAX_CONSECUTIVE_PROBE_FAILURES:-3}"
RESTART_API_ON_PROBE_FAILURE="${VECTOR_RETRY_RESTART_API_ON_PROBE_FAILURE:-1}"
QUOTA_SLEEP_SECONDS="${VECTOR_RETRY_QUOTA_SLEEP_SECONDS:-900}"

mkdir -p "$RUN_DIR"
cd "$API_DIR"

if [ "${VECTOR_RETRY_INTERNAL_LOG:-1}" = "1" ]; then
  exec > >(tee -a "$RUN_DIR/watchdog.log") 2>&1
fi

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

health_ok() {
  curl -fsS --max-time "$HEALTH_TIMEOUT_SECONDS" "$API_BASE_URL/health" >/dev/null 2>&1
}

restart_api() {
  log "Restarting local API/Wrangler to refresh remote AI/Vectorize bindings"
  pkill -f "pnpm dev:api:local" >/dev/null 2>&1 || true
  pkill -f "wrangler dev" >/dev/null 2>&1 || true
  pkill -f "workerd" >/dev/null 2>&1 || true
  sleep 4

  if lsof -nP -iTCP:"$API_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    log "Port $API_PORT is still occupied after stop attempt; waiting before restart"
    sleep 6
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

  restart_api
  if health_ok; then
    log "API is healthy after restart"
    return 0
  fi

  log "API is still unhealthy after restart attempt"
  return 1
}

probe_embeddings() {
  local probe_file="$RUN_DIR/vector-probe-latest.json"
  local status_file="$RUN_DIR/vector-probe-status.txt"
  local status
  status="$(
    curl -sS --max-time "$PROBE_TIMEOUT_SECONDS" -o "$probe_file" -w '%{http_code}' \
      "$API_BASE_URL/admin/retrieval/vectors/probe" \
      -H 'content-type: application/json' \
      --data '{"queryText":"ant infestation in an apartment","topK":1}' || true
  )"
  printf '%s\n' "$status" > "$status_file"
  [ "$status" = "200" ]
}

probe_failed_due_to_quota() {
  local probe_file="$RUN_DIR/vector-probe-latest.json"
  [ -f "$probe_file" ] && grep -q '4006: you have used up your daily free allocation' "$probe_file"
}

read_state_offset() {
  if [ "$RESUME_FROM_STATE" != "1" ] || [ ! -f "$STATE_FILE" ]; then
    printf '%s\n' "$VECTOR_OFFSET_START"
    return 0
  fi

  node -e "const fs=require('fs'); const p=process.argv[1]; const fallback=Number(process.argv[2] || 0); try { const s=JSON.parse(fs.readFileSync(p, 'utf8')); console.log(Number.isFinite(Number(s.nextOffset)) ? Number(s.nextOffset) : fallback); } catch { console.log(fallback); }" "$STATE_FILE" "$VECTOR_OFFSET_START"
}

write_state() {
  local status="$1"
  local round="$2"
  local current_offset="$3"
  local next_offset="$4"
  local report_name="$5"
  VECTOR_RETRY_LIMIT="$VECTOR_LIMIT" VECTOR_RETRY_BATCH_SIZE="$VECTOR_BATCH_SIZE" VECTOR_RETRY_OFFSET_STRIDE="$VECTOR_OFFSET_STRIDE" VECTOR_RETRY_INCLUDE_DOCUMENT_CHUNKS="$INCLUDE_DOCUMENT_CHUNKS" VECTOR_RETRY_INCLUDE_TRUSTED_CHUNKS="$INCLUDE_TRUSTED_CHUNKS" \
    node -e "const fs=require('fs'); const [p,status,round,currentOffset,nextOffset,reportName]=process.argv.slice(1); const payload={updatedAt:new Date().toISOString(),status,round:Number(round),currentOffset:Number(currentOffset),nextOffset:Number(nextOffset),limit:Number(process.env.VECTOR_RETRY_LIMIT || 0),batchSize:Number(process.env.VECTOR_RETRY_BATCH_SIZE || 0),offsetStride:Number(process.env.VECTOR_RETRY_OFFSET_STRIDE || process.env.VECTOR_RETRY_LIMIT || 0),reportName,includeDocumentChunks:process.env.VECTOR_RETRY_INCLUDE_DOCUMENT_CHUNKS || '0',includeTrustedChunks:process.env.VECTOR_RETRY_INCLUDE_TRUSTED_CHUNKS || '1'}; fs.writeFileSync(p, JSON.stringify(payload,null,2)+'\n');" "$STATE_FILE" "$status" "$round" "$current_offset" "$next_offset" "$report_name"
}

log "Starting vector backfill retry watchdog"
log "Run dir: $RUN_DIR"
log "Settings: limit=$VECTOR_LIMIT batchSize=$VECTOR_BATCH_SIZE offsetStart=$VECTOR_OFFSET_START offsetStride=$VECTOR_OFFSET_STRIDE stateFile=$STATE_FILE resume=$RESUME_FROM_STATE includeDocumentChunks=$INCLUDE_DOCUMENT_CHUNKS includeTrustedChunks=$INCLUDE_TRUSTED_CHUNKS sleep=${SLEEP_SECONDS}s quotaSleep=${QUOTA_SLEEP_SECONDS}s maxProbeFailures=$MAX_CONSECUTIVE_PROBE_FAILURES restartOnProbeFailure=$RESTART_API_ON_PROBE_FAILURE"

CONSECUTIVE_PROBE_FAILURES=0
CURRENT_OFFSET="$(read_state_offset)"
log "Initial vector offset: $CURRENT_OFFSET"

for ROUND in $(seq 1 "$MAX_ROUNDS"); do
  log "Vector retry round $ROUND starting"
  if ! ensure_api; then
    log "API health failed; sleeping"
    sleep "$SLEEP_SECONDS"
    continue
  fi

  if ! probe_embeddings; then
    if probe_failed_due_to_quota; then
      CONSECUTIVE_PROBE_FAILURES=0
      log "Embedding probe hit Workers AI allocation limit; sleeping ${QUOTA_SLEEP_SECONDS}s without restarting API"
      sleep "$QUOTA_SLEEP_SECONDS"
      continue
    fi

    CONSECUTIVE_PROBE_FAILURES=$((CONSECUTIVE_PROBE_FAILURES + 1))
    log "Embedding probe failed ($CONSECUTIVE_PROBE_FAILURES consecutive); sleeping before retry"
    if [ "$RESTART_API_ON_PROBE_FAILURE" = "1" ] && [ "$CONSECUTIVE_PROBE_FAILURES" -ge "$MAX_CONSECUTIVE_PROBE_FAILURES" ]; then
      restart_api
      CONSECUTIVE_PROBE_FAILURES=0
    fi
    sleep "$SLEEP_SECONDS"
    continue
  fi
  CONSECUTIVE_PROBE_FAILURES=0

  TS="$(date +%Y%m%d-%H%M%S)"
  REPORT_NAME="retrieval-vector-backfill-retry-${ROUND}-${TS}.json"
  MARKDOWN_NAME="retrieval-vector-backfill-retry-${ROUND}-${TS}.md"
  log "Embedding probe passed; running vector backfill round $ROUND at offset $CURRENT_OFFSET"
  if ! API_BASE_URL="$API_BASE_URL" \
    RETRIEVAL_VECTOR_BACKFILL_LIMIT="$VECTOR_LIMIT" \
    RETRIEVAL_VECTOR_BACKFILL_OFFSET="$CURRENT_OFFSET" \
    RETRIEVAL_VECTOR_BACKFILL_BATCH_SIZE="$VECTOR_BATCH_SIZE" \
    RETRIEVAL_VECTOR_BACKFILL_INCLUDE_DOCUMENT_CHUNKS="$INCLUDE_DOCUMENT_CHUNKS" \
    RETRIEVAL_VECTOR_BACKFILL_INCLUDE_TRUSTED_CHUNKS="$INCLUDE_TRUSTED_CHUNKS" \
    RETRIEVAL_VECTOR_BACKFILL_REPORT_NAME="$REPORT_NAME" \
    RETRIEVAL_VECTOR_BACKFILL_MARKDOWN_NAME="$MARKDOWN_NAME" \
    node ./scripts/retrieval-vector-backfill.mjs >> "$RUN_DIR/vector.log" 2>&1; then
    log "Vector backfill round $ROUND failed at offset $CURRENT_OFFSET"
    write_state "failed" "$ROUND" "$CURRENT_OFFSET" "$CURRENT_OFFSET" "$REPORT_NAME"
  else
    NEXT_OFFSET=$((CURRENT_OFFSET + VECTOR_OFFSET_STRIDE))
    write_state "ok" "$ROUND" "$CURRENT_OFFSET" "$NEXT_OFFSET" "$REPORT_NAME"
    log "Vector backfill round $ROUND complete; next offset $NEXT_OFFSET"
    CURRENT_OFFSET="$NEXT_OFFSET"
  fi

  sleep "$SLEEP_SECONDS"
done

log "Vector backfill retry watchdog finished"
