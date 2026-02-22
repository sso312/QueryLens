#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEXT_SQL_PORT_DEFAULT=8002
VIS_PORT_DEFAULT=8080

if [ -f "$ROOT_DIR/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$ROOT_DIR/.env"
  set +a
fi

TEXT_SQL_PORT="${TEXT_SQL_HOST_PORT:-$TEXT_SQL_PORT_DEFAULT}"
VIS_PORT="${VIS_API_HOST_PORT:-$VIS_PORT_DEFAULT}"

QVIS_PY="$ROOT_DIR/backend/query-visualization/.venv/bin/python"
T2S_PY="$ROOT_DIR/backend/text-to-sql/backend/.venv/bin/python"

QVIS_LOG="$ROOT_DIR/var/logs/smoke_query_visualization.log"
T2S_LOG="$ROOT_DIR/var/logs/smoke_text_to_sql.log"

mkdir -p "$ROOT_DIR/var/logs"

log() {
  printf '[smoke] %s\n' "$*"
}

require_file() {
  local path="$1"
  if [ ! -f "$path" ]; then
    printf 'error: required file not found: %s\n' "$path" >&2
    exit 1
  fi
}

wait_for_http() {
  local url="$1"
  local timeout_sec="${2:-45}"
  local i=0
  while [ "$i" -lt "$timeout_sec" ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

cleanup() {
  local exit_code="$1"
  if [ -n "${QVIS_PID:-}" ] && kill -0 "$QVIS_PID" >/dev/null 2>&1; then
    kill "$QVIS_PID" >/dev/null 2>&1 || true
    wait "$QVIS_PID" 2>/dev/null || true
  fi
  if [ -n "${T2S_PID:-}" ] && kill -0 "$T2S_PID" >/dev/null 2>&1; then
    kill "$T2S_PID" >/dev/null 2>&1 || true
    wait "$T2S_PID" 2>/dev/null || true
  fi
  if [ "$exit_code" -ne 0 ]; then
    printf '\n[smoke] text-to-sql log tail\n' >&2
    tail -n 80 "$T2S_LOG" >&2 || true
    printf '\n[smoke] query-visualization log tail\n' >&2
    tail -n 80 "$QVIS_LOG" >&2 || true
  fi
}

trap 'cleanup $?' EXIT

require_file "$QVIS_PY"
require_file "$T2S_PY"

log "starting text-to-sql API on port $TEXT_SQL_PORT"
(
  set -a
  if [ -f "$ROOT_DIR/backend/text-to-sql/.env" ]; then
    # shellcheck disable=SC1091
    . "$ROOT_DIR/backend/text-to-sql/.env"
  fi
  set +a
  cd "$ROOT_DIR/backend/text-to-sql/backend"
  exec "$T2S_PY" -m uvicorn app.main:app --host 127.0.0.1 --port "$TEXT_SQL_PORT"
) >"$T2S_LOG" 2>&1 &
T2S_PID=$!

if ! wait_for_http "http://127.0.0.1:$TEXT_SQL_PORT/health" 45; then
  printf 'error: text-to-sql health check failed\n' >&2
  exit 1
fi
log "text-to-sql health check passed"

log "starting query-visualization API on port $VIS_PORT"
(
  set -a
  if [ -f "$ROOT_DIR/backend/query-visualization/.env" ]; then
    # shellcheck disable=SC1091
    . "$ROOT_DIR/backend/query-visualization/.env"
  fi
  set +a
  cd "$ROOT_DIR/backend/query-visualization"
  exec "$QVIS_PY" -m uvicorn src.api.server:app --host 127.0.0.1 --port "$VIS_PORT"
) >"$QVIS_LOG" 2>&1 &
QVIS_PID=$!

if ! wait_for_http "http://127.0.0.1:$VIS_PORT/health" 45; then
  printf 'error: query-visualization health check failed\n' >&2
  exit 1
fi
log "query-visualization health check passed"

payload='{
  "user_query": "성별 평균 입원일수를 비교해줘",
  "sql": "SELECT gender, avg_los FROM sample",
  "rows": [
    {"gender": "M", "avg_los": 5.2},
    {"gender": "F", "avg_los": 4.7},
    {"gender": "M", "avg_los": 6.1},
    {"gender": "F", "avg_los": 4.3}
  ]
}'

log "running visualization smoke request"
response="$(curl -fsS "http://127.0.0.1:$VIS_PORT/visualize" \
  -H 'Content-Type: application/json' \
  -d "$payload")"

printf '%s' "$response" | "$QVIS_PY" -c '
import json, sys
obj = json.load(sys.stdin)
analyses = obj.get("analyses") or []
if not analyses:
    raise SystemExit("no analyses returned")
first = analyses[0]
if first.get("figure_json") is None:
    raise SystemExit("first analysis has no figure_json")
chart_type = (first.get("chart_spec") or {}).get("chart_type")
print(f"[smoke] analyses={len(analyses)} chart_type={chart_type}")
'

log "all smoke tests passed"
