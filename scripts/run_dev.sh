#!/bin/zsh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BACKEND_MODE="${BACKEND_MODE:-cloudsql}"
BACKEND_HOST="${BACKEND_HOST:-0.0.0.0}"
BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_HOST="${FRONTEND_HOST:-0.0.0.0}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"
API_PROXY_TARGET="${VITE_API_PROXY_TARGET:-http://127.0.0.1:${BACKEND_PORT}}"
BACKEND_HEALTH_URL="${BACKEND_HEALTH_URL:-http://127.0.0.1:${BACKEND_PORT}/api/v1/health}"
BACKEND_START_TIMEOUT_S="${BACKEND_START_TIMEOUT_S:-60}"

BACKEND_PID=""
FRONTEND_PID=""

cleanup() {
  for pid in "$FRONTEND_PID" "$BACKEND_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" 2>/dev/null || true
    fi
  done
}

trap cleanup EXIT INT TERM

echo "Starting TerraZoning backend on ${BACKEND_HOST}:${BACKEND_PORT}..."
typeset -i waited=0
if curl -fsS "$BACKEND_HEALTH_URL" >/dev/null 2>&1; then
  echo "Detected healthy backend already running on ${BACKEND_PORT}; reusing it."
else
  if nc -z 127.0.0.1 "$BACKEND_PORT" >/dev/null 2>&1; then
    echo "Port ${BACKEND_PORT} is already in use, but backend health check failed at ${BACKEND_HEALTH_URL}." >&2
    echo "Stop the conflicting process or choose a different BACKEND_PORT." >&2
    exit 1
  fi

  (
    cd "$ROOT_DIR"
    if [[ "$BACKEND_MODE" == "local" ]]; then
      cd backend
      uv run uvicorn app.main:app --reload --host "$BACKEND_HOST" --port "$BACKEND_PORT"
    else
      APP_HOST="$BACKEND_HOST" APP_PORT="$BACKEND_PORT" ./scripts/run_backend_cloudsql.sh serve
    fi
  ) &
  BACKEND_PID=$!

  echo "Waiting for backend readiness: ${BACKEND_HEALTH_URL}"
  until curl -fsS "$BACKEND_HEALTH_URL" >/dev/null 2>&1; do
    if ! kill -0 "$BACKEND_PID" >/dev/null 2>&1; then
      echo "Backend exited before becoming ready." >&2
      wait "$BACKEND_PID"
      exit 1
    fi
    if (( waited >= BACKEND_START_TIMEOUT_S )); then
      echo "Backend did not become ready within ${BACKEND_START_TIMEOUT_S}s." >&2
      exit 1
    fi
    sleep 1
    waited+=1
  done

  echo "Backend ready after ${waited}s."
fi

echo "Starting frontend on ${FRONTEND_HOST}:${FRONTEND_PORT}..."
(
  cd "$ROOT_DIR/frontend"
  VITE_API_PROXY_TARGET="$API_PROXY_TARGET" npm run dev -- --host "$FRONTEND_HOST" --port "$FRONTEND_PORT"
) &
FRONTEND_PID=$!

wait "$FRONTEND_PID"
