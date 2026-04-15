#!/bin/zsh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
MATHS_IAC_DIR="${MATHS_IAC_DIR:-/Users/michalszarek/worksapace/maths-iac}"
DATA_STACK_DIR="$MATHS_IAC_DIR/environments/dev/data"
CLOUDSQL_CACHE_DIR="${TMPDIR:-/tmp}/terrazoning-cloudsql"
OUTPUT_CACHE_FILE="$CLOUDSQL_CACHE_DIR/terragrunt-output.json"
OUTPUT_LOCK_DIR="$CLOUDSQL_CACHE_DIR/terragrunt-output.lock"

PROXY_HOST="${CLOUDSQL_PROXY_HOST:-127.0.0.1}"
REQUESTED_PROXY_PORT="${CLOUDSQL_PROXY_PORT:-}"
APP_HOST="${APP_HOST:-0.0.0.0}"
APP_PORT="${APP_PORT:-8000}"
MODE="${1:-serve}"

require_bin() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required binary: $1" >&2
    exit 1
  fi
}

require_bin cloud-sql-proxy
require_bin python3
require_bin uv
require_bin terragrunt
require_bin tofu

if [[ ! -d "$DATA_STACK_DIR" ]]; then
  echo "Missing data stack dir: $DATA_STACK_DIR" >&2
  exit 1
fi

mkdir -p "$CLOUDSQL_CACHE_DIR"
chmod 700 "$CLOUDSQL_CACHE_DIR"

terragrunt_output_json() {
  cd "$DATA_STACK_DIR"
  terragrunt output -json
}

release_output_lock() {
  rm -rf "$OUTPUT_LOCK_DIR"
}

load_output_json() {
  if [[ "${CLOUDSQL_REFRESH_OUTPUT:-0}" != "1" ]] && [[ -s "$OUTPUT_CACHE_FILE" ]]; then
    cat "$OUTPUT_CACHE_FILE"
    return 0
  fi

  while ! mkdir "$OUTPUT_LOCK_DIR" 2>/dev/null; do
    sleep 0.2
  done

  if [[ "${CLOUDSQL_REFRESH_OUTPUT:-0}" != "1" ]] && [[ -s "$OUTPUT_CACHE_FILE" ]]; then
    release_output_lock
    cat "$OUTPUT_CACHE_FILE"
    return 0
  fi

  local output_json
  if ! output_json="$(terragrunt_output_json 2>/tmp/terrazoning-terragrunt-output.log)"; then
    if rg -q "Required plugins are not installed|tofu init" /tmp/terrazoning-terragrunt-output.log 2>/dev/null; then
      echo "Terragrunt/OpenTofu plugins missing for $DATA_STACK_DIR — running init..."
      (
        cd "$DATA_STACK_DIR"
        terragrunt init -upgrade
      )
      output_json="$(terragrunt_output_json)"
    elif [[ -s "$OUTPUT_CACHE_FILE" ]]; then
      release_output_lock
      cat "$OUTPUT_CACHE_FILE"
      return 0
    else
      release_output_lock
      cat /tmp/terrazoning-terragrunt-output.log >&2 || true
      exit 1
    fi
  fi

  printf '%s' "$output_json" > "${OUTPUT_CACHE_FILE}.tmp"
  chmod 600 "${OUTPUT_CACHE_FILE}.tmp"
  mv "${OUTPUT_CACHE_FILE}.tmp" "$OUTPUT_CACHE_FILE"
  release_output_lock
  printf '%s' "$output_json"
}

OUTPUT_JSON="$(load_output_json)"
export OUTPUT_JSON

choose_free_port() {
  python3 - <<'PY'
import socket

with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

INSTANCE_CONNECTION_NAME="$(python3 - <<'PY'
import json, os
payload = json.loads(os.environ["OUTPUT_JSON"])
print(payload["cloudsql_instance_connection_name"]["value"])
PY
)"

DB_PASSWORD="$(python3 - <<'PY'
import json, os
payload = json.loads(os.environ["OUTPUT_JSON"])
print(payload["cloudsql_db_password"]["value"])
PY
)"

DB_USER="${CLOUDSQL_DB_USER:-admin}"
DB_NAME="${CLOUDSQL_DB_NAME:-terrazoning}"

if [[ -n "$REQUESTED_PROXY_PORT" ]]; then
  PROXY_PORT="$REQUESTED_PROXY_PORT"
  REUSE_EXISTING_PROXY=true
else
  # Each invocation gets its own proxy port so concurrent campaign runs and
  # short-lived helper commands cannot tear down each other's Cloud SQL proxy.
  PROXY_PORT="$(choose_free_port)"
  REUSE_EXISTING_PROXY=false
fi

export DATABASE_URL="postgresql://${DB_USER}:${DB_PASSWORD}@${PROXY_HOST}:${PROXY_PORT}/${DB_NAME}"

PROXY_PID=""
PROXY_LOG_FILE="$(mktemp -t terrazoning-cloudsql-proxy.XXXXXX.log)"

cleanup() {
  release_output_lock
  if [[ -n "$PROXY_PID" ]] && kill -0 "$PROXY_PID" >/dev/null 2>&1; then
    kill "$PROXY_PID" >/dev/null 2>&1 || true
    wait "$PROXY_PID" 2>/dev/null || true
  fi
}

trap cleanup EXIT INT TERM

if [[ "$REUSE_EXISTING_PROXY" == true ]] && nc -z "$PROXY_HOST" "$PROXY_PORT" >/dev/null 2>&1; then
  :
else
  cloud-sql-proxy \
    --address "$PROXY_HOST" \
    --port "$PROXY_PORT" \
    "$INSTANCE_CONNECTION_NAME" >"$PROXY_LOG_FILE" 2>&1 &
  PROXY_PID=$!
  sleep 3
fi

echo "Cloud SQL target: $INSTANCE_CONNECTION_NAME"
echo "Database URL host: ${PROXY_HOST}:${PROXY_PORT}/${DB_NAME}"
if [[ -n "$PROXY_PID" ]]; then
  echo "Proxy log: $PROXY_LOG_FILE"
fi

cd "$BACKEND_DIR"

case "$MODE" in
  health)
    uv run python - <<'PY'
import asyncio
from sqlalchemy import text
from app.core.database import AsyncSessionLocal

async def main() -> None:
    async with AsyncSessionLocal() as session:
        await session.execute(text("SELECT 1"))
    print("health check ok")

asyncio.run(main())
PY
    ;;
  exec)
    shift
    TARGET_SUBDIR="${1:-}"
    if [[ -z "$TARGET_SUBDIR" ]]; then
      echo "Usage: $0 exec <subdir> -- <command...>" >&2
      exit 1
    fi
    shift
    if [[ "${1:-}" == "--" ]]; then
      shift
    fi
    if [[ $# -eq 0 ]]; then
      echo "Usage: $0 exec <subdir> -- <command...>" >&2
      exit 1
    fi
    TARGET_DIR="$ROOT_DIR/$TARGET_SUBDIR"
    if [[ ! -d "$TARGET_DIR" ]]; then
      echo "Missing target directory: $TARGET_DIR" >&2
      exit 1
    fi
    cd "$TARGET_DIR"
    "$@"
    ;;
  serve)
    uv run uvicorn app.main:app --reload --host "$APP_HOST" --port "$APP_PORT"
    ;;
  *)
    echo "Usage: $0 [serve|health|exec]" >&2
    exit 1
    ;;
esac
