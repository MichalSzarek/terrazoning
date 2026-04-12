#!/bin/zsh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
MATHS_IAC_DIR="${MATHS_IAC_DIR:-/Users/michalszarek/worksapace/maths-iac}"
DATA_STACK_DIR="$MATHS_IAC_DIR/environments/dev/data"

PROXY_HOST="${CLOUDSQL_PROXY_HOST:-127.0.0.1}"
PROXY_PORT="${CLOUDSQL_PROXY_PORT:-6543}"
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

if [[ ! -d "$DATA_STACK_DIR" ]]; then
  echo "Missing data stack dir: $DATA_STACK_DIR" >&2
  exit 1
fi

OUTPUT_JSON="$(cd "$DATA_STACK_DIR" && terragrunt output -json)"
export OUTPUT_JSON

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

export DATABASE_URL="postgresql://${DB_USER}:${DB_PASSWORD}@${PROXY_HOST}:${PROXY_PORT}/${DB_NAME}"

PROXY_PID=""

cleanup() {
  if [[ -n "$PROXY_PID" ]] && kill -0 "$PROXY_PID" >/dev/null 2>&1; then
    kill "$PROXY_PID" >/dev/null 2>&1 || true
    wait "$PROXY_PID" 2>/dev/null || true
  fi
}

trap cleanup EXIT INT TERM

if ! nc -z "$PROXY_HOST" "$PROXY_PORT" >/dev/null 2>&1; then
  cloud-sql-proxy \
    --address "$PROXY_HOST" \
    --port "$PROXY_PORT" \
    "$INSTANCE_CONNECTION_NAME" >/tmp/terrazoning-cloudsql-proxy.log 2>&1 &
  PROXY_PID=$!
  sleep 3
fi

echo "Cloud SQL target: $INSTANCE_CONNECTION_NAME"
echo "Database URL host: ${PROXY_HOST}:${PROXY_PORT}/${DB_NAME}"

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
  serve)
    uv run uvicorn app.main:app --reload --host "$APP_HOST" --port "$APP_PORT"
    ;;
  *)
    echo "Usage: $0 [serve|health]" >&2
    exit 1
    ;;
esac
