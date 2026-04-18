#!/usr/bin/env bash
set -euo pipefail

project_id="${GCP_PROJECT_ID:-maths-489717}"
region="${GCP_REGION:-europe-west1}"
api_service="${TERRAZONING_API_SERVICE:-terrazoning-api}"
backend_port="${BACKEND_PORT:-8001}"
frontend_host="${FRONTEND_HOST:-0.0.0.0}"
frontend_port="${FRONTEND_PORT:-5174}"

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

gcloud config set project "${project_id}" >/dev/null

cleanup() {
  jobs -pr | xargs -r kill >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "Proxying TerraZoning API from Cloud Run on http://127.0.0.1:${backend_port}"
gcloud run services proxy "${api_service}" --project "${project_id}" --region "${region}" --port "${backend_port}" &

echo "Starting local Vite frontend on http://127.0.0.1:${frontend_port} with API proxy target http://127.0.0.1:${backend_port}"
cd "${repo_root}/frontend"
VITE_API_PROXY_TARGET="http://127.0.0.1:${backend_port}" npm run dev -- --host "${frontend_host}" --port "${frontend_port}"
