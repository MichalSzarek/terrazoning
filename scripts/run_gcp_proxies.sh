#!/usr/bin/env bash
set -euo pipefail

project_id="${GCP_PROJECT_ID:-maths-489717}"
region="${GCP_REGION:-europe-west1}"
api_service="${TERRAZONING_API_SERVICE:-terrazoning-api}"
frontend_service="${TERRAZONING_FRONTEND_SERVICE:-terrazoning-frontend}"
api_port="${BACKEND_PORT:-8000}"
frontend_port="${FRONTEND_PORT:-5173}"

gcloud config set project "${project_id}" >/dev/null

cleanup() {
  jobs -pr | xargs -r kill >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "Proxying TerraZoning API on http://localhost:${api_port}"
gcloud run services proxy "${api_service}" --project "${project_id}" --region "${region}" --port "${api_port}" &

echo "Proxying TerraZoning frontend on http://localhost:${frontend_port}"
gcloud run services proxy "${frontend_service}" --project "${project_id}" --region "${region}" --port "${frontend_port}" &

wait
