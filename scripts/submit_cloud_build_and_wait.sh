#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <gcloud-builds-submit-args...>" >&2
  exit 2
fi

poll_interval="${CLOUD_BUILD_POLL_INTERVAL_SECONDS:-15}"

build_json="$(gcloud builds submit --async --format=json "$@")"
build_id="$(
  printf '%s' "${build_json}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])'
)"
log_url="$(
  printf '%s' "${build_json}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("logUrl",""))'
)"

echo "Submitted Cloud Build: ${build_id}"
if [[ -n "${log_url}" ]]; then
  echo "Cloud Build logs: ${log_url}"
fi

last_status=""
while true; do
  build_status="$(
    gcloud builds describe "${build_id}" --format='value(status)'
  )"

  if [[ "${build_status}" != "${last_status}" ]]; then
    echo "Cloud Build status: ${build_status}"
    last_status="${build_status}"
  fi

  case "${build_status}" in
    SUCCESS)
      echo "Cloud Build ${build_id} completed successfully."
      exit 0
      ;;
    FAILURE|INTERNAL_ERROR|TIMEOUT|CANCELLED|EXPIRED)
      echo "Cloud Build ${build_id} finished with terminal status: ${build_status}" >&2
      if [[ -n "${log_url}" ]]; then
        echo "Inspect logs: ${log_url}" >&2
      fi
      exit 1
      ;;
    QUEUED|PENDING|WORKING)
      sleep "${poll_interval}"
      ;;
    *)
      echo "Cloud Build ${build_id} returned unexpected status: ${build_status}" >&2
      if [[ -n "${log_url}" ]]; then
        echo "Inspect logs: ${log_url}" >&2
      fi
      exit 1
      ;;
  esac
done
