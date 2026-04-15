SHELL := /bin/zsh

PROVINCES ?= slaskie malopolskie
MAX_PAGES ?= 3
TERYT ?=
PROVINCE ?=
BATCH_SIZE ?= 100
BACKLOG_FORMAT ?= csv
BACKLOG_OUTPUT ?=
EXPECTED_STATE ?= enabled
SMOKE_LIMIT ?= 1
BACKEND_HOST ?= 0.0.0.0
BACKEND_PORT ?= 8000
FRONTEND_HOST ?= 0.0.0.0
FRONTEND_PORT ?= 5173
APP_HOST ?= $(BACKEND_HOST)
APP_PORT ?= $(BACKEND_PORT)
GCP_PROJECT_ID ?= maths-489717
GCP_REGION ?= europe-west1
ARTIFACT_REGISTRY_REPOSITORY ?= python-apps
TERRAZONING_BACKEND_IMAGE ?= terrazoning-backend
TERRAZONING_FRONTEND_IMAGE ?= terrazoning-frontend
TERRAZONING_API_SERVICE ?= terrazoning-api
TERRAZONING_FRONTEND_SERVICE ?= terrazoning-frontend
TERRAZONING_JOB_SCRAPE_LIVE ?= terrazoning-scrape-live
TERRAZONING_JOB_GEO_RESOLVE ?= terrazoning-geo-resolve
TERRAZONING_JOB_DELTA ?= terrazoning-delta
TERRAZONING_JOB_PLANNING_SIGNAL_SYNC ?= terrazoning-planning-signal-sync
TERRAZONING_JOB_FUTURE_BUILDABILITY ?= terrazoning-future-buildability
TERRAZONING_JOB_CAMPAIGN_ROLLOUT ?= terrazoning-campaign-rollout
TERRAZONING_FUTURE_BUILDABILITY_ENABLED ?= true
TERRAZONING_MAP_STYLE_URL ?=
CLOUD_BUILD_SOURCE_STAGING_DIR ?= gs://maths-cloudbuild-source-478521031206/source
CLOUD_BUILD_SERVICE_ACCOUNT ?= projects/maths-489717/serviceAccounts/478521031206-compute@developer.gserviceaccount.com
GCP_JOB_ARGS ?=
TERRAZONING_FRONTEND_API_BASE_URL ?= http://localhost:8000

.PHONY: help sync-all sync-backend sync-scraper sync-frontend scrape-dry scrape-live mpzp-sync mpzp-one \
	mpzp-uncovered mpzp-registry mpzp-ruda reparse-bronze geo-resolve delta planning-signal-sync \
	future-buildability future-buildability-status future-buildability-backlog future-buildability-smoke force-retry load-all-data \
	refresh-all gliwice-cluster status doctor sync-slaskie sync-malopolskie report-slaskie \
	report-malopolskie delta-gap-malopolskie campaign-slaskie campaign-malopolskie campaign-all campaign-rollout-cloudsql \
	run run-local run-backend run-frontend backend-dev backend-cloudsql cloudsql-health \
	gcp-deploy-backend gcp-deploy-frontend gcp-service-urls gcp-smoke-api gcp-smoke-frontend \
	gcp-auth gcp-proxy gcp-proxy-frontend gcp-proxy-api \
	gcp-job-scrape-live gcp-job-geo-resolve gcp-job-delta gcp-job-planning-signal-sync \
	gcp-job-future-buildability gcp-job-campaign-rollout

help:
	@echo "TerraZoning data operations"
	@echo ""
	@echo "  make run               - run Cloud SQL backend + frontend (dev) with backend health gating"
	@echo "  make run-local         - run local PostGIS backend + frontend (dev) with backend health gating"
	@echo "  make run-backend       - run FastAPI backend locally (BACKEND_PORT=8000)"
	@echo "  make run-frontend      - run Vite frontend (FRONTEND_PORT=5173)"
	@echo "  make gcp-deploy-backend - submit Cloud Build for TerraZoning backend + jobs image sync"
	@echo "  make gcp-deploy-frontend - submit Cloud Build for TerraZoning frontend"
	@echo "  make gcp-service-urls  - print deployed TerraZoning Cloud Run URLs"
	@echo "  make gcp-smoke-api     - smoke check deployed TerraZoning API"
	@echo "  make gcp-smoke-frontend - smoke check deployed TerraZoning frontend"
	@echo "  make gcp-auth          - gcloud auth login + select maths project"
	@echo "  make gcp-proxy         - start both TerraZoning Cloud Run proxies locally"
	@echo "  make gcp-proxy-frontend - proxy TerraZoning frontend locally via gcloud (default port 5173)"
	@echo "  make gcp-proxy-api     - proxy TerraZoning API locally via gcloud (default port 8000)"
	@echo "  make gcp-job-scrape-live - execute TerraZoning scrape-live Cloud Run job"
	@echo "  make gcp-job-geo-resolve - execute TerraZoning geo-resolve Cloud Run job"
	@echo "  make gcp-job-delta     - execute TerraZoning delta Cloud Run job"
	@echo "  make gcp-job-planning-signal-sync - execute planning signal sync Cloud Run job"
	@echo "  make gcp-job-future-buildability - execute future-buildability Cloud Run job"
	@echo "  make gcp-job-campaign-rollout - execute TerraZoning full campaign Cloud Run job"
	@echo "  make sync-all          - install/update backend, scraper, and frontend deps"
	@echo "  make sync-backend      - install/update backend deps"
	@echo "  make sync-scraper      - install/update scraper deps"
	@echo "  make sync-frontend     - install/update frontend deps"
	@echo "  make scrape-dry        - dry-run Komornik scrape (PROVINCES='slaskie malopolskie')"
	@echo "  make scrape-live       - live Komornik scrape (MAX_PAGES=3 by default)"
	@echo "  make mpzp-registry     - show configured MPZP sources"
	@echo "  make mpzp-uncovered    - show gminy without planning coverage"
	@echo "  make mpzp-sync         - sync all configured MPZP sources"
	@echo "  make mpzp-one TERYT=... - sync one configured MPZP source"
	@echo "  make sync-slaskie      - sync configured MPZP sources only for Śląskie"
	@echo "  make sync-malopolskie  - sync configured MPZP sources only for Małopolskie"
	@echo "  make mpzp-ruda         - sync only Ruda Slaska via WMS grid ingest"
	@echo "  make backend-dev       - run backend against local docker-compose PostGIS"
	@echo "  make backend-cloudsql  - run backend against Cloud SQL via auth proxy"
	@echo "  make cloudsql-health   - smoke-check Cloud SQL connection for backend"
	@echo "  make reparse-bronze    - rerun Bronze extraction on saved listings"
	@echo "  make geo-resolve       - run GeoResolver"
	@echo "  make delta             - run DeltaEngine"
	@echo "  make planning-signal-sync - sync normalized future-buildability planning signals"
	@echo "  make future-buildability - run FutureBuildabilityEngine"
	@echo "  make future-buildability-status - print rollout status, source health, and near-threshold backlog"
	@echo "  make future-buildability-backlog - export the source-discovery backlog"
	@echo "  make future-buildability-smoke - smoke-test rollout guardrails"
	@echo "  make force-retry       - reset queues and rerun GeoResolver + DeltaEngine"
	@echo "  make load-all-data     - MPZP sync + force retry + status summary"
	@echo "  make refresh-all       - reparse Bronze + MPZP sync + force retry + status"
	@echo "  make gliwice-cluster   - replay the Gliwice cluster helper"
	@echo "  make report-slaskie    - province report for Śląskie"
	@echo "  make report-malopolskie - province report for Małopolskie"
	@echo "  make delta-gap-malopolskie - report campaign diagnostics for Małopolskie"
	@echo "  make campaign-slaskie  - full automated campaign for Śląskie"
	@echo "  make campaign-malopolskie - full automated campaign for Małopolskie"
	@echo "  make campaign-all      - run both province campaigns sequentially"
	@echo "  make campaign-rollout-cloudsql - campaigns + planning signals + future_buildability on Cloud SQL"
	@echo "  make doctor            - light self-heal and backend status check"
	@echo "  make status            - print core DB counts"

sync-all: sync-backend sync-scraper sync-frontend

sync-backend:
	cd backend && uv sync

sync-scraper:
	cd scraper && uv sync

sync-frontend:
	cd frontend && npm install

backend-dev:
	cd backend && uv run uvicorn app.main:app --reload --host $(APP_HOST) --port $(APP_PORT)

backend-cloudsql:
	APP_HOST=$(APP_HOST) APP_PORT=$(APP_PORT) ./scripts/run_backend_cloudsql.sh serve

cloudsql-health:
	./scripts/run_backend_cloudsql.sh health

run-backend:
	cd backend && uv run uvicorn app.main:app --reload --host $(BACKEND_HOST) --port $(BACKEND_PORT)

run-frontend:
	cd frontend && VITE_API_PROXY_TARGET=http://127.0.0.1:$(BACKEND_PORT) npm run dev -- --host $(FRONTEND_HOST) --port $(FRONTEND_PORT)

run:
	BACKEND_HOST=$(BACKEND_HOST) BACKEND_PORT=$(BACKEND_PORT) FRONTEND_HOST=$(FRONTEND_HOST) FRONTEND_PORT=$(FRONTEND_PORT) ./scripts/run_dev.sh

run-local:
	BACKEND_MODE=local BACKEND_HOST=$(BACKEND_HOST) BACKEND_PORT=$(BACKEND_PORT) FRONTEND_HOST=$(FRONTEND_HOST) FRONTEND_PORT=$(FRONTEND_PORT) ./scripts/run_dev.sh
scrape-dry:
	./scripts/run_backend_cloudsql.sh exec scraper -- uv run python run_live.py --dry-run --provinces $(PROVINCES) --max-pages 1 --verbose

scrape-live:
	./scripts/run_backend_cloudsql.sh exec scraper -- uv run python run_live.py --provinces $(PROVINCES) --max-pages $(MAX_PAGES)

mpzp-registry:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python run_wfs_sync.py --list-registry

mpzp-uncovered:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python run_wfs_sync.py --list-uncovered

mpzp-sync:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python run_wfs_sync.py

sync-slaskie:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python run_wfs_sync.py --province slaskie

sync-malopolskie:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python run_wfs_sync.py --province malopolskie

mpzp-one:
	@if [ -z "$(TERYT)" ]; then \
		echo "Usage: make mpzp-one TERYT=2466011"; \
		exit 1; \
	fi
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python run_wfs_sync.py --teryt $(TERYT)

mpzp-ruda:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python run_wfs_sync.py --teryt 2472011

reparse-bronze:
	./scripts/run_backend_cloudsql.sh exec scraper -- uv run python reparse_bronze.py --disable-llm

geo-resolve:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python -m app.services.geo_resolver

delta:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python -m app.services.delta_engine

planning-signal-sync:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python run_planning_signal_sync.py $(if $(TERYT),--teryt $(TERYT),)

future-buildability:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python run_future_buildability.py \
		--batch-size $(BATCH_SIZE) \
		$(if $(TERYT),--teryt-gmina $(TERYT),) \
		$(if $(PROVINCE),--province $(PROVINCE),)

future-buildability-status:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python print_future_buildability_status.py $(if $(PROVINCE),--province $(PROVINCE),)

future-buildability-backlog:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python export_future_buildability_backlog.py \
		$(if $(PROVINCE),--province $(PROVINCE),) \
		--format $(BACKLOG_FORMAT) \
		$(if $(BACKLOG_OUTPUT),--output $(BACKLOG_OUTPUT),)

future-buildability-smoke:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python smoke_future_buildability_rollout.py \
		--expected-state $(EXPECTED_STATE) \
		--limit $(SMOKE_LIMIT)

force-retry:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python force_retry.py

gliwice-cluster:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python run_gliwice_cluster.py --replay

report-slaskie:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python run_province_campaign.py --province slaskie --stage report --parallel

report-malopolskie:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python run_province_campaign.py --province malopolskie --stage report --parallel

delta-gap-malopolskie:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python run_province_campaign.py --province malopolskie --stage report --parallel

campaign-slaskie:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python run_province_campaign.py --province slaskie --stage full --autofix --parallel

campaign-malopolskie:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python run_province_campaign.py --province malopolskie --stage full --autofix --parallel

campaign-all:
	$(MAKE) campaign-slaskie
	$(MAKE) campaign-malopolskie

campaign-rollout-cloudsql:
	$(MAKE) campaign-slaskie
	$(MAKE) campaign-malopolskie
	$(MAKE) planning-signal-sync
	$(MAKE) future-buildability PROVINCE=slaskie
	$(MAKE) future-buildability PROVINCE=malopolskie
	$(MAKE) future-buildability-status PROVINCE=slaskie
	$(MAKE) future-buildability-status PROVINCE=malopolskie
	$(MAKE) status

doctor:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python print_status.py

status:
	./scripts/run_backend_cloudsql.sh exec backend -- uv run python print_status.py

load-all-data:
	$(MAKE) mpzp-sync
	$(MAKE) force-retry
	$(MAKE) planning-signal-sync
	$(MAKE) future-buildability
	$(MAKE) status

refresh-all:
	$(MAKE) reparse-bronze
	$(MAKE) mpzp-sync
	$(MAKE) force-retry
	$(MAKE) planning-signal-sync
	$(MAKE) future-buildability
	$(MAKE) status

gcp-deploy-backend:
	gcloud builds submit --config cloudbuild.backend.yaml --project $(GCP_PROJECT_ID) \
		--service-account $(CLOUD_BUILD_SERVICE_ACCOUNT) \
		--gcs-source-staging-dir $(CLOUD_BUILD_SOURCE_STAGING_DIR) \
		--substitutions SHORT_SHA=$$(git rev-parse --short HEAD),_PROJECT_ID=$(GCP_PROJECT_ID),_REGION=$(GCP_REGION),_REPOSITORY=$(ARTIFACT_REGISTRY_REPOSITORY),_IMAGE=$(TERRAZONING_BACKEND_IMAGE),_SERVICE=$(TERRAZONING_API_SERVICE),_SYNC_JOBS=true,_JOB_SCRAPE_LIVE=$(TERRAZONING_JOB_SCRAPE_LIVE),_JOB_GEO_RESOLVE=$(TERRAZONING_JOB_GEO_RESOLVE),_JOB_DELTA=$(TERRAZONING_JOB_DELTA),_JOB_PLANNING_SIGNAL_SYNC=$(TERRAZONING_JOB_PLANNING_SIGNAL_SYNC),_JOB_FUTURE_BUILDABILITY=$(TERRAZONING_JOB_FUTURE_BUILDABILITY),_JOB_CAMPAIGN_ROLLOUT=$(TERRAZONING_JOB_CAMPAIGN_ROLLOUT)

gcp-deploy-frontend:
	@API_BASE_URL="$(TERRAZONING_FRONTEND_API_BASE_URL)"; \
	if [ -z "$$API_BASE_URL" ]; then \
		API_BASE_URL="$$(gcloud run services describe $(TERRAZONING_API_SERVICE) --project $(GCP_PROJECT_ID) --region $(GCP_REGION) --format='value(status.url)')"; \
	fi; \
	gcloud builds submit --config cloudbuild.frontend.yaml --project $(GCP_PROJECT_ID) \
		--service-account $(CLOUD_BUILD_SERVICE_ACCOUNT) \
		--gcs-source-staging-dir $(CLOUD_BUILD_SOURCE_STAGING_DIR) \
		--substitutions SHORT_SHA=$$(git rev-parse --short HEAD),_PROJECT_ID=$(GCP_PROJECT_ID),_REGION=$(GCP_REGION),_REPOSITORY=$(ARTIFACT_REGISTRY_REPOSITORY),_IMAGE=$(TERRAZONING_FRONTEND_IMAGE),_SERVICE=$(TERRAZONING_FRONTEND_SERVICE),_API_BASE_URL=$$API_BASE_URL,_FUTURE_BUILDABILITY_ENABLED=$(TERRAZONING_FUTURE_BUILDABILITY_ENABLED),_MAP_STYLE_URL=$(TERRAZONING_MAP_STYLE_URL)

gcp-service-urls:
	@printf 'API: '
	@gcloud run services describe $(TERRAZONING_API_SERVICE) --project $(GCP_PROJECT_ID) --region $(GCP_REGION) --format='value(status.url)'
	@printf 'Frontend: '
	@gcloud run services describe $(TERRAZONING_FRONTEND_SERVICE) --project $(GCP_PROJECT_ID) --region $(GCP_REGION) --format='value(status.url)'

gcp-smoke-api:
	@API_URL=$$(gcloud run services describe $(TERRAZONING_API_SERVICE) --project $(GCP_PROJECT_ID) --region $(GCP_REGION) --format='value(status.url)'); \
	TOKEN=$$(gcloud auth print-identity-token --audiences "$${API_URL}"); \
	curl --fail --show-error --silent -H "Authorization: Bearer $${TOKEN}" "$${API_URL}/api/v1/health" >/dev/null && echo "API smoke check ok: $${API_URL}"

gcp-smoke-frontend:
	@FRONTEND_URL=$$(gcloud run services describe $(TERRAZONING_FRONTEND_SERVICE) --project $(GCP_PROJECT_ID) --region $(GCP_REGION) --format='value(status.url)'); \
	TOKEN=$$(gcloud auth print-identity-token --audiences "$${FRONTEND_URL}"); \
	curl --fail --show-error --silent -H "Authorization: Bearer $${TOKEN}" "$${FRONTEND_URL}" >/dev/null && echo "Frontend smoke check ok: $${FRONTEND_URL}"

gcp-auth:
	gcloud auth login
	gcloud config set project $(GCP_PROJECT_ID)

gcp-proxy:
	bash ./scripts/run_gcp_proxies.sh

gcp-proxy-frontend:
	gcloud run services proxy $(TERRAZONING_FRONTEND_SERVICE) --project $(GCP_PROJECT_ID) --region $(GCP_REGION) --port $(FRONTEND_PORT)

gcp-proxy-api:
	gcloud run services proxy $(TERRAZONING_API_SERVICE) --project $(GCP_PROJECT_ID) --region $(GCP_REGION) --port $(BACKEND_PORT)

gcp-job-scrape-live:
	gcloud run jobs execute $(TERRAZONING_JOB_SCRAPE_LIVE) --project $(GCP_PROJECT_ID) --region $(GCP_REGION) --wait

gcp-job-geo-resolve:
	gcloud run jobs execute $(TERRAZONING_JOB_GEO_RESOLVE) --project $(GCP_PROJECT_ID) --region $(GCP_REGION) --wait

gcp-job-delta:
	gcloud run jobs execute $(TERRAZONING_JOB_DELTA) --project $(GCP_PROJECT_ID) --region $(GCP_REGION) --wait

gcp-job-planning-signal-sync:
	gcloud run jobs execute $(TERRAZONING_JOB_PLANNING_SIGNAL_SYNC) --project $(GCP_PROJECT_ID) --region $(GCP_REGION) --wait \
		$(if $(GCP_JOB_ARGS),--args "$(GCP_JOB_ARGS)",)

gcp-job-future-buildability:
	gcloud run jobs execute $(TERRAZONING_JOB_FUTURE_BUILDABILITY) --project $(GCP_PROJECT_ID) --region $(GCP_REGION) --wait \
		$(if $(GCP_JOB_ARGS),--args "$(GCP_JOB_ARGS)",)

gcp-job-campaign-rollout:
	gcloud run jobs execute $(TERRAZONING_JOB_CAMPAIGN_ROLLOUT) --project $(GCP_PROJECT_ID) --region $(GCP_REGION) --wait \
		$(if $(GCP_JOB_ARGS),--args "$(GCP_JOB_ARGS)",)
