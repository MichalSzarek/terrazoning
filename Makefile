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
APP_HOST ?= 0.0.0.0
APP_PORT ?= 8000

.PHONY: help sync-all sync-backend sync-scraper sync-frontend scrape-dry scrape-live mpzp-sync mpzp-one \
	mpzp-uncovered mpzp-registry mpzp-ruda reparse-bronze geo-resolve delta planning-signal-sync \
	future-buildability future-buildability-status future-buildability-backlog future-buildability-smoke force-retry load-all-data \
	refresh-all gliwice-cluster status doctor sync-slaskie sync-malopolskie report-slaskie \
	report-malopolskie delta-gap-malopolskie campaign-slaskie campaign-malopolskie campaign-all

help:
	@echo "TerraZoning data operations"
	@echo ""
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

scrape-dry:
	cd scraper && uv run python run_live.py --dry-run --provinces $(PROVINCES) --max-pages 1 --verbose

scrape-live:
	cd scraper && uv run python run_live.py --provinces $(PROVINCES) --max-pages $(MAX_PAGES)

mpzp-registry:
	cd backend && uv run python run_wfs_sync.py --list-registry

mpzp-uncovered:
	cd backend && uv run python run_wfs_sync.py --list-uncovered

mpzp-sync:
	cd backend && uv run python run_wfs_sync.py

sync-slaskie:
	cd backend && uv run python run_wfs_sync.py --province slaskie

sync-malopolskie:
	cd backend && uv run python run_wfs_sync.py --province malopolskie

mpzp-one:
	@if [ -z "$(TERYT)" ]; then \
		echo "Usage: make mpzp-one TERYT=2466011"; \
		exit 1; \
	fi
	cd backend && uv run python run_wfs_sync.py --teryt $(TERYT)

mpzp-ruda:
	cd backend && uv run python run_wfs_sync.py --teryt 2472011

reparse-bronze:
	cd scraper && uv run python reparse_bronze.py --disable-llm

geo-resolve:
	cd backend && uv run python -m app.services.geo_resolver

delta:
	cd backend && uv run python -m app.services.delta_engine

planning-signal-sync:
	cd backend && uv run python run_planning_signal_sync.py $(if $(TERYT),--teryt $(TERYT),)

future-buildability:
	cd backend && uv run python run_future_buildability.py \
		--batch-size $(BATCH_SIZE) \
		$(if $(TERYT),--teryt-gmina $(TERYT),) \
		$(if $(PROVINCE),--province $(PROVINCE),)

future-buildability-status:
	cd backend && uv run python print_future_buildability_status.py $(if $(PROVINCE),--province $(PROVINCE),)

future-buildability-backlog:
	cd backend && uv run python export_future_buildability_backlog.py \
		$(if $(PROVINCE),--province $(PROVINCE),) \
		--format $(BACKLOG_FORMAT) \
		$(if $(BACKLOG_OUTPUT),--output $(BACKLOG_OUTPUT),)

future-buildability-smoke:
	cd backend && uv run python smoke_future_buildability_rollout.py \
		--expected-state $(EXPECTED_STATE) \
		--limit $(SMOKE_LIMIT)

force-retry:
	cd backend && uv run python force_retry.py

gliwice-cluster:
	cd backend && uv run python run_gliwice_cluster.py --replay

report-slaskie:
	cd backend && uv run python run_province_campaign.py --province slaskie --stage report --parallel

report-malopolskie:
	cd backend && uv run python run_province_campaign.py --province malopolskie --stage report --parallel

delta-gap-malopolskie:
	cd backend && uv run python run_province_campaign.py --province malopolskie --stage report --parallel

campaign-slaskie:
	cd backend && uv run python run_province_campaign.py --province slaskie --stage full --autofix --parallel

campaign-malopolskie:
	cd backend && uv run python run_province_campaign.py --province malopolskie --stage full --autofix --parallel

campaign-all:
	$(MAKE) campaign-slaskie
	$(MAKE) campaign-malopolskie

doctor:
	cd backend && (uv run python print_status.py || (uv sync && uv run python print_status.py))

status:
	cd backend && uv run python print_status.py

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
