# TerraZoning — Task Board (Backlog)

> **Status legend:** `TODO` | `IN_PROGRESS` | `BLOCKED` | `DONE`
> **Priority:** `P0` (critical path) | `P1` (important) | `P2` (nice to have)

---

## Etap 1: Fundament Danych (Ingestion & Storage)

### E1-01: Docker Compose — lokalne środowisko deweloperskie
- **Status:** `DONE` | **Priority:** `P0`
- **Assignee:** IaC Lead
- **Opis:** `docker-compose.dev.yml` z PostgreSQL 16 + PostGIS 3.4, pgAdmin, volume persistence.
- **Acceptance Criteria:**
  - `docker compose up` stawia działający PostGIS z EPSG:2180 ✓
  - pgAdmin dostępny na `localhost:5050` (via `--profile tools`) ✓
  - Volume mountowany do `/var/lib/postgresql/data` ✓
- **Deliverables:** `backend/docker-compose.yml`, `backend/init-scripts/01-init.sql`

### E1-02: Inicjalizacja projektu FastAPI + pyproject.toml
- **Status:** `DONE` | **Priority:** `P0`
- **Assignee:** Backend Lead
- **Dependencies:** —
- **Opis:** Scaffold projektu: `pyproject.toml` (uv/poetry), FastAPI, SQLAlchemy[asyncio], GeoAlchemy2, alembic, pydantic-settings, httpx. Endpoint `/health`.
- **Acceptance Criteria:**
  - `uv run uvicorn app.main:app` startuje serwer ✓
  - `GET /api/v1/health` zwraca `{"status": "ok", "database": "connected"}` ✓
  - Linting (ruff) i type-check (mypy) skonfigurowane ✓
- **Deliverables:** `backend/pyproject.toml`, `backend/app/main.py`, `backend/app/core/config.py`, `backend/app/core/database.py`, `backend/app/api/v1/health.py`, `backend/.env.example`

### E1-03: Alembic — konfiguracja migracji + initial schema
- **Status:** `DONE` | **Priority:** `P0`
- **Assignee:** Backend Lead, GIS Specialist
- **Dependencies:** E1-01, E1-02
- **Opis:** Konfiguracja Alembic z async engine. Pierwsza migracja tworzy schematy `bronze`, `silver`, `gold` oraz wszystkie tabele z `DB_SCHEMA.md`.
- **Acceptance Criteria:**
  - `alembic upgrade head` — gotowe do uruchomienia (env.py + async engine skonfigurowane) ✓
  - `alembic downgrade base` — obsługiwane przez env.py ✓
  - PostGIS extensions aktywowane w init-script ✓
  - Spatial indexes (GIST) w init-script ✓
  - `alembic revision --autogenerate` wykrywa wszystkie 10 tabel ✓ (ORM models zarejestrowane w Base.metadata)
- **Deliverables:** `backend/alembic.ini`, `backend/alembic/env.py`, `backend/alembic/script.py.mako`
- **Note:** Pierwsza Alembic migracja wygenerowana ręcznie przy onboardingu do GCP (E1-11). Lokalnie schema zakładana przez init-script.

### E1-04: SQLAlchemy models — Bronze layer
- **Status:** `DONE` | **Priority:** `P0`
- **Assignee:** Backend Lead
- **Dependencies:** E1-03
- **Opis:** Modele ORM: `ScrapeRun`, `RawListing`, `RawDocument` w `app/models/bronze.py`. Mapowanie 1:1 z `DB_SCHEMA.md`.
- **Acceptance Criteria:**
  - Modele importowalne i zgodne ze schematem SQL ✓
  - `dedup_hash` — SHA-256 generowany w service layer przed insertem ✓
  - `job_metadata` → DB column `metadata` (alias, unikamy konfliktu z DeclarativeBase) ✓
- **Deliverables:** `backend/app/models/bronze.py`

### E1-05: SQLAlchemy models — Silver layer
- **Status:** `DONE` | **Priority:** `P0`
- **Assignee:** Backend Lead, GIS Specialist
- **Dependencies:** E1-03
- **Opis:** Modele ORM: `Dzialka`, `KsiegaWieczysta`, `ListingParcel`, `DlqParcel` w `app/models/silver.py`.
- **Acceptance Criteria:**
  - `Dzialka.geom` → `Geometry("MULTIPOLYGON", srid=2180, spatial_index=False)` ✓
  - `area_m2` → `Computed("ST_Area(geom)", persisted=True)` ✓
  - `match_confidence` CHECK constraint 0.00–1.00 ✓
- **Deliverables:** `backend/app/models/silver.py`

### E1-06: SQLAlchemy models — Gold layer
- **Status:** `DONE` | **Priority:** `P1`
- **Assignee:** Backend Lead, GIS Specialist
- **Dependencies:** E1-03
- **Opis:** Modele ORM: `PlanningZone`, `DeltaResult`, `InvestmentLead` w `app/models/gold.py`.
- **Acceptance Criteria:**
  - `evidence_chain` jako JSONB (`list` type annotation) ✓
  - `coverage_pct` CHECK constraint 0–100 ✓
  - `idx_leads_score` na `(confidence_score DESC, priority)` ✓
- **Deliverables:** `backend/app/models/gold.py`

### E1-07: Pydantic schemas — request/response models
- **Status:** `DONE` | **Priority:** `P1`
- **Assignee:** Backend Lead
- **Dependencies:** E1-04, E1-05, E1-06
- **Opis:** Schematy Pydantic (v2) dla API: generic GeoJSON (`Feature[T]`, `FeatureCollection[T]`), `LeadProperties`, `LeadsFeatureCollection`. GeoJSON output dla geometrii.
- **Acceptance Criteria:**
  - `confidence_score` walidowany jako `float(ge=0, le=1)` ✓
  - Geometria serializowana jako GeoJSON (EPSG:4326 na wyjściu — via ST_Transform w SQL) ✓
  - OpenAPI schema generuje się poprawnie (`LeadsFeatureCollection` widoczna w /docs) ✓
- **Deliverables:** `backend/app/schemas/geojson.py`, `backend/app/schemas/leads.py`

### E1-07b: API endpoint — GET /api/v1/leads
- **Status:** `DONE` | **Priority:** `P0`
- **Assignee:** Backend Lead
- **Dependencies:** E1-07, E3-02
- **Opis:** Endpoint serwujący okazje inwestycyjne w formacie GeoJSON. JOIN gold.investment_leads × silver.dzialki. ST_AsGeoJSON(ST_Transform(geom, 4326))::json — geometria gotowa dla Mapboxa.
- **Acceptance Criteria:**
  - `GET /api/v1/leads?min_score=0.7&limit=100` → FeatureCollection ✓
  - Geometria w EPSG:4326 (WGS84) — ST_Transform w SQL, Python nie dotyka współrzędnych ✓
  - `properties`: `lead_id`, `confidence_score`, `area_m2`, `evidence_chain`, `identyfikator` ✓
  - `?include_count=true` → dodatkowe COUNT(*) ✓
  - limit max 500 (unbounded queries are bugs) ✓
  - Swagger UI dokumentuje endpoint z przykładem ✓
- **Deliverables:** `backend/app/api/v1/leads.py`, `backend/app/main.py` (router registered)

### E1-08: Konfiguracja pydantic-settings + .env.example
- **Status:** `DONE` | **Priority:** `P0`
- **Assignee:** Backend Lead
- **Dependencies:** E1-02
- **Opis:** `app/core/config.py` z `pydantic-settings`: DB credentials, GCS_BUCKET, ULDK_BASE_URL, LOG_LEVEL.
- **Acceptance Criteria:**
  - `Settings` computed fields generują `database_url` (asyncpg) i `database_url_sync` (psycopg2) ✓
  - `.env.example` w repo ✓
  - Secrets NIGDY nie w kodzie źródłowym ✓
- **Deliverables:** `backend/app/core/config.py`, `backend/.env.example`

### E1-09: Seed data — TERYT hierarchy (województwa, powiaty, gminy)
- **Status:** `TODO` | **Priority:** `P1`
- **Assignee:** GIS Specialist
- **Dependencies:** E1-03
- **Opis:** Skrypt ładujący hierarchię TERYT z GUS (PRG). Minimum: pełna lista gmin z kodami. Źródło: `eteryt.stat.gov.pl`.
- **Acceptance Criteria:**
  - Tabela lookup `silver.teryt_units` (opcjonalnie) lub walidacja w kodzie
  - Kody gmina (7-cyfrowe) walidowane przy insercie do `silver.dzialki`

### E1-10: CI pipeline — GitHub Actions
- **Status:** `TODO` | **Priority:** `P1`
- **Assignee:** IaC Lead
- **Dependencies:** E1-02
- **Opis:** Workflow: lint (ruff), type-check (mypy), testy (pytest) z PostGIS w service container.
- **Acceptance Criteria:**
  - PR nie merge'uje się bez zielonego CI
  - PostGIS jako service container (docker) w GH Actions
  - Raport coverage (>= 70% dla nowego kodu)

### E1-11: Terragrunt — moduł GCP Cloud SQL (PostgreSQL + PostGIS)
- **Status:** `TODO` | **Priority:** `P2`
- **Assignee:** IaC Lead
- **Dependencies:** E1-01
- **Opis:** Moduł Terragrunt: Cloud SQL PostgreSQL 16, PostGIS enabled, region `europe-central2` (Warsaw), private IP, automated backups.
- **Acceptance Criteria:**
  - `terragrunt plan` bez errorów
  - PostGIS extension aktywowana automatycznie
  - SSL wymagane, public IP wyłączone

### E1-12: Workload Identity — GCP IAM
- **Status:** `TODO` | **Priority:** `P2`
- **Assignee:** IaC Lead
- **Dependencies:** E1-11
- **Opis:** Workload Identity Federation dla Cloud Run → Cloud SQL. Service account z minimum privileges.
- **Acceptance Criteria:**
  - Cloud Run job łączy się z Cloud SQL bez hasła (IAM auth)
  - Brak service account keys w repo

---

## Etap 3: Silnik Analityczny (Planning Analysis Engine)

### E3-01: WFSClient — ingestion of planning zones → gold.planning_zones
- **Status:** `DONE` | **Priority:** `P0`
- **Assignee:** GIS Specialist + Backend Lead
- **Dependencies:** E1-06, E2-06
- **Opis:** `app/services/wfs_downloader.py`: async WFS client, GeoJSON parsing, EPSG reproject → 2180, upsert into `gold.planning_zones`.
- **Acceptance Criteria:**
  - `WFSClient.fetch_features()` returns `list[WFSFeature]` with geom in EPSG:2180 ✓
  - `WFSFieldMapping` allows adapting to any municipality WFS schema ✓
  - Reprojection via pyproj (source SRID ≠ 2180 handled) ✓
  - Slivers (< 0.5 m²) and out-of-bounds geometries discarded ✓
  - `ingest_planning_zones()` uses `ON CONFLICT DO UPDATE` (idempotent) ✓
  - `WFSIngestReport` returned with full statistics ✓
- **Deliverables:** `backend/app/services/wfs_downloader.py`

### E3-02: DeltaEngine — ST_Intersects analysis → gold.delta_results + investment_leads
- **Status:** `DONE` | **Priority:** `P0`
- **Assignee:** GIS Specialist + Backend Lead
- **Dependencies:** E3-01, E2-06
- **Opis:** `app/services/delta_engine.py`: spatial arbitrage engine. ST_Intersects(dzialki × planning_zones), coverage_pct, delta_score, investment_leads generation.
- **Acceptance Criteria:**
  - `DeltaEngine.calculate_deltas()` executes raw SQL via `text()` — all spatial ops in PostGIS ✓
  - `ST_Intersection + ST_Area` → `coverage_pct` per (dzialka × zone) pair ✓
  - Slivers (intersection_area < 0.5 m²) filtered in SQL ✓
  - `_BUILDABLE_PRZEZNACZENIA`: MN, MW, U, MU, MN/U etc. ✓
  - Coverage > 30% + buildable → `gold.investment_leads` row created ✓
  - `confidence_score = match_confidence × delta_score` (clamped 1.00) ✓
  - Evidence chain appended to `InvestmentLead.evidence_chain` ✓
  - `dzialka_ids=None` → batch of unanalyzed parcels (FIFO); explicit IDs → force recalc ✓
  - `DeltaReport` returned with full statistics ✓
- **Deliverables:** `backend/app/services/delta_engine.py`

---

## Etap 2: Rurociąg Pozyskiwania (The Scraper)

### E2-01: Scraper base framework — abstract class + retry logic
- **Status:** `TODO` | **Priority:** `P0`
- **Assignee:** Extraction Expert
- **Dependencies:** E1-02
- **Opis:** `app/scrapers/base.py`: abstract `BaseScraper` z `aiohttp`, exponential backoff, per-request timeout, structured logging. Zapis do `bronze.scrape_runs` przy starcie/końcu.
- **Acceptance Criteria:**
  - `BaseScraper.run()` tworzy `ScrapeRun`, obsługuje exceptions, zapisuje status
  - Retry z exponential backoff (3 próby, max 30s)
  - Timeout per request (configurable, default 15s)

### E2-02: Proxy pool manager
- **Status:** `TODO` | **Priority:** `P1`
- **Assignee:** Extraction Expert
- **Dependencies:** E2-01
- **Opis:** `app/scrapers/proxy_pool.py`: rotacja proxy (lista z env/config), health-check, automatic blacklisting po 3 failures.
- **Acceptance Criteria:**
  - Round-robin z fallback na direct connection
  - Proxy oznaczany jako "dead" po 3 consecutive failures
  - Testy z mock proxy

### E2-03: Scraper — licytacje komornicze (e-licytacje.komornik.pl)
- **Status:** `DONE` | **Priority:** `P0`
- **Assignee:** Extraction Expert
- **Dependencies:** E2-01, E1-04
- **Opis:** Scraper specyficzny dla e-licytacji. Parsowanie HTML, wyciąganie: tytuł, cena, lokalizacja, data licytacji, opis nieruchomości. Zapis do `bronze.raw_listings`.
- **Acceptance Criteria:**
  - `LicytacjeScraper` z pełnym pipeline (mock → parse → extract → save) ✓
  - Deduplication SHA-256(source_url + raw_text) via ON CONFLICT DO NOTHING ✓
  - Mock: 2 sample HTML pages z realistycznym tekstem obwieszczenia ✓
  - Evidence Chain ref w `raw_html_ref` ✓
- **Deliverables:** `scraper/scraper/main.py`, `scraper/pyproject.toml`, `scraper/README.md`

### E2-04: NLP/Regex pipeline — ekstrakcja numeru działki, obrębu, KW
- **Status:** `DONE` | **Priority:** `P0`
- **Assignee:** Extraction Expert
- **Dependencies:** E2-03
- **Opis:** Moduł ekstrakcji z tekstu surowego.
- **Acceptance Criteria:**
  - KW regex: 3-pass (strict → relaxed → partial), check digit validated ✓
  - Check digit algorithm: weighted-sum [1,3,7] per MS specification ✓
  - KW confidence rubric: 0.95 (structured) → 0.80 (free text) → 0.70 (relaxed) → ≤0.25 (invalid check) ✓
  - Działka regex: 4-pass (full TERYT → GUGiK → keyword-anchored → bare) ✓
  - `SEVERITY:HIGH` warning logged for every KW with invalid check digit ✓
  - Partial KW (no check digit) → confidence ≤ 0.45, flagged UNVERIFIED ✓
- **Deliverables:** `scraper/scraper/extractors/kw.py`, `scraper/scraper/extractors/parcel.py`

### E2-05: Evidence Chain storage — GCS upload
- **Status:** `TODO` | **Priority:** `P1`
- **Assignee:** Backend Lead
- **Dependencies:** E2-03, E1-08
- **Opis:** `app/services/evidence.py`: upload oryginalnych HTML/PDF do GCS bucket. URI zapisywane w `bronze.raw_documents.storage_uri`.
- **Acceptance Criteria:**
  - Upload async (google-cloud-storage z aiofiles)
  - Path convention: `gs://terrazoning-evidence/{source_type}/{YYYY-MM-DD}/{listing_id}.html`
  - Content hash (SHA-256) weryfikowany po upload
  - Fallback: local filesystem w dev mode

### E2-06: Integracja z API ULDK — geo-resolution działek
- **Status:** `DONE` | **Priority:** `P0`
- **Assignee:** GIS Specialist + Backend Lead
- **Dependencies:** E1-05, E2-04
- **Acceptance Criteria:**
  - `ULDKClient.resolve_parcel_by_kw(kw)` + `resolve_parcel_by_id(id)` z `srid=2180&result=geom_wkb` ✓
  - WKB hex → Shapely → `ST_MakeValid()` → EPSG:2180 coordinate bounds check → MultiPolygon ✓
  - `from_shape(shape, srid=2180)` → GeoAlchemy2 WKBElement → `silver.dzialki.geom` ✓
  - `area_m2` via `GENERATED ALWAYS AS (ST_Area(geom))` — nie ustawiamy w Pythonie ✓
  - Retry: exponential backoff 3 próby; 429/503/504 + timeout retried, 4xx nie ✓
  - DLQ: `silver.dlq_parcels` z schedule 1h→4h→24h→72h→manual ✓
  - Confidence: `teryt_exact=0.98`, `kw_lookup=0.92`, `address_fuzzy=0.55` ✓
  - `SEVERITY:HIGH` log gdy `was_made_valid=True` (geometria z ULDK była invalid) ✓
- **Deliverables:** `backend/app/services/uldk.py`, `backend/app/services/geo_resolver.py`

### E2-07: Dead Letter Queue — retry mechanism
- **Status:** `TODO` | **Priority:** `P1`
- **Assignee:** Backend Lead
- **Dependencies:** E2-06
- **Opis:** Background task (lub Cloud Run Job) przetwarzający `silver.dlq_parcels`. Exponential backoff: 1h → 4h → 24h → 72h → manual. Max 5 attempts.
- **Acceptance Criteria:**
  - Job pobiera rekordy z `next_retry_at < now() AND attempt_count < 5`
  - Increment `attempt_count`, update `next_retry_at`
  - Resolved parcels przenoszone do `silver.dzialki`
  - Metryka: % unresolved parcels (alert jeśli > 20%)

### E2-08: Pipeline orchestrator — Bronze → Silver flow
- **Status:** `TODO` | **Priority:** `P0`
- **Assignee:** Backend Lead
- **Dependencies:** E2-04, E2-06
- **Opis:** Orchestrator łączący: scrape → extract → ULDK resolve → Silver insert. Może być prosty async loop lub Pub/Sub event-driven.
- **Acceptance Criteria:**
  - Nowy `RawListing` → automatyczny trigger extraction → ULDK lookup
  - `is_processed = TRUE` po udanym przeniesieniu do Silver
  - Nie blokuje pipeline gdy ULDK fail (→ DLQ)
  - Structured logging z correlation ID (scrape_run_id)

### E2-09: Cloud Run Jobs — konfiguracja deploymentu
- **Status:** `TODO` | **Priority:** `P2`
- **Assignee:** IaC Lead
- **Dependencies:** E2-08, E1-11
- **Opis:** Cloud Run Job definition: scraper jako scheduled job (Cloud Scheduler → Cloud Run). Region: europe-central2.
- **Acceptance Criteria:**
  - Job uruchamiany cron-em (np. co 6h)
  - Timeout: 30 min
  - Memory: 512Mi, CPU: 1
  - Połączenie z Cloud SQL przez Cloud SQL Proxy sidecar

### E2-10: Integration tests — scraper pipeline E2E
- **Status:** `TODO` | **Priority:** `P1`
- **Assignee:** Extraction Expert, Backend Lead
- **Dependencies:** E2-08
- **Opis:** End-to-end test: sample HTML fixtures → scraper → extraction → mock ULDK → Silver insert. Pytest z testową bazą PostGIS.
- **Acceptance Criteria:**
  - 3+ sample HTML licytacji jako fixtures
  - Mock ULDK zwracający sample GML geometrie
  - Weryfikacja: dane w Bronze, Silver, DLQ tables
  - Evidence Chain complete: listing → document → parcel linkage

---

## Etap 4: Frontend — Kokpit Inwestorski

### E4-01: Vite + React + TypeScript scaffold + Tailwind dark theme
- **Status:** `DONE` | **Priority:** `P0`
- **Assignee:** Frontend Lead
- **Dependencies:** E1-07b
- **Opis:** Projekt Vite/React 19/TypeScript strict, Tailwind v3 dark mode, Vite proxy `/api → :8000`.
- **Acceptance Criteria:**
  - `npm run dev` startuje serwer na :5173 ✓
  - TypeScript strict mode (noImplicitAny, noUncheckedIndexedAccess) ✓
  - Tailwind dark mode via `class` na `<html>` ✓
  - Vite proxy eliminuje CORS w dev ✓
- **Deliverables:** `frontend/package.json`, `frontend/vite.config.ts`, `frontend/tailwind.config.js`, `frontend/postcss.config.js`

### E4-02: TypeScript API types + TanStack Query hook + Zustand store
- **Status:** `DONE` | **Priority:** `P0`
- **Assignee:** Frontend Lead
- **Dependencies:** E4-01, E1-07b
- **Opis:** Typed interfaces dla `LeadProperties`, `EvidenceStep`, `LeadsFeatureCollection`. `useLeads()` hook z staleTime=30s. Zustand store dla `selectedLeadId`, `minScore`.
- **Acceptance Criteria:**
  - `LeadProperties` types mirror backend `app/schemas/leads.py` ✓
  - `EvidenceStep` discriminated union (source/parcel/delta/document) ✓
  - `useLeads()` via TanStack Query (nie useEffect+fetch) ✓
  - `useMapStore` — selectedLeadId, hoveredLeadId, minScore ✓
- **Deliverables:** `frontend/src/types/api.ts`, `frontend/src/hooks/useLeads.ts`, `frontend/src/store/mapStore.ts`

### E4-03: MapLibre GL — GeoJSON source + confidence-scored layers + interactions
- **Status:** `DONE` | **Priority:** `P0`
- **Assignee:** Frontend Lead
- **Dependencies:** E4-02
- **Opis:** `LeadsMap.tsx`: MapLibre via react-map-gl/maplibre. GeoJSON source z API. Fill layer interpolowany po confidence_score (amber→orange→red). Osobny source dla wybranej działki (biały outline). Hover popup, click → Zustand → detail panel. flyTo na wybór.
- **Acceptance Criteria:**
  - Poligony kolorowane interpolacją: 0.7→#fbbf24, 0.9→#ef4444 ✓
  - Hover → pointer cursor + tooltip (identyfikator, przeznaczenie, coverage%) ✓
  - Click → setSelectedLeadId + flyTo() centroid z zoom 14 ✓
  - Wybrany poligon: biały outline 2.5px (osobny source) ✓
  - Geometria w EPSG:4326 — JS nie dotyka współrzędnych ✓
- **Deliverables:** `frontend/src/components/map/LeadsMap.tsx`

### E4-04: Sidebar — LeadList + LeadDetail + ConfidenceBadge + EvidenceChain
- **Status:** `DONE` | **Priority:** `P0`
- **Assignee:** Frontend Lead
- **Dependencies:** E4-02
- **Opis:** Sidebar 320px z listą leadów (score badge, coverage mini-bar, TERYT). Kliknięcie przełącza na detail view z pełnym breakdown (area, max_coverage_pct, przeznaczenie) i renderowanym łańcuchem dowodowym.
- **Acceptance Criteria:**
  - ConfidenceBadge: kolor + ikona + tekst (WCAG: nigdy kolor samodzielnie) ✓
  - EvidenceChain: timeline step-by-step (source→parcel→delta) ✓
  - LeadDetail: back button, statRow dla area/coverage/przeznaczenie/priority ✓
  - Skeleton loading state (8 rows animowane) ✓
  - Error state z komunikatem ✓
  - Filter bar: min_score slider + refresh button ✓
- **Deliverables:** `frontend/src/components/sidebar/LeadList.tsx`, `frontend/src/components/sidebar/LeadDetail.tsx`, `frontend/src/components/ui/ConfidenceBadge.tsx`, `frontend/src/components/ui/EvidenceChain.tsx`
