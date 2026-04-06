# TerraZoning — Database Schema (PostGIS)

> **Canonical Projection:** EPSG:2180 (PUWG 1992)
> **Engine:** PostgreSQL 16 + PostGIS 3.4
> **Architecture:** Medallion (Bronze → Silver → Gold)

---

## Prerequisites

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS pg_trgm;      -- fuzzy text matching for addresses
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

---

## 1. Bronze Layer — Surowe Dane (Raw Ingestion)

Zasada: **Trust No One.** Dane trafiają tu w oryginalnej postaci. Żadnych transformacji geometrii, żadnego parsowania — tylko zapis i metadane scrapowania.

### 1.1 `bronze.scrape_runs`

Meta-dane każdego uruchomienia scrapera (job tracking).

```sql
CREATE TABLE bronze.scrape_runs (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_name     TEXT NOT NULL,                  -- np. 'e-licytacje', 'otodom', 'geoportal_pog'
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running'  -- running | completed | failed | partial
                    CHECK (status IN ('running', 'completed', 'failed', 'partial')),
    records_found   INTEGER DEFAULT 0,
    records_saved   INTEGER DEFAULT 0,
    proxy_used      TEXT,                           -- identyfikator proxy (rotacja)
    error_message   TEXT,
    metadata        JSONB DEFAULT '{}'::jsonb,      -- dowolne parametry job-a
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_scrape_runs_source ON bronze.scrape_runs (source_name, started_at DESC);
```

### 1.2 `bronze.raw_listings`

Surowe ogłoszenia i licytacje — tekst wyciągnięty ze stron, bez normalizacji.

```sql
CREATE TABLE bronze.raw_listings (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scrape_run_id   UUID NOT NULL REFERENCES bronze.scrape_runs(id),
    source_url      TEXT NOT NULL,                  -- URL ogłoszenia/licytacji
    source_type     TEXT NOT NULL                   -- 'licytacja_komornicza' | 'ogloszenie' | 'przetarg'
                    CHECK (source_type IN ('licytacja_komornicza', 'ogloszenie', 'przetarg', 'inne')),
    title           TEXT,
    raw_text        TEXT,                           -- wyekstrahowany tekst
    price_zl        NUMERIC(14,2),                  -- cena w PLN (jeśli znaleziona)
    area_m2         NUMERIC(12,2),                  -- powierzchnia w m² (jeśli znaleziona)
    auction_date    DATE,                           -- data licytacji (jeśli dotyczy)

    -- Surowe pola wyciągnięte przez regex/NLP (mogą być NULL lub błędne)
    raw_numer_dzialki   TEXT,                       -- np. '123/4' — jeszcze nie zwalidowany
    raw_obreb           TEXT,                       -- np. 'Wola' — jeszcze nie znormalizowany
    raw_gmina           TEXT,
    raw_powiat          TEXT,
    raw_wojewodztwo     TEXT,
    raw_kw              TEXT,                       -- nr Księgi Wieczystej (jeśli znaleziony)

    -- Evidence Chain
    raw_html_ref    TEXT,                           -- ścieżka do bronze.raw_documents lub GCS URI
    raw_pdf_ref     TEXT,

    dedup_hash      TEXT NOT NULL,                  -- SHA-256 z (source_url + raw_text) do deduplication
    is_processed    BOOLEAN NOT NULL DEFAULT FALSE, -- przeniesione do Silver?
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_raw_listings_dedup ON bronze.raw_listings (dedup_hash);
CREATE INDEX idx_raw_listings_source_type ON bronze.raw_listings (source_type, created_at DESC);
CREATE INDEX idx_raw_listings_unprocessed ON bronze.raw_listings (is_processed) WHERE NOT is_processed;
CREATE INDEX idx_raw_listings_scrape_run ON bronze.raw_listings (scrape_run_id);
```

### 1.3 `bronze.raw_documents`

Archiwum oryginalnych plików HTML/PDF — niezbędne dla Evidence Chain.

```sql
CREATE TABLE bronze.raw_documents (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    listing_id      UUID REFERENCES bronze.raw_listings(id),
    document_type   TEXT NOT NULL CHECK (document_type IN ('html', 'pdf', 'screenshot', 'json')),
    storage_uri     TEXT NOT NULL,                  -- GCS URI: gs://terrazoning-evidence/...
    file_size_bytes BIGINT,
    content_hash    TEXT NOT NULL,                  -- SHA-256 zawartości pliku
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_raw_documents_listing ON bronze.raw_documents (listing_id);
CREATE UNIQUE INDEX idx_raw_documents_hash ON bronze.raw_documents (content_hash);
```

---

## 2. Silver Layer — Znormalizowane Dane Przestrzenne

Zasada: **EPSG:2180 is King.** Każda geometria przechowywana w PUWG 1992. TERYT jako klucz identyfikacji przestrzennej. Każdy rekord ma `match_confidence`.

### 2.1 `silver.dzialki`

Znormalizowane działki z geometrią pobraną z ULDK.

```sql
CREATE TABLE silver.dzialki (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Identyfikacja TERYT (hierarchia administracyjna)
    teryt_wojewodztwo   CHAR(2) NOT NULL,           -- kod TERYT województwa
    teryt_powiat        CHAR(4) NOT NULL,           -- kod TERYT powiatu
    teryt_gmina         CHAR(7) NOT NULL,           -- kod TERYT gminy
    teryt_obreb         CHAR(9) NOT NULL,           -- kod TERYT obrębu
    numer_dzialki       TEXT NOT NULL,               -- np. '123/4'
    identyfikator       TEXT NOT NULL UNIQUE,         -- pełny ID: teryt_obreb + '.' + numer_dzialki

    -- Geometria (EPSG:2180 — PUWG 1992)
    geom                GEOMETRY(MultiPolygon, 2180) NOT NULL,
    area_m2             NUMERIC(12,2)
                        GENERATED ALWAYS AS (ST_Area(geom)) STORED,

    -- Metadata z ULDK
    uldk_response_date  TIMESTAMPTZ,                -- kiedy pobrano z ULDK
    uldk_raw_response   JSONB,                      -- surowa odpowiedź API

    -- Confidence & Status
    match_confidence    NUMERIC(3,2) NOT NULL DEFAULT 0.00
                        CHECK (match_confidence >= 0.00 AND match_confidence <= 1.00),
    resolution_status   TEXT NOT NULL DEFAULT 'pending'
                        CHECK (resolution_status IN ('pending', 'resolved', 'failed', 'retry')),
    failure_reason      TEXT,                       -- powód błędu (jeśli ULDK nie odpowiedział)

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Spatial index (GIST) — krytyczny dla ST_Intersects queries
CREATE INDEX idx_dzialki_geom ON silver.dzialki USING GIST (geom);

-- TERYT hierarchy lookups
CREATE INDEX idx_dzialki_gmina ON silver.dzialki (teryt_gmina);
CREATE INDEX idx_dzialki_obreb ON silver.dzialki (teryt_obreb);
CREATE INDEX idx_dzialki_status ON silver.dzialki (resolution_status) WHERE resolution_status != 'resolved';
```

### 2.2 `silver.ksiegi_wieczyste`

Powiązania Ksiąg Wieczystych z działkami.

```sql
CREATE TABLE silver.ksiegi_wieczyste (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    numer_kw        TEXT NOT NULL,                  -- np. 'WA1M/00012345/6'
    dzialka_id      UUID NOT NULL REFERENCES silver.dzialki(id),
    sad_rejonowy    TEXT,                           -- nazwa sądu
    is_verified     BOOLEAN NOT NULL DEFAULT FALSE, -- potwierdzone w EKW?
    verified_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_kw_numer ON silver.ksiegi_wieczyste (numer_kw);
CREATE INDEX idx_kw_dzialka ON silver.ksiegi_wieczyste (dzialka_id);
```

### 2.3 `silver.listing_parcels`

Junction table: surowe ogłoszenie (Bronze) → rozwiązana działka (Silver). Jedno ogłoszenie może dotyczyć wielu działek.

```sql
CREATE TABLE silver.listing_parcels (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    listing_id      UUID NOT NULL REFERENCES bronze.raw_listings(id),
    dzialka_id      UUID NOT NULL REFERENCES silver.dzialki(id),

    -- Jak pewni jesteśmy tego powiązania?
    match_confidence NUMERIC(3,2) NOT NULL DEFAULT 0.00
                     CHECK (match_confidence >= 0.00 AND match_confidence <= 1.00),
    match_method     TEXT NOT NULL                  -- 'teryt_exact' | 'kw_lookup' | 'address_fuzzy' | 'manual'
                     CHECK (match_method IN ('teryt_exact', 'kw_lookup', 'address_fuzzy', 'manual')),

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_listing_parcels_pair ON silver.listing_parcels (listing_id, dzialka_id);
CREATE INDEX idx_listing_parcels_dzialka ON silver.listing_parcels (dzialka_id);
```

---

## 3. Gold Layer — Wyniki Analizy (Delta & Leads)

Zasada: **Local-First GIS.** Plany zagospodarowania pobrane i przechowywane lokalnie. Analiza `ST_Intersects` wykonywana w bazie, nigdy przez WMS w locie.

### 3.1 `gold.planning_zones`

Lokalne kopie stref planistycznych (MPZP, POG, Studium) — wektorowe poligony.

```sql
CREATE TABLE gold.planning_zones (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Identyfikacja planu
    plan_type           TEXT NOT NULL               -- 'mpzp' | 'pog' | 'studium'
                        CHECK (plan_type IN ('mpzp', 'pog', 'studium')),
    plan_name           TEXT NOT NULL,              -- nazwa uchwały/planu
    uchwala_nr          TEXT,                       -- numer uchwały gminy
    teryt_gmina         CHAR(7) NOT NULL,           -- której gminy dotyczy

    -- Przeznaczenie terenu
    przeznaczenie       TEXT NOT NULL,              -- symbol: 'MN', 'MU', 'R', 'ZL', 'U', 'P' etc.
    przeznaczenie_opis  TEXT,                       -- opis słowny: 'zabudowa mieszkaniowa jednorodzinna'

    -- Geometria strefy (EPSG:2180)
    geom                GEOMETRY(MultiPolygon, 2180) NOT NULL,
    area_m2             NUMERIC(14,2)
                        GENERATED ALWAYS AS (ST_Area(geom)) STORED,

    -- Metadata źródła
    source_wfs_url      TEXT,                       -- WFS/GML z którego pobrano
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    plan_effective_date DATE,                       -- data wejścia w życie planu

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_planning_zones_geom ON gold.planning_zones USING GIST (geom);
CREATE INDEX idx_planning_zones_gmina ON gold.planning_zones (teryt_gmina);
CREATE INDEX idx_planning_zones_type ON gold.planning_zones (plan_type);
```

### 3.2 `gold.delta_results`

Wyniki analizy "Delta" — przecięcia działki z planami zagospodarowania. Odpowiada na pytanie: "Jaki % działki jest objęty nowym planem i jakie przeznaczenie ma ta część?"

```sql
CREATE TABLE gold.delta_results (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dzialka_id          UUID NOT NULL REFERENCES silver.dzialki(id),
    planning_zone_id    UUID NOT NULL REFERENCES gold.planning_zones(id),

    -- Wynik przestrzenny
    intersection_geom   GEOMETRY(MultiPolygon, 2180),   -- ST_Intersection result
    intersection_area_m2 NUMERIC(12,2) NOT NULL,
    coverage_pct        NUMERIC(5,2) NOT NULL            -- % powierzchni działki objęty planem
                        CHECK (coverage_pct >= 0.00 AND coverage_pct <= 100.00),

    -- Zmiana przeznaczenia (Delta)
    przeznaczenie_before TEXT,                           -- dotychczasowe (np. 'R' — rolne)
    przeznaczenie_after  TEXT NOT NULL,                  -- nowe wg planu (np. 'MN' — zabudowa)
    is_upgrade           BOOLEAN NOT NULL DEFAULT FALSE, -- czy zmiana jest "na plus" dla inwestora

    -- Scoring
    delta_score          NUMERIC(3,2) NOT NULL DEFAULT 0.00
                         CHECK (delta_score >= 0.00 AND delta_score <= 1.00),

    computed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_delta_dzialka ON gold.delta_results (dzialka_id);
CREATE INDEX idx_delta_zone ON gold.delta_results (planning_zone_id);
CREATE INDEX idx_delta_upgrade ON gold.delta_results (is_upgrade, delta_score DESC) WHERE is_upgrade;
```

### 3.3 `gold.investment_leads`

Finalne leady inwestycyjne — agregat z Delta + listing + confidence. To jest tabela konsumowana przez kokpit inwestorski.

```sql
CREATE TABLE gold.investment_leads (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dzialka_id          UUID NOT NULL REFERENCES silver.dzialki(id),
    listing_id          UUID REFERENCES bronze.raw_listings(id),  -- może być NULL (lead z samej analizy planów)

    -- Scoring końcowy
    confidence_score    NUMERIC(3,2) NOT NULL DEFAULT 0.00
                        CHECK (confidence_score >= 0.00 AND confidence_score <= 1.00),
    priority            TEXT NOT NULL DEFAULT 'medium'
                        CHECK (priority IN ('critical', 'high', 'medium', 'low')),

    -- Podsumowanie Delta
    max_coverage_pct    NUMERIC(5,2),                   -- najwyższy % pokrycia planem
    dominant_przeznaczenie TEXT,                         -- dominujące nowe przeznaczenie
    price_per_m2_zl     NUMERIC(10,2),                  -- cena za m² z ogłoszenia
    estimated_value_uplift_pct NUMERIC(7,2),             -- szacowany wzrost wartości (%)

    -- Evidence Chain (referencje)
    evidence_chain      JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- Format: [
    --   {"step": "source",     "ref": "bronze.raw_listings.id",     "url": "..."},
    --   {"step": "document",   "ref": "bronze.raw_documents.id",    "uri": "gs://..."},
    --   {"step": "parcel",     "ref": "silver.dzialki.id",          "teryt": "..."},
    --   {"step": "delta",      "ref": "gold.delta_results.id",      "coverage": 72.5}
    -- ]

    -- Status workflow
    status              TEXT NOT NULL DEFAULT 'new'
                        CHECK (status IN ('new', 'reviewed', 'shortlisted', 'rejected', 'invested')),
    reviewed_at         TIMESTAMPTZ,
    notes               TEXT,                           -- notatki analityka

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_leads_score ON gold.investment_leads (confidence_score DESC, priority);
CREATE INDEX idx_leads_status ON gold.investment_leads (status) WHERE status NOT IN ('rejected');
CREATE INDEX idx_leads_dzialka ON gold.investment_leads (dzialka_id);
```

---

## 4. Supporting Tables

### 4.1 `silver.dlq_parcels` (Dead Letter Queue)

Działki, których nie udało się rozwiązać przez ULDK — do powtórzenia.

```sql
CREATE TABLE silver.dlq_parcels (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    listing_id      UUID NOT NULL REFERENCES bronze.raw_listings(id),
    raw_teryt_input TEXT NOT NULL,                  -- co próbowaliśmy rozwiązać
    attempt_count   INTEGER NOT NULL DEFAULT 1,
    last_error      TEXT,
    next_retry_at   TIMESTAMPTZ NOT NULL DEFAULT (now() + INTERVAL '1 hour'),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_dlq_retry ON silver.dlq_parcels (next_retry_at) WHERE attempt_count < 5;
```

---

## 5. Backend Folder Structure

**Stack:** FastAPI + async SQLAlchemy + Alembic + GeoAlchemy2

Uzasadnienie: FastAPI jest async-native (kluczowe dla event-driven architecture), automatycznie generuje OpenAPI spec (kontrakt dla agentów), a Pydantic modele naturalnie wspierają `confidence_score` walidację.

```
backend/
├── alembic/                        # Migracje bazy danych
│   ├── alembic.ini
│   ├── env.py
│   └── versions/
│       └── 001_initial_medallion_schema.py
├── app/
│   ├── __init__.py
│   ├── main.py                     # FastAPI app factory
│   ├── config.py                   # pydantic-settings (DATABASE_URL, GCS_BUCKET, etc.)
│   ├── db/
│   │   ├── __init__.py
│   │   ├── session.py              # async SQLAlchemy engine + session factory
│   │   └── models/
│   │       ├── __init__.py
│   │       ├── bronze.py           # RawListing, RawDocument, ScrapeRun
│   │       ├── silver.py           # Dzialka, KsiegaWieczysta, ListingParcel, DlqParcel
│   │       └── gold.py             # PlanningZone, DeltaResult, InvestmentLead
│   ├── api/
│   │   ├── __init__.py
│   │   ├── deps.py                 # Dependency injection (get_db, get_current_user)
│   │   └── v1/
│   │       ├── __init__.py
│   │       ├── health.py           # /health, /readiness
│   │       ├── leads.py            # CRUD + filtering investment leads
│   │       └── parcels.py          # Działki lookup by TERYT, bbox, etc.
│   ├── services/
│   │   ├── __init__.py
│   │   ├── uldk.py                 # ULDK API client (async, z retry + DLQ)
│   │   ├── delta.py                # Spatial analysis engine (ST_Intersects logic)
│   │   └── evidence.py             # Evidence chain builder + GCS upload
│   ├── scrapers/
│   │   ├── __init__.py
│   │   ├── base.py                 # Abstract scraper + retry/proxy logic
│   │   ├── licytacje.py            # e-licytacje.komornik.pl
│   │   └── proxy_pool.py           # Rotating proxy manager
│   └── schemas/                    # Pydantic request/response models
│       ├── __init__.py
│       ├── listing.py
│       ├── parcel.py
│       └── lead.py
├── tests/
│   ├── conftest.py                 # Fixtures: test DB, sample geometries
│   ├── test_uldk.py
│   ├── test_delta.py
│   └── test_scrapers/
├── pyproject.toml
├── Dockerfile
└── docker-compose.dev.yml          # PostGIS 16 + pgAdmin (local dev)
```
