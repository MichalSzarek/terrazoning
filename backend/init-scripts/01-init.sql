-- =============================================================================
-- TerraZoning — Initial Database Schema
-- Medallion Architecture: Bronze (raw) → Silver (normalized) → Gold (analyzed)
-- Canonical CRS: EPSG:2180 (PUWG 1992) for ALL spatial data
-- =============================================================================

-- ---------------------------------------------------------------------------
-- EXTENSIONS
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS pg_trgm;       -- trigram indexes for fuzzy text search
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";   -- uuid_generate_v4()

-- ---------------------------------------------------------------------------
-- SCHEMAS
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- =============================================================================
-- BRONZE LAYER — Surowe Dane (Raw Ingestion)
-- Zasada: Trust No One. Dane trafiają tu bez transformacji geometrii.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- bronze.scrape_runs
-- Meta-dane każdego uruchomienia scrapera (job tracking, audit trail)
-- ---------------------------------------------------------------------------
CREATE TABLE bronze.scrape_runs (
    id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_name     TEXT        NOT NULL,           -- np. 'e-licytacje', 'otodom', 'geoportal_pog'
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    status          TEXT        NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'completed', 'failed', 'partial')),
    records_found   INTEGER     DEFAULT 0,
    records_saved   INTEGER     DEFAULT 0,
    proxy_used      TEXT,                           -- identyfikator proxy (rotacja)
    error_message   TEXT,
    metadata        JSONB       DEFAULT '{}'::jsonb, -- dowolne parametry job-a
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE bronze.scrape_runs IS
    'Audit log for every scraper run. One row per invocation. '
    'Never delete — needed for Evidence Chain traceability.';

CREATE INDEX idx_scrape_runs_source
    ON bronze.scrape_runs (source_name, started_at DESC);

-- ---------------------------------------------------------------------------
-- bronze.raw_listings
-- Surowe ogłoszenia i licytacje — tekst wyciągnięty ze stron, bez normalizacji.
-- ---------------------------------------------------------------------------
CREATE TABLE bronze.raw_listings (
    id                  UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    scrape_run_id       UUID        NOT NULL REFERENCES bronze.scrape_runs(id),
    source_url          TEXT        NOT NULL,
    source_type         TEXT        NOT NULL
                        CHECK (source_type IN ('licytacja_komornicza', 'ogloszenie', 'przetarg', 'inne')),
    title               TEXT,
    raw_text            TEXT,
    price_zl            NUMERIC(14,2),
    area_m2             NUMERIC(12,2),
    auction_date        DATE,

    -- Surowe pola wyciągnięte przez regex/NLP (mogą być NULL lub błędne)
    raw_numer_dzialki   TEXT,                       -- np. '123/4' — jeszcze nie zwalidowany
    raw_obreb           TEXT,                       -- np. 'Wola' — jeszcze nie znormalizowany
    raw_gmina           TEXT,
    raw_powiat          TEXT,
    raw_wojewodztwo     TEXT,
    raw_kw              TEXT,                       -- nr Księgi Wieczystej (jeśli znaleziony)

    -- Evidence Chain pointers
    raw_html_ref        TEXT,                       -- ścieżka do bronze.raw_documents lub GCS URI
    raw_pdf_ref         TEXT,

    dedup_hash          TEXT        NOT NULL,       -- SHA-256 z (source_url + raw_text)
    is_processed        BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE bronze.raw_listings IS
    'Raw auction/listing records. Never mutate scraped fields after insert — '
    'append-only for audit integrity. Update only is_processed and updated_at.';
COMMENT ON COLUMN bronze.raw_listings.dedup_hash IS
    'SHA-256(source_url || raw_text). Prevents re-inserting the same listing '
    'across multiple scrape runs.';

CREATE UNIQUE INDEX idx_raw_listings_dedup
    ON bronze.raw_listings (dedup_hash);
CREATE INDEX idx_raw_listings_source_type
    ON bronze.raw_listings (source_type, created_at DESC);
CREATE INDEX idx_raw_listings_unprocessed
    ON bronze.raw_listings (is_processed) WHERE NOT is_processed;
CREATE INDEX idx_raw_listings_scrape_run
    ON bronze.raw_listings (scrape_run_id);

-- Trigram indexes for fuzzy address matching (NLP pipeline uses these)
CREATE INDEX idx_raw_listings_trgm_obreb
    ON bronze.raw_listings USING GIN (raw_obreb gin_trgm_ops);
CREATE INDEX idx_raw_listings_trgm_gmina
    ON bronze.raw_listings USING GIN (raw_gmina gin_trgm_ops);

-- ---------------------------------------------------------------------------
-- bronze.raw_documents
-- Archiwum oryginalnych plików HTML/PDF — fundament Evidence Chain.
-- ---------------------------------------------------------------------------
CREATE TABLE bronze.raw_documents (
    id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    listing_id      UUID        REFERENCES bronze.raw_listings(id),
    document_type   TEXT        NOT NULL
                    CHECK (document_type IN ('html', 'pdf', 'screenshot', 'json')),
    storage_uri     TEXT        NOT NULL,           -- GCS URI: gs://terrazoning-evidence/...
    file_size_bytes BIGINT,
    content_hash    TEXT        NOT NULL,           -- SHA-256 zawartości pliku
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE bronze.raw_documents IS
    'Immutable archive of original HTML/PDF/screenshot files. '
    'Every row is the "proof" link in the Evidence Chain. Never delete, never update.';
COMMENT ON COLUMN bronze.raw_documents.storage_uri IS
    'GCS path convention: gs://terrazoning-evidence/{source_type}/{YYYY-MM-DD}/{listing_id}.{ext}';

CREATE INDEX idx_raw_documents_listing
    ON bronze.raw_documents (listing_id);
CREATE UNIQUE INDEX idx_raw_documents_hash
    ON bronze.raw_documents (content_hash);

-- =============================================================================
-- SILVER LAYER — Znormalizowane Dane Przestrzenne
-- Zasada: EPSG:2180 is King. TERYT jako klucz identyfikacji przestrzennej.
-- Każdy rekord ma match_confidence.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- silver.dzialki
-- Znormalizowane działki z geometrią pobraną z ULDK.
-- Geometria przechowywana w EPSG:2180 (PUWG 1992) — BEZ WYJĄTKU.
-- ---------------------------------------------------------------------------
CREATE TABLE silver.dzialki (
    id                  UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Identyfikacja TERYT (hierarchia administracyjna PL)
    teryt_wojewodztwo   CHAR(2)         NOT NULL,   -- kod TERYT województwa (2 cyfry)
    teryt_powiat        CHAR(4)         NOT NULL,   -- kod TERYT powiatu (4 cyfry)
    teryt_gmina         CHAR(7)         NOT NULL,   -- kod TERYT gminy (7 cyfr)
    teryt_obreb         CHAR(9)         NOT NULL,   -- kod TERYT obrębu (9 cyfr)
    numer_dzialki       TEXT            NOT NULL,   -- np. '123/4', '45/AB'
    identyfikator       TEXT            NOT NULL UNIQUE, -- teryt_obreb || '.' || numer_dzialki

    -- Geometria — WYŁĄCZNIE EPSG:2180 (PUWG 1992)
    -- GIS Specialist NOTE: ST_MakeValid() MUST be applied before INSERT.
    -- Geometry type: MultiPolygon because działki can be non-contiguous (paski, enklawy).
    geom                GEOMETRY(MultiPolygon, 2180) NOT NULL,
    area_m2             NUMERIC(12,2)
                        GENERATED ALWAYS AS (ST_Area(geom)) STORED,

    -- Metadata z ULDK API
    uldk_response_date  TIMESTAMPTZ,               -- kiedy pobrano z ULDK
    uldk_raw_response   JSONB,                     -- surowa odpowiedź API (dla debugowania)

    -- Confidence & Resolution Status
    match_confidence    NUMERIC(3,2)    NOT NULL DEFAULT 0.00
                        CHECK (match_confidence >= 0.00 AND match_confidence <= 1.00),
    resolution_status   TEXT            NOT NULL DEFAULT 'pending'
                        CHECK (resolution_status IN ('pending', 'resolved', 'failed', 'retry')),
    failure_reason      TEXT,

    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now()
);

COMMENT ON TABLE silver.dzialki IS
    'Normalized parcels with ULDK-resolved geometries. All geometry in EPSG:2180. '
    'TERYT identyfikator is the canonical spatial foreign key for the entire system.';
COMMENT ON COLUMN silver.dzialki.geom IS
    'CRITICAL: EPSG:2180 (PUWG 1992) only. Apply ST_MakeValid() before insert. '
    'ST_Transform to 4326 only at API response boundary.';
COMMENT ON COLUMN silver.dzialki.area_m2 IS
    'Computed from ST_Area(geom) in EPSG:2180 — result is in square meters. '
    'Cross-check against ULDK reported area: flag if discrepancy > 5%, reject if > 15%.';
COMMENT ON COLUMN silver.dzialki.identyfikator IS
    'Format: {teryt_obreb}.{numer_dzialki}, e.g. 141201_1.0001.123/4. '
    'Matches GUGiK cadastral identifier format.';

-- Spatial index (GIST) — MANDATORY, enables ST_Intersects performance
CREATE INDEX idx_dzialki_geom
    ON silver.dzialki USING GIST (geom);

-- TERYT hierarchy lookups
CREATE INDEX idx_dzialki_gmina
    ON silver.dzialki (teryt_gmina);
CREATE INDEX idx_dzialki_obreb
    ON silver.dzialki (teryt_obreb);

-- Partial index for pending/failed resolutions (ULDK retry queue)
CREATE INDEX idx_dzialki_status
    ON silver.dzialki (resolution_status) WHERE resolution_status != 'resolved';

-- Trigram index for numer_dzialki fuzzy matching
CREATE INDEX idx_dzialki_trgm_numer
    ON silver.dzialki USING GIN (numer_dzialki gin_trgm_ops);

-- ---------------------------------------------------------------------------
-- silver.ksiegi_wieczyste
-- Powiązania Ksiąg Wieczystych z działkami.
-- ---------------------------------------------------------------------------
CREATE TABLE silver.ksiegi_wieczyste (
    id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    numer_kw        TEXT        NOT NULL,           -- np. 'WA1M/00012345/6'
    dzialka_id      UUID        NOT NULL REFERENCES silver.dzialki(id),
    sad_rejonowy    TEXT,                           -- nazwa sądu rejonowego
    is_verified     BOOLEAN     NOT NULL DEFAULT FALSE,
    verified_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE silver.ksiegi_wieczyste IS
    'Land registry (KW) to parcel mapping. One KW can span multiple działki. '
    'is_verified=true means cross-checked with EKW (Elektroniczne Księgi Wieczyste).';
COMMENT ON COLUMN silver.ksiegi_wieczyste.numer_kw IS
    'Format: {kod_sadu}/{numer_ks}/{cyfra_kontrolna}, e.g. WA1M/00012345/6';

CREATE INDEX idx_kw_numer ON silver.ksiegi_wieczyste (numer_kw);
CREATE INDEX idx_kw_dzialka ON silver.ksiegi_wieczyste (dzialka_id);

-- Trigram on KW number for loose matching (partial matches from scraped text)
CREATE INDEX idx_kw_trgm_numer
    ON silver.ksiegi_wieczyste USING GIN (numer_kw gin_trgm_ops);

-- ---------------------------------------------------------------------------
-- silver.listing_parcels
-- Junction: raw listing (Bronze) → resolved działka (Silver).
-- Jedno ogłoszenie może dotyczyć wielu działek.
-- ---------------------------------------------------------------------------
CREATE TABLE silver.listing_parcels (
    id               UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    listing_id       UUID        NOT NULL REFERENCES bronze.raw_listings(id),
    dzialka_id       UUID        NOT NULL REFERENCES silver.dzialki(id),
    match_confidence NUMERIC(3,2) NOT NULL DEFAULT 0.00
                     CHECK (match_confidence >= 0.00 AND match_confidence <= 1.00),
    match_method     TEXT        NOT NULL
                     CHECK (match_method IN ('teryt_exact', 'kw_lookup', 'address_fuzzy', 'manual')),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE silver.listing_parcels IS
    'Junction table linking raw listings to resolved parcels. '
    'match_method records HOW the linkage was established — critical for audit.';

CREATE UNIQUE INDEX idx_listing_parcels_pair
    ON silver.listing_parcels (listing_id, dzialka_id);
CREATE INDEX idx_listing_parcels_listing
    ON silver.listing_parcels (listing_id);
CREATE INDEX idx_listing_parcels_dzialka
    ON silver.listing_parcels (dzialka_id);

-- ---------------------------------------------------------------------------
-- silver.dlq_parcels — Dead Letter Queue
-- Działki, których nie udało się rozwiązać przez ULDK. Max 5 prób.
-- ---------------------------------------------------------------------------
CREATE TABLE silver.dlq_parcels (
    id               UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    listing_id       UUID        NOT NULL REFERENCES bronze.raw_listings(id),
    raw_teryt_input  TEXT        NOT NULL,          -- surowy input, który próbowaliśmy rozwiązać
    attempt_count    INTEGER     NOT NULL DEFAULT 1
                     CHECK (attempt_count >= 1 AND attempt_count <= 5),
    last_error       TEXT,
    next_retry_at    TIMESTAMPTZ NOT NULL DEFAULT (now() + INTERVAL '1 hour'),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE silver.dlq_parcels IS
    'Dead Letter Queue for ULDK resolution failures. '
    'Retry schedule: 1h → 4h → 24h → 72h → manual. Max 5 attempts. '
    'Records with attempt_count=5 require human review.';

-- Only index retryable rows (attempt_count < 5)
CREATE INDEX idx_dlq_retry
    ON silver.dlq_parcels (next_retry_at) WHERE attempt_count < 5;
CREATE INDEX idx_dlq_listing
    ON silver.dlq_parcels (listing_id);

-- =============================================================================
-- GOLD LAYER — Wyniki Analizy (Delta & Investment Leads)
-- Zasada: Local-First GIS. Plany zagospodarowania pobrane i przechowywane lokalnie.
-- ST_Intersects wykonywany w bazie — NIGDY przez WMS w locie.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- gold.planning_zones
-- Lokalne kopie stref planistycznych (MPZP, POG, Studium) — wektory.
-- ---------------------------------------------------------------------------
CREATE TABLE gold.planning_zones (
    id                  UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Identyfikacja planu
    plan_type           TEXT            NOT NULL
                        CHECK (plan_type IN ('mpzp', 'pog', 'studium')),
    plan_name           TEXT            NOT NULL,   -- nazwa uchwały/dokumentu
    uchwala_nr          TEXT,                       -- numer uchwały rady gminy
    teryt_gmina         CHAR(7)         NOT NULL,   -- której gminy dotyczy

    -- Przeznaczenie terenu (klasyfikacja MPZP)
    przeznaczenie       TEXT            NOT NULL,   -- symbol: 'MN', 'MW', 'U', 'R', 'ZL', 'KD' etc.
    przeznaczenie_opis  TEXT,                       -- opis słowny

    -- Geometria strefy (EPSG:2180 — obowiązkowo)
    -- GIS Specialist NOTE: Apply ST_MakeValid() + ST_Snap() before INSERT.
    geom                GEOMETRY(MultiPolygon, 2180) NOT NULL,
    area_m2             NUMERIC(14,2)
                        GENERATED ALWAYS AS (ST_Area(geom)) STORED,

    -- Metadata źródła
    source_wfs_url      TEXT,                       -- WFS/GML endpoint użyty do ingestionu
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    plan_effective_date DATE,

    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now()
);

COMMENT ON TABLE gold.planning_zones IS
    'Local copy of spatial planning zones (MPZP/POG/Studium). '
    'Never query government WMS in production — ingest here, query locally. '
    'This table IS the local-first GIS cache described in the Architecture Commandments.';
COMMENT ON COLUMN gold.planning_zones.przeznaczenie IS
    'MPZP zone code. Key values: MN (residential single), MW (multi-family), '
    'U (commercial), R (agricultural), ZL (forest — near-impossible to rezone), '
    'KD (roads — expropriation risk). See DB_SCHEMA.md for full symbol table.';
COMMENT ON COLUMN gold.planning_zones.geom IS
    'EPSG:2180 only. Source WFS may return EPSG:4326 or PUWG 1965 — '
    'always ST_Transform to 2180 before insert.';

-- Spatial index — enables fast ST_Intersects against silver.dzialki
CREATE INDEX idx_planning_zones_geom
    ON gold.planning_zones USING GIST (geom);
CREATE INDEX idx_planning_zones_gmina
    ON gold.planning_zones (teryt_gmina);
CREATE INDEX idx_planning_zones_type
    ON gold.planning_zones (plan_type);
CREATE INDEX idx_planning_zones_przeznaczenie
    ON gold.planning_zones (przeznaczenie);

-- ---------------------------------------------------------------------------
-- gold.delta_results
-- Wyniki analizy "Delta" — przecięcia działki z planami zagospodarowania.
-- Odpowiada: "Jaki % działki objęty nowym planem i jakie przeznaczenie?"
-- ---------------------------------------------------------------------------
CREATE TABLE gold.delta_results (
    id                      UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
    dzialka_id              UUID            NOT NULL REFERENCES silver.dzialki(id),
    planning_zone_id        UUID            NOT NULL REFERENCES gold.planning_zones(id),

    -- Wynik przestrzenny (wynik ST_Intersection)
    intersection_geom       GEOMETRY(MultiPolygon, 2180),   -- może być NULL dla granicznych przypadków
    intersection_area_m2    NUMERIC(12,2)   NOT NULL,
    coverage_pct            NUMERIC(5,2)    NOT NULL
                            CHECK (coverage_pct >= 0.00 AND coverage_pct <= 100.00),

    -- Zmiana przeznaczenia (Delta)
    przeznaczenie_before    TEXT,           -- dotychczasowe (np. 'R' — rolne, z Studium/starszego MPZP)
    przeznaczenie_after     TEXT            NOT NULL,   -- nowe wg aktualnego planu
    is_upgrade              BOOLEAN         NOT NULL DEFAULT FALSE,

    -- Scoring delta (wkład do gold.investment_leads.confidence_score)
    delta_score             NUMERIC(3,2)    NOT NULL DEFAULT 0.00
                            CHECK (delta_score >= 0.00 AND delta_score <= 1.00),

    computed_at             TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT now()
);

COMMENT ON TABLE gold.delta_results IS
    'Results of ST_Intersects(dzialka, planning_zone) analysis. '
    'One row per (działka, plan_zone) pair. Multiple rows per działka are normal '
    '— a parcel may overlap multiple zones (e.g. 60% MN + 40% ZP). '
    'coverage_pct is computed as: ST_Area(intersection) / silver.dzialki.area_m2 * 100.';
COMMENT ON COLUMN gold.delta_results.intersection_geom IS
    'Result of ST_Intersection(dzialka.geom, planning_zone.geom). '
    'GIS Specialist: apply ST_MakeValid() and sliver detection (area < 0.5 m²) '
    'before storing. Null allowed for is_upgrade=false rows where we only need coverage_pct.';

CREATE INDEX idx_delta_dzialka
    ON gold.delta_results (dzialka_id);
CREATE INDEX idx_delta_zone
    ON gold.delta_results (planning_zone_id);

-- Partial index — hot path for investor dashboard (only positive findings)
CREATE INDEX idx_delta_upgrade
    ON gold.delta_results (is_upgrade, delta_score DESC) WHERE is_upgrade;

-- Spatial index on intersection geometry (for map viewport queries on overlay)
CREATE INDEX idx_delta_intersection_geom
    ON gold.delta_results USING GIST (intersection_geom)
    WHERE intersection_geom IS NOT NULL;

-- ---------------------------------------------------------------------------
-- gold.investment_leads
-- Finalne leady inwestycyjne — agregat Delta + listing + confidence.
-- To jest tabela konsumowana bezpośrednio przez kokpit inwestorski.
-- ---------------------------------------------------------------------------
CREATE TABLE gold.investment_leads (
    id                          UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    dzialka_id                  UUID        NOT NULL REFERENCES silver.dzialki(id),
    listing_id                  UUID        REFERENCES bronze.raw_listings(id),    -- NULL jeśli lead z analizy planów

    -- Scoring końcowy
    confidence_score            NUMERIC(3,2) NOT NULL DEFAULT 0.00
                                CHECK (confidence_score >= 0.00 AND confidence_score <= 1.00),
    priority                    TEXT        NOT NULL DEFAULT 'medium'
                                CHECK (priority IN ('critical', 'high', 'medium', 'low')),

    -- Podsumowanie Delta
    max_coverage_pct            NUMERIC(5,2),
    dominant_przeznaczenie      TEXT,
    price_per_m2_zl             NUMERIC(10,2),
    estimated_value_uplift_pct  NUMERIC(7,2),

    -- Evidence Chain — pełny łańcuch dowodowy od źródła do wyniku
    -- Format: JSON array ze steps: source → document → parcel → delta
    evidence_chain              JSONB       NOT NULL DEFAULT '[]'::jsonb,

    -- Workflow status
    status                      TEXT        NOT NULL DEFAULT 'new'
                                CHECK (status IN ('new', 'reviewed', 'shortlisted', 'rejected', 'invested')),
    reviewed_at                 TIMESTAMPTZ,
    notes                       TEXT,

    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE gold.investment_leads IS
    'Final investment leads consumed by the investor dashboard. '
    'confidence_score aggregates: match_confidence (Silver) + delta_score (Gold). '
    'evidence_chain JSON array must link all steps: source URL → raw document → '
    'działka TERYT → delta result. Incomplete evidence_chain = incomplete lead.';
COMMENT ON COLUMN gold.investment_leads.evidence_chain IS
    'JSON array format: '
    '[{"step":"source","ref":"bronze.raw_listings.id","url":"..."}, '
    ' {"step":"document","ref":"bronze.raw_documents.id","uri":"gs://..."}, '
    ' {"step":"parcel","ref":"silver.dzialki.id","teryt":"..."}, '
    ' {"step":"delta","ref":"gold.delta_results.id","coverage":72.5}]';

CREATE INDEX idx_leads_score
    ON gold.investment_leads (confidence_score DESC, priority);
CREATE INDEX idx_leads_status
    ON gold.investment_leads (status) WHERE status NOT IN ('rejected');
CREATE INDEX idx_leads_dzialka
    ON gold.investment_leads (dzialka_id);
CREATE INDEX idx_leads_listing
    ON gold.investment_leads (listing_id) WHERE listing_id IS NOT NULL;

-- =============================================================================
-- VERIFICATION QUERIES
-- Run after init to confirm schema integrity.
-- =============================================================================

-- Verify all 3 schemas exist
DO $$
DECLARE
    schema_count INT;
BEGIN
    SELECT COUNT(*) INTO schema_count
    FROM information_schema.schemata
    WHERE schema_name IN ('bronze', 'silver', 'gold');

    IF schema_count != 3 THEN
        RAISE EXCEPTION 'Schema creation failed: expected 3 schemas, found %', schema_count;
    END IF;
END $$;

-- Verify PostGIS EPSG:2180 is registered
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM spatial_ref_sys WHERE srid = 2180
    ) THEN
        RAISE EXCEPTION 'EPSG:2180 not found in spatial_ref_sys. PostGIS installation may be incomplete.';
    END IF;
END $$;

-- Verify geometry column SRIDs
DO $$
DECLARE
    srid_violations INT;
BEGIN
    SELECT COUNT(*) INTO srid_violations
    FROM geometry_columns
    WHERE f_table_schema IN ('silver', 'gold')
      AND srid != 2180;

    IF srid_violations > 0 THEN
        RAISE EXCEPTION
            'SRID violation: % geometry columns not in EPSG:2180. '
            'Check silver.dzialki, gold.planning_zones, gold.delta_results.',
            srid_violations;
    END IF;
END $$;
