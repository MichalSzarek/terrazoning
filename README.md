# TerraZoning

System arbitrażu gruntowego — koreluje ogłoszenia licytacji komorniczych z danymi przestrzennymi (MPZP/POG) i wskazuje niedowartościowane działki budowlane.

---

## Jak to działa

```
Licytacje komornicze (HTML)
        │
        ▼
   [Scraper]  ──── regex KW + TERYT ──→  bronze.raw_listings
        │
        ▼
  [GeoResolver] ── API ULDK ──────────→  silver.dzialki (geometria EPSG:2180)
        │
        ▼
  [DeltaEngine] ── ST_Intersects ─────→  gold.delta_results
        │                                gold.investment_leads
        ▼
  [FastAPI] ─── GET /api/v1/leads ────→  GeoJSON EPSG:4326
        │
        ▼
  [Kokpit Inwestorski]  ──────────────→  MapLibre GL (mapa + sidebar)
```

---

## Wymagania

| Narzędzie | Minimalna wersja |
|---|---|
| Docker + Docker Compose | 24+ |
| Python | 3.12+ |
| [uv](https://docs.astral.sh/uv/) | najnowsza |
| Node.js | 20+ |

---

## Uruchomienie krok po kroku

### Krok 1 — Baza danych (PostGIS)

```bash
cd backend/

# Uruchom PostgreSQL 16 + PostGIS 3.4
docker compose up -d db

# Sprawdź, czy baza jest zdrowa
docker compose ps
# Oczekiwany status: db  running (healthy)
```

Baza jest gotowa gdy `Status = healthy`. Schemat (schematy `bronze`, `silver`, `gold`, wszystkie tabele, indeksy GiST) tworzony jest automatycznie przez skrypt `init-scripts/01-init.sql` przy pierwszym starcie.

> **pgAdmin** (opcjonalnie): `docker compose --profile tools up -d pgadmin`
> Dostępny pod `http://localhost:5050` — login: `admin@terrazoning.local` / `admin`

---

### Krok 2 — Backend (FastAPI)

```bash
cd backend/

# Skopiuj zmienne środowiskowe
cp .env.example .env
# Domyślne wartości pasują do docker-compose — nie musisz nic zmieniać lokalnie

# Zainstaluj zależności
uv sync

# Uruchom serwer
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Sprawdź, czy backend działa:

```bash
curl http://localhost:8000/api/v1/health
# Oczekiwana odpowiedź:
# {"status":"ok","database":"connected","version":"0.1.0","timestamp":"..."}
```

Dokumentacja API (Swagger UI): **http://localhost:8000/docs**

---

### Krok 3 — Scraper (pierwsze dane)

```bash
cd scraper/

# Skopiuj zmienne środowiskowe
cp .env.example .env

# Zainstaluj zależności (instaluje też backend jako editable package)
uv sync

# Uruchom scraper — załaduje przykładowe ogłoszenia do bronze.raw_listings
uv run python -m scraper.main
```

Oczekiwany wynik:

```
10:00:01 INFO  scraper.main | Created scrape_run id=<uuid>
10:00:01 INFO  scraper.main | Saved listing id=<uuid> kw=WA1M/00012345/2 confidence=0.80
...
============================================================
SCRAPE COMPLETE — Listings found: 2  Saved: 2  Skipped: 0
============================================================
```

Uruchomienie po raz drugi da `Skipped (dedup): 2` — deduplication SHA-256 działa.

---

### Krok 4 — Geo-Resolver (geometria działek)

GeoResolver pobiera geometrię z API ULDK (GUGiK) i zapisuje do `silver.dzialki`.

```bash
cd backend/

uv run python -m app.services.geo_resolver
```

> Wymaga połączenia z internetem (API ULDK: `uldk.gugik.gov.pl`).
> Działki nierozwiązane trafiają do `silver.dlq_parcels` z harmonogramem ponowień.

---

### Krok 5 — Delta Engine (analiza przestrzenna)

Delta Engine wykonuje `ST_Intersects` między działkami a strefami planistycznymi
i tworzy leady inwestycyjne w `gold.investment_leads`.

```bash
cd backend/

uv run python -m app.services.delta_engine
```

> **Uwaga:** `gold.planning_zones` musi zawierać dane (strefy MPZP).
> Bez stref engine nie wygeneruje wyników — patrz sekcja [Ingestion stref MPZP](#ingestion-stref-mpzp) poniżej.

---

### Krok 6 — Frontend (Kokpit Inwestorski)

```bash
cd frontend/

npm install

npm run dev
```

Otwórz **http://localhost:5173**

Kokpit wyświetla mapę Polski z kolorowymi poligonami działek (im czerwieńszy, tym wyższy `confidence_score`) oraz sidebar z listą leadów i pełnym łańcuchem dowodowym.

---

## Weryfikacja danych w bazie

```bash
# Uruchom psql wewnątrz kontenera
docker exec -it terrazoning_db psql -U terrazoning -d terrazoning

# Sprawdź liczbę rekordów w każdej warstwie
SELECT 'bronze.raw_listings'  AS tabela, COUNT(*) FROM bronze.raw_listings
UNION ALL
SELECT 'silver.dzialki',              COUNT(*) FROM silver.dzialki
UNION ALL
SELECT 'silver.dlq_parcels',          COUNT(*) FROM silver.dlq_parcels
UNION ALL
SELECT 'gold.planning_zones',         COUNT(*) FROM gold.planning_zones
UNION ALL
SELECT 'gold.investment_leads',       COUNT(*) FROM gold.investment_leads;

# Podejrzyj leady z geometrią w WGS84
SELECT identyfikator, area_m2, confidence_score,
       ST_AsText(ST_Transform(d.geom, 4326)) AS geom_wgs84
FROM gold.investment_leads il
JOIN silver.dzialki d ON d.id = il.dzialka_id
ORDER BY confidence_score DESC
LIMIT 5;
```

---

## Ingestion stref MPZP

Aby Delta Engine miał z czym porównywać działki, załaduj strefy planistyczne:

```bash
cd backend/

# Przykład: załaduj strefy MPZP dla wybranej gminy
uv run python -m app.services.wfs_downloader
```

Lub programowo w Python:

```python
from app.services.wfs_downloader import run_wfs_ingest
import asyncio

report = asyncio.run(run_wfs_ingest(
    wfs_url="https://integracja.gugik.gov.pl/cgi-bin/KrajowaIntegracjaUzytkowaniaTerenu",
    layer_name="app:ump_strefy_przeznaczenia",
    plan_type="mpzp",
    teryt_gmina="1412011",   # 7-cyfrowy kod TERYT gminy
    source_srid=4326,
    cql_filter="gmina_teryt='1412011'",
))
print(f"Załadowano {report.features_upserted} stref")
```

---

## Codzienna praca z systemem

### Uruchamianie wszystkiego naraz

Terminal 1 — baza:
```bash
cd backend && docker compose up -d db
```

Terminal 2 — backend:
```bash
cd backend && uv run uvicorn app.main:app --reload --port 8000
```

Terminal 3 — frontend:
```bash
cd frontend && npm run dev
```

### Ponowne uruchomienie pipeline'u

```bash
# 1. Nowe ogłoszenia
cd scraper && uv run python -m scraper.main

# 2. Rozwiązanie geometrii
cd backend && uv run python -m app.services.geo_resolver

# 3. Analiza przestrzenna → nowe leady
cd backend && uv run python -m app.services.delta_engine
```

Odśwież stronę — mapa zaktualizuje się automatycznie (TanStack Query, staleTime=30s).

### Filtrowanie leadów przez API

```bash
# Leady z confidence ≥ 0.9 (prime targets)
curl "http://localhost:8000/api/v1/leads?min_score=0.9&limit=20"

# Wszystkie leady z liczbą totalną
curl "http://localhost:8000/api/v1/leads?min_score=0.7&include_count=true"
```

---

## Zatrzymanie środowiska

```bash
# Zatrzymaj backend i frontend: Ctrl+C w terminalach

# Zatrzymaj bazę (zachowuje dane w volume)
cd backend && docker compose down

# Zatrzymaj bazę + usuń dane (czysty reset)
cd backend && docker compose down -v
```

---

## Troubleshooting

### `connection refused` na porcie 5432

Baza jeszcze się nie uruchomiła. Sprawdź status:
```bash
docker compose ps
# Poczekaj aż Status zmieni się na "healthy"
```

### `GET /api/v1/health` zwraca `database: unreachable`

Sprawdź, czy `.env` w `backend/` istnieje i ma poprawne dane:
```bash
cat backend/.env
# DB_HOST=localhost, DB_PORT=5432, DB_USER=terrazoning, DB_PASSWORD=terrazoning
```

### Mapa jest pusta (brak poligonów)

Pipeline nie wygenerował jeszcze leadów. Uruchom kolejno:
```bash
cd scraper  && uv run python -m scraper.main
cd backend  && uv run python -m app.services.geo_resolver
cd backend  && uv run python -m app.services.delta_engine
```
Następnie sprawdź: `curl http://localhost:8000/api/v1/leads | python3 -m json.tool`

### Scraper zwraca `Skipped (dedup): N`

To normalne — SHA-256 deduplication działa poprawnie. Ogłoszenie z tym samym URL i treścią nie zostanie zapisane dwukrotnie.

### Frontend nie może połączyć się z backendem (CORS)

Vite proxy w `vite.config.ts` kieruje `/api/*` → `localhost:8000`. Upewnij się, że backend działa na porcie **8000** i frontend na **5173**.

---

## Struktura projektu

```
terrazoning/
├── backend/                 FastAPI + SQLAlchemy + PostGIS
│   ├── app/
│   │   ├── api/v1/          Endpointy REST (health, leads)
│   │   ├── models/          ORM: bronze / silver / gold
│   │   ├── schemas/         Pydantic: GeoJSON, LeadProperties
│   │   └── services/        uldk.py, geo_resolver.py, delta_engine.py, wfs_downloader.py
│   ├── alembic/             Migracje bazy
│   ├── init-scripts/        01-init.sql — schemat tworzony przy starcie Dockera
│   └── docker-compose.yml   PostGIS 16 + pgAdmin
├── scraper/                 Ekstrakcja ogłoszeń licytacji
│   └── scraper/
│       ├── main.py          LicytacjeScraper
│       └── extractors/      kw.py (regex KW + check digit), parcel.py
├── frontend/                Kokpit Inwestorski (React + MapLibre)
│   └── src/
│       ├── components/map/  LeadsMap.tsx
│       ├── components/sidebar/ LeadList, LeadDetail
│       └── components/ui/   ConfidenceBadge, EvidenceChain
└── docs/
    ├── DB_SCHEMA.md         Specyfikacja tabel i kolumn
    └── TASK_BOARD.md        Backlog zadań z statusami
```
