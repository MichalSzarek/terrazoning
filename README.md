# TerraZoning

System arbitrażu gruntowego — koreluje ogłoszenia licytacji komorniczych z danymi przestrzennymi (MPZP/POG) i wskazuje niedowartościowane działki budowlane.

Aktualny plan rolloutowy dla strategii `future_buildable`:
- [2026-04-10-future-buildable-full-rollout-checklist.md](/Users/michalszarek/worksapace/terrazoning/docs/plan/2026-04-10-future-buildable-full-rollout-checklist.md)

---

## Jak to działa

```
licytacje.komornik.pl (HTML/SSR)
        │
        ▼
  [KomornikCrawler]  ─── regex KW + obreb + parcel ──→  bronze.raw_listings
        │                 (śląskie + małopolskie, kat. LAND)
        ▼
  [GeoResolver] ─── ULDK GetParcelById      ──────────→  silver.dzialki
                └── ULDK GetParcelByIdOrNr              (geometria EPSG:2180)
                         nierozwiązane → silver.dlq_parcels
        │
        ▼
  [DeltaEngine] ─── ST_Intersects(dzialka, mpzp) ────→  gold.delta_results
                                                         gold.investment_leads
        │
        ▼
  [FastAPI] ──── GET /api/v1/leads ──────────────────→  GeoJSON EPSG:4326
        │
        ▼
  [Kokpit Inwestorski]  ──────────────────────────────→  MapLibre GL (mapa + sidebar)
```

### Warstwy danych (Medallion Architecture)

| Warstwa | Tabela | Zawartość |
|---|---|---|
| Bronze | `bronze.raw_listings` | Surowe ogłoszenia — HTML, URL, KW, numer działki, pewność ekstrakcji |
| Silver | `silver.dzialki` | Geometria działki w EPSG:2180, TERYT, `match_confidence` |
| Silver | `silver.dlq_parcels` | Nierozwiązane działki z harmonogramem ponowień |
| Gold | `gold.investment_leads` | Leady po ST_Intersects z MPZP, `confidence_score` |

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
# Oczekiwany status: terrazoning_db  running (healthy)
```

Baza jest gotowa gdy `Status = healthy`. Schemat (`bronze`, `silver`, `gold`, wszystkie tabele, indeksy GiST) tworzony jest automatycznie przez `init-scripts/01-init.sql` przy pierwszym starcie.

> **pgAdmin** (opcjonalnie): `docker compose --profile tools up -d pgadmin`
> Dostępny pod `http://localhost:5050` — login: `admin@terrazoning.local` / `admin`

---

### Krok 2 — Backend (FastAPI)

```bash
cd backend/

# Skopiuj zmienne środowiskowe
cp .env.example .env
# Domyślne wartości pasują do docker-compose — nie musisz nic zmieniać lokalnie
# `FUTURE_BUILDABILITY_ENABLED=true` włącza rollout future-buildable po stronie API

# Zainstaluj zależności
uv sync

# Uruchom serwer
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Sprawdź, czy backend działa:

```bash
curl http://localhost:8000/api/v1/health
# {"status":"ok","database":"connected","version":"0.1.0","timestamp":"..."}
```

Dokumentacja API (Swagger UI): **http://localhost:8000/docs**

### Backend przeciwko Cloud SQL (`maths`)

Jeśli chcesz uruchomić TerraZoning bez lokalnego Postgresa, backend obsługuje teraz
`DATABASE_URL` i ma gotowy workflow pod Cloud SQL Auth Proxy:

```bash
# Wymaga: gcloud auth application-default login
# oraz lokalnego repo maths-iac z aktualnym state dev/data
make cloudsql-health
make backend-cloudsql
```

Ten tryb:
- pobiera parametry instancji z `maths-iac/environments/dev/data`
- uruchamia `cloud-sql-proxy` na `127.0.0.1:6543`
- ustawia `DATABASE_URL=postgresql://admin:...@127.0.0.1:6543/terrazoning`
- startuje backend już przeciwko Cloud SQL

Jeśli chcesz własny URL, możesz też ustawić go ręcznie:

```bash
cd backend
DATABASE_URL=postgresql://admin:secret@127.0.0.1:6543/terrazoning \
  uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

---

### Krok 3 — Scraper (live, licytacje.komornik.pl)

```bash
cd scraper/

# Zainstaluj zależności scraper + backend jako editable package
uv sync

# Dry-run: pobierz i parsuj 1 stronę, bez zapisu do DB
uv run python run_live.py --dry-run --provinces slaskie --max-pages 1 --verbose

# Właściwy scrape: obie prowincje, 2 strony (≈40 ogłoszeń)
uv run python run_live.py --provinces slaskie malopolskie --max-pages 2

# Pełny sweep (3 strony × 2 prowincje ≈ 86 ogłoszeń grunty)
uv run python run_live.py --provinces slaskie malopolskie --max-pages 3
```

Oczekiwany wynik:

```
============================================================
LIVE SCRAPE COMPLETE
============================================================
  Listings found : 86
  Saved          : 77
  Skipped (dedup): 0
  Failed         : 1
  Duration       : 206s
============================================================
```

Przy kolejnym uruchomieniu `Skipped (dedup): 77` — deduplication SHA-256 działa.

> **Prowincje docelowe:** `slaskie` i `malopolskie`.
> Skraper filtruje po stronie serwera (`?mainCategory=REAL_ESTATE&province=śląskie&subCategory=LAND`)
> i odrzuca ogłoszenia spoza tych prowincji jako post-filtr.

---

### Krok 4 — Geo-Resolver (geometria działek)

GeoResolver pobiera geometrię z API ULDK (GUGiK) i zapisuje do `silver.dzialki`.

```bash
cd backend/

uv run python -m app.services.geo_resolver
```

Oczekiwany wynik:

```
============================================================
GEO RESOLVER COMPLETE
============================================================
  Total processed : 77
  Resolved        : 2
  Sent to DLQ     : 75
  Success rate    : 2.6%
  Duration        : 50s
============================================================
```

> **Dlaczego tak mała skuteczność?**
> API ULDK wymaga pełnego identyfikatora TERYT: `{gmina7}.{obreb4}.{numer}`.
> Portal licytacje.komornik.pl w ~95% ogłoszeń nie podaje nazwy obrębu w tekście.
> GeoResolver stosuje dwie strategie:
>
> | Strategia | Metoda | Wymaga |
> |---|---|---|
> | 1 | `GetParcelById` | pełny TERYT (gmina + obreb + numer) |
> | 2 | `GetParcelByIdOrNr` | nazwa obrębu + numer działki |
>
> Ogłoszenia bez obrębu (strategia 1 i 2 niedostępna) trafiają do `silver.dlq_parcels`
> z komunikatem `TERYT_INCOMPLETE` i harmonogramem ponowień (+1h, +4h, +24h, +72h).
>
> Wymaga połączenia z internetem (`uldk.gugik.gov.pl`).

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

cp .env.example .env
# `VITE_FUTURE_BUILDABILITY_ENABLED=true` pokazuje segment future-buildable w UI

npm run dev
```

Otwórz **http://localhost:5173**

Kokpit wyświetla mapę Polski z kolorowymi poligonami działek (im czerwieńszy, tym wyższy `confidence_score`) oraz sidebar z listą leadów i pełnym łańcuchem dowodowym.

---

## Weryfikacja danych w bazie

```bash
# Uruchom psql wewnątrz kontenera
docker exec -it terrazoning_db psql -U terrazoning -d terrazoning

-- Liczba rekordów w każdej warstwie
SELECT 'bronze.raw_listings'  AS tabela, COUNT(*) FROM bronze.raw_listings
UNION ALL
SELECT 'silver.dzialki',              COUNT(*) FROM silver.dzialki
UNION ALL
SELECT 'silver.dlq_parcels',          COUNT(*) FROM silver.dlq_parcels
UNION ALL
SELECT 'gold.planning_zones',         COUNT(*) FROM gold.planning_zones
UNION ALL
SELECT 'gold.investment_leads',       COUNT(*) FROM gold.investment_leads;

-- Rozwiązane działki z geometrią
SELECT identyfikator, teryt_gmina, numer_dzialki, round(area_m2, 2) AS area_m2, match_confidence
FROM silver.dzialki
ORDER BY created_at DESC;

-- DLQ: powody nierozwiązanych działek
SELECT last_error, COUNT(*)
FROM silver.dlq_parcels
GROUP BY last_error
ORDER BY count DESC;

-- Leady inwestycyjne
SELECT identyfikator, area_m2, confidence_score,
       ST_AsText(ST_Transform(d.geom, 4326)) AS geom_wgs84
FROM gold.investment_leads il
JOIN silver.dzialki d ON d.id = il.dzialka_id
ORDER BY confidence_score DESC
LIMIT 5;
```

---

## Ingestion stref MPZP

### Tryb testowy (syntetyczne strefy — do uruchomienia pipeline'u)

Ogólnopolski WFS GUGiK dla stref MPZP (`integracja.gugik.gov.pl`) został wycofany i jest niedostępny. Zastąpiono go usługą WMS-only (tylko wizualizacja). Aby przetestować cały pipeline bez prawdziwych danych MPZP, użyj skryptu seed:

```bash
cd backend/

# Tworzy syntetyczne strefy planistyczne wokół istniejących działek (bufor 300 m)
uv run python seed_test_zones.py

# Wyczyść i wygeneruj ponownie z innym buforem
uv run python seed_test_zones.py --clear --buffer-m 500 --verbose
```

### Tryb produkcyjny (prawdziwy WFS gminy)

Dane MPZP publikowane są na poziomie gminnym. Jeśli gmina udostępnia WFS:

```bash
cd backend/

uv run python run_wfs_sync.py \
  --wfs-url "https://<gis.gmina.pl>/wfs" \
  --layer-name "<nazwa_warstwy>" \
  --plan-type mpzp \
  --source-srid 2180 \
  --verbose
```

Lub programowo:

```python
from app.services.wfs_downloader import run_wfs_ingest
import asyncio

report = asyncio.run(run_wfs_ingest(
    wfs_url="https://<gis.gmina.pl>/wfs",
    layer_name="<warstwa_mpzp>",
    plan_type="mpzp",
    teryt_gmina="2416085",  # 7-cyfrowy kod TERYT gminy
    source_srid=2180,
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
# 1. Nowe ogłoszenia (live)
cd scraper && uv run python run_live.py --provinces slaskie malopolskie --max-pages 3

# 2. Rozwiązanie geometrii
cd backend && uv run python -m app.services.geo_resolver

# 3a. Załaduj strefy MPZP (testowo — syntetyczne):
cd backend && uv run python seed_test_zones.py

# 3b. Lub z prawdziwego WFS gminy:
#     cd backend && uv run python run_wfs_sync.py --wfs-url <URL> --layer-name <LAYER>

# 4. Analiza przestrzenna → nowe leady
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

### Scraper zwraca `0 listings found`

Portal zmienił strukturę URL lub blokuje ruch. Diagnostyka:
```bash
# Sprawdź, czy portal odpowiada
curl -L "https://licytacje.komornik.pl/wyszukiwarka/obwieszczenia-o-licytacji?mainCategory=REAL_ESTATE&province=%C5%9Bl%C4%85skie&subCategory=LAND"

# Dry-run z verbose — widać selektory, linki, body response
cd scraper && uv run python run_live.py --dry-run --provinces slaskie --max-pages 1 --verbose
```

### Scraper zwraca `Skipped (dedup): N`

To normalne — SHA-256 deduplication działa poprawnie. Ogłoszenie z tym samym URL i treścią nie zostanie zapisane dwukrotnie.

### GeoResolver: `resolved=0`, wszystko w DLQ

Większość ogłoszeń z portalu nie zawiera nazwy obrębu w tekście — bez niej ULDK nie może wyszukać działki. Sprawdź DLQ:

```bash
docker exec terrazoning_db psql -U terrazoning -d terrazoning \
  -c "SELECT last_error, COUNT(*) FROM silver.dlq_parcels GROUP BY last_error;"
```

Typowe komunikaty:
- `TERYT_INCOMPLETE` — scraped numer działki, brak nazwy obrębu w tekście strony
- `ULDK_NOT_FOUND` — obreb był, ale ULDK nie znalazł takiej działki (np. błędna ekstrakcja)
- `KW_RESOLUTION_UNSUPPORTED` — ogłoszenie ma tylko numer KW, bez działki (wymaga integracji z ekw.ms.gov.pl)

### Mapa jest pusta (brak poligonów)

Pipeline nie wygenerował jeszcze leadów. Sprawdź kolejno:
```bash
# Ile działek w silver?
docker exec terrazoning_db psql -U terrazoning -d terrazoning \
  -c "SELECT COUNT(*) FROM silver.dzialki;"

# Ile stref MPZP?
docker exec terrazoning_db psql -U terrazoning -d terrazoning \
  -c "SELECT COUNT(*) FROM gold.planning_zones;"

# Uruchom pipeline
cd scraper  && uv run python run_live.py --provinces slaskie malopolskie --max-pages 3
cd backend  && uv run python -m app.services.geo_resolver
cd backend  && uv run python seed_test_zones.py   # syntetyczne strefy MPZP
cd backend  && uv run python -m app.services.delta_engine
```

Następnie sprawdź: `curl http://localhost:8000/api/v1/leads | python3 -m json.tool`

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
│   │   └── services/
│   │       ├── uldk.py          Klient API ULDK — GetParcelById, GetParcelByIdOrNr
│   │       ├── geo_resolver.py  Bronze → Silver: 2 strategie ULDK + DLQ
│   │       ├── delta_engine.py  ST_Intersects(dzialka, mpzp) → gold.investment_leads
│   │       └── wfs_downloader.py  Ingestion stref MPZP z WFS GUGiK
│   ├── init-scripts/        01-init.sql — schemat tworzony przy starcie Dockera
│   └── docker-compose.yml   PostGIS 16 + pgAdmin
├── scraper/                 Ekstrakcja ogłoszeń licytacji
│   ├── run_live.py          Punkt wejścia — live scrape z CLI (--provinces, --max-pages, --dry-run)
│   └── scraper/
│       ├── komornik_crawler.py  KomornikCrawler — crawl licytacje.komornik.pl
│       └── extractors/
│           ├── kw.py        Regex KW + walidacja cyfry kontrolnej
│           └── parcel.py    Ekstrakcja numeru działki + obrębu (TERYT, keyword, bare)
├── frontend/                Kokpit Inwestorski (React + MapLibre)
│   └── src/
│       ├── components/map/      LeadsMap.tsx
│       ├── components/sidebar/  LeadList, LeadDetail
│       └── components/ui/       ConfidenceBadge, EvidenceChain
└── docs/
    ├── DB_SCHEMA.md         Specyfikacja tabel i kolumn
    └── TASK_BOARD.md        Backlog zadań z statusami
```
