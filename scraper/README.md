# TerraZoning Scraper

Bronze Layer ingestion pipeline for licytacje komornicze (bailiff auctions).

## Prerequisites

- Docker PostGIS running: `cd ../backend && docker compose up -d db`
- Python 3.12+
- [uv](https://docs.astral.sh/uv/) installed

## Setup

```bash
cd scraper/

# Install dependencies (installs backend as local editable package too)
uv sync

# Configure environment
cp .env.example .env
# edit .env if your DB credentials differ from defaults
```

## Run the scraper

```bash
# From scraper/ directory
uv run python -m scraper.main
```

Expected output:
```
10:00:01 INFO    scraper.main | Created scrape_run id=<uuid> source=licytacje.komornik.pl
10:00:01 WARNING scraper.main | KW CHECK DIGIT INVALID [SEVERITY:HIGH] kw=PO1P/00054321/9 ...
10:00:01 INFO    scraper.main | KW flagged UNVERIFIED kw=KR1K/00077712/? ...
10:00:01 INFO    scraper.main | Saved listing id=<uuid> kw=WA1M/00012345/2 confidence=0.80
10:00:01 INFO    scraper.main | Saved listing id=<uuid> kw=WA1M/00012345/2 confidence=0.45
10:00:01 INFO    scraper.main | Updated scrape_run <uuid> → status=completed

============================================================
SCRAPE COMPLETE
============================================================
  Scrape Run ID  : <uuid>
  Source         : licytacje.komornik.pl
  Listings found : 2
  Saved          : 2
  Skipped (dedup): 0
  Failed         : 0
  Duration       : 0.12s
============================================================
```

Run twice — second run produces `Skipped (dedup): 2` (SHA-256 deduplication at work).

## What gets written to the database

### `bronze.scrape_runs`
One row per run with `status=running → completed/failed/partial`.

### `bronze.raw_listings`
One row per listing:
- `raw_kw` — highest-confidence KW in canonical form `CCCC/NNNNNNNN/D`
- `raw_numer_dzialki` — best extracted działka number
- `raw_html_ref` — pointer to full extraction metadata (JSON Evidence Chain)
- `dedup_hash` — SHA-256(source_url + raw_text) prevents double-insert

## KW Regex — how it works

The extractor applies three passes:

| Pass | Pattern | Confidence |
|------|---------|-----------|
| Strict | `[A-Z]{2}[0-9][A-Z]/\d{8}/[0-9]` | 0.80–0.95 |
| Relaxed | Spaces or dashes as separators | 0.70 |
| Partial | No check digit | ≤ 0.45 |

After each match: check digit is validated using the Ministry of Justice
weighted-sum algorithm. Invalid check digit → confidence drops to ≤ 0.25
and a `SEVERITY:HIGH` warning is logged.

## Verify the extracted data

```bash
# Check what's in the DB
docker exec -it terrazoning_db psql -U terrazoning -d terrazoning -c "
SELECT
    id,
    source_url,
    raw_kw,
    raw_numer_dzialki,
    raw_obreb,
    price_zl,
    area_m2,
    is_processed,
    created_at
FROM bronze.raw_listings
ORDER BY created_at DESC
LIMIT 10;
"

# Check scrape_runs
docker exec -it terrazoning_db psql -U terrazoning -d terrazoning -c "
SELECT id, source_name, status, records_found, records_saved, finished_at
FROM bronze.scrape_runs
ORDER BY created_at DESC;
"
```

## Architecture

```
MOCK_HTML
    │
    ▼
LicytacjeScraper.run()
    │
    ├── create_scrape_run()          → bronze.scrape_runs (status=running)
    │
    ├── _parse_listing(url, html)
    │       ├── BeautifulSoup → raw_text
    │       ├── extract_kw_from_text()    ← extractors/kw.py
    │       │       ├── RE_STRICT regex
    │       │       ├── RE_RELAXED regex
    │       │       ├── RE_NO_CHECK regex
    │       │       ├── validate_check_digit() [weighted-sum algo]
    │       │       └── confidence scoring per persona rubric
    │       ├── extract_parcel_ids()  ← extractors/parcel.py
    │       └── field extractors (price, area, date, location)
    │
    ├── save_listing()               → bronze.raw_listings (ON CONFLICT DO NOTHING)
    │       └── dedup_hash = SHA-256(source_url + raw_text)
    │
    └── update_scrape_run()          → bronze.scrape_runs (status=completed)
```

## Next steps (E2-04 → E2-06)

1. **E2-04** — Replace `MOCK_LISTING_PAGES` with real `httpx` requests + proxy rotation
2. **E2-05** — Upload raw HTML to GCS; store URI in `raw_html_ref`
3. **E2-06** — Feed `raw_kw` + `raw_numer_dzialki` into the ULDK resolver → Silver layer
