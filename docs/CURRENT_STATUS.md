# TerraZoning — Current Status

Stan na `2026-04-09`.

Ten dokument jest krótkim runbookiem operacyjnym: pokazuje, co system robi dziś, jakie są aktualne metryki oraz jak uruchamiać najważniejsze etapy pipeline'u.

---

## 1. Zakres systemu

TerraZoning jest potokiem danych dla arbitrażu gruntowego:

1. scraper pobiera ogłoszenia komornicze z `licytacje.komornik.pl`
2. ekstraktory wyciągają numer działki, KW, lokalność, cenę i powierzchnię
3. `GeoResolver` rozwiązuje działki do geometrii katastralnej
4. `MPZP sync` ładuje lokalne strefy planistyczne do PostGIS
5. `DeltaEngine` liczy przecięcia działka × strefa i tworzy leady inwestycyjne
6. FastAPI wystawia GeoJSON dla frontendowego kokpitu

---

## 2. Aktualne metryki

### Globalnie

| Metryka | Wartość |
|---|---:|
| `bronze.raw_listings` | `85` |
| `bronze.pending` | `0` |
| `silver.dzialki` | `96` |
| `silver.dlq_parcels` | `0` |
| `gold.planning_zones` | `49 555` |
| `gold.investment_leads` | `27` |
| `manual_backlog` | `9` |

### Główne województwa

| Województwo | Kod TERYT | `silver.dzialki` | `gold.investment_leads` |
|---|---|---:|---:|
| Małopolskie | `12` | `49` | `15` |
| Śląskie | `24` | `45` | `12` |

### Coverage status

| Województwo | Uncovered gminy | Uncovered działki |
|---|---:|---:|
| Małopolskie | `8` | `10` |
| Śląskie | `5` | `6` |

### Jakość cen leadów

| Metryka | Wartość |
|---|---:|
| Leady z `price_per_m2_zl` | `27 / 27` |
| Leady z `price_signal = reliable` | `25 / 27` |
| Leady oznaczane jako `cena do weryfikacji` | `2` |

Interpretacja:
- system jest już operacyjny end-to-end
- `Małopolskie` i `Śląskie` są aktywnie pokrywane
- aktywny DLQ został wyzerowany, a wyczerpane sprawy trafiają do jawnego `manual_backlog`
- `Mykanów / Grabowa (2404112)` zostało odblokowane przez `gison_raster` i wygenerowało nowy lead
- `Żegocina / Bełdno (1201092)` zostało odblokowane przez `gison_raster` i wyniosło lokalny lead do top opportunities
- `Bochnia / Chełm (1201022)` zostało odblokowane przez `gison_raster`; coverage zeszło z backlogu, ale obecny plan daje rural-only przecięcie bez nowego leada
- `Zawoja (1215082)` została odblokowana przez `gison_raster` i wygenerowała nowy lead
- `Pawłowice / Warszowice (2410042)` zostały odblokowane przez `gison_raster` i wygenerowały nowy lead
- `Knurów (2405011)` został odblokowany przez `wms_grid` na bazie publicznego popupu GISON i zszedł z uncovered backlogu
- `Pszczyna / Łąka (2410055)` została odblokowana przez `wms_grid` i wygenerowała nowy lead z przeznaczeniem `MN`
- `Czerwionka-Leszczyny / Dębieńsko (2412014)` została oznaczona jako covered przez `wms_grid`; portal potwierdza MPZP dla działki, ale publiczny popup nie oddaje symbolu strefy, więc zapisujemy konserwatywny `MPZP_UNK`
- bezpośrednie probe'y dla Poczesnej, Bojszów i Wodzisławia Śląskiego nadal nie dały nowych bezpiecznych źródeł MPZP do obecnej architektury
- bezpośrednie probe'y dla Poczesnej i Bojszów wracają już kontrolowanym raportem `legend_missing_semantics` z błędem `Raster payload is not a decodable image`, zamiast tracebacka
- główny dalszy wzrost zależy przede wszystkim od dalszego coverage MPZP i jakości cen, nie od samego scrapera
- pozostały uncovered backlog w Śląskiem i Małopolsce jest już listą kontrolowanych wyjątków z jawną kategorią i `next_action`, a nie nieznanym stanem systemu

---

## 3. Co działa produkcyjnie

### Scraper i ekstrakcja

- live scrape z `licytacje.komornik.pl`
- deduplikacja po `SHA-256`
- ekstrakcja KW, numeru działki, lokalności i ceny
- regex + fallbacki tekstowe + LLM fallback dla trudnych przypadków
- reparse Bronze bez ponownego scrapowania

### GeoResolver

- `ULDK GetParcelById`
- `ULDK GetParcelByIdOrNr`
- fallback miejski dla dużych miast
- official notice enrichment
- publiczne fallbacki WFS / MSIP dla części JST
- DLQ z retry, replayem i jawnym `manual_backlog` dla wyczerpanych / parserowych przypadków

### MPZP / GIS

- standardowy ingest WFS
- dodatkowy ingest `wms_grid` dla trudniejszych JST, m.in. `Knurów`, `Pszczyna / Łąka` i `Czerwionka-Leszczyny / Dębieńsko`
- produkcyjny ingest `gison_raster` dla Jabłonki, Jeleśni, Andrychowa, Mykanowa/Grabowej, Żegociny/Bełdna, Bochni/Chełmu, Zawoi oraz Pawłowic/Warszowic
- lokalne `planning_zones` w PostGIS
- DeltaEngine z normalizacją symboli planistycznych (`MN.1`, `19.MN`, `U/MN-3` itd.)
- filtry jakości przestrzennej:
  - lead tylko jeśli `coverage >= 30%`
  - albo `intersection_area_m2 >= 500`

### Frontend

- widoczne leady na niskim zoomie
- lista i szczegóły leadów
- widok kwarantanny / human-in-the-loop
- metryki inwestorskie:
  - cena wejścia
  - `zł/m²`
  - powierzchnia budowlana
  - pokrycie MPZP
  - jakość ceny
- sortowanie i filtrowanie po jakości ceny
- jawne sygnały `price_signal`, `quality_signal` i brakujące metryki w detalu leada
- watchlista inwestora jest teraz zapisywana po stronie backendu (`/api/v1/watchlist`)
- licznik `new` w watchliście działa jako trwały inbox „od ostatniego przeglądu”
- watchlista może podnosić alert desktop dla nowych dopasowań
- eksport shortlisty zawiera teraz status, `reviewed_at`, notatkę i sygnały jakości

### Raporty operatorskie

- `run_province_campaign.py --stage report` pokazuje teraz:
  - `DLQ by category`
  - `Lead quality`
  - `Top opportunities`
- uncovered backlog raportuje:
  - `coverage_category`
  - `next_action`
  - oraz coraz więcej JST z pipeline'u `gison_raster`
- `reset_queues()` czyści stale DLQ automatycznie przed replayem
- `reset_queues()` archiwizuje też wyczerpane manualne / parserowe sprawy do `backend/.runtime/manual_backlog.json`

---

## 4. Najważniejsze komendy

Uruchamiaj z katalogu repo:

```bash
cd /Users/michalszarek/worksapace/terrazoning
```

### Szybki status i diagnoza

```bash
make doctor
make status
make report-slaskie
make report-malopolskie
```

### Scraper

```bash
make scrape-dry
make scrape-live
```

Wariant bez Makefile:

```bash
cd scraper
uv run python run_live.py --provinces slaskie malopolskie --max-pages 3
```

### Reparse Bronze

```bash
make reparse-bronze
```

### GeoResolver

```bash
make geo-resolve
```

### DeltaEngine

```bash
make delta
```

### MPZP

```bash
make mpzp-registry
make mpzp-uncovered
make mpzp-sync
make mpzp-one TERYT=2466011
make sync-slaskie
make sync-malopolskie
```

### Replay / pełne odświeżenie

```bash
make force-retry
make load-all-data
make refresh-all
```

### Kampanie wojewódzkie

```bash
make campaign-slaskie
make campaign-malopolskie
make campaign-all
```

### Frontend i backend

```bash
cd backend
uv run uvicorn app.main:app --reload --port 8000
```

```bash
cd frontend
npm run dev
```

---

## 5. Główne ograniczenia na dziś

### Coverage MPZP

Największy dalszy wzrost leadów zależy od dopięcia kolejnych gmin, szczególnie tych, które nadal wystawiają tylko:
- granice planów
- APP / dokumenty
- rastry bez jawnych stref wektorowych

### Trudne JST

Najtrudniejsze przypadki to gminy z tzw. „fasadą WFS”, gdzie:
- capability endpoint istnieje
- ale brakuje warstwy stref przeznaczenia
- albo warstwa istnieje schematycznie, lecz zwraca pusty `FeatureCollection`

### Jakość cen

Parser cen został mocno poprawiony, ale nadal warto monitorować edge case'y:
- udziały w nieruchomości
- wzmianki o `wadium`, `rękojmi`, `najniższym postąpieniu`
- mniej standardowe formuły sprzedażowe

---

## 6. Rekomendowane dalsze kroki

1. Dalsze zwiększanie coverage MPZP dla ostatnich uncovered gmin w Śląskiem: `2406092`, `2401075`, `2404082`, `2414042`, `2415041`.
2. Rozpoznanie i odfiltrowanie pozostałych leadów z jakością `review_required`, zanim trafią do shortlisty inwestorskiej.
3. Rozbudowa ingestu dla kolejnych źródeł `APP/WMS raster+legend`, gdzie nie ma pełnego WFS strefowego.
4. Dalsze porządkowanie `manual_backlog` i source discovery dla niepokrytych JST.

---

## 7. Powiązane dokumenty

- [README.md](/Users/michalszarek/worksapace/terrazoning/README.md)
- [DB_SCHEMA.md](/Users/michalszarek/worksapace/terrazoning/docs/DB_SCHEMA.md)
- [TASK_BOARD.md](/Users/michalszarek/worksapace/terrazoning/docs/TASK_BOARD.md)
- [RECENT_CHANGES_2026_04.md](/Users/michalszarek/worksapace/terrazoning/docs/RECENT_CHANGES_2026_04.md)
