# TerraZoning — Plan Dalszej Implementacji MPZP Coverage

> Obszar: `backend/run_wfs_sync.py` + `backend/app/services/wfs_downloader.py` + operacyjny replay `geo_resolver` / `delta_engine`
> Cel biznesowy: zwiększyć pokrycie `gold.planning_zones`, aby odzyskane działki z `silver.dzialki` realnie wpadały do `gold.investment_leads`.

---

## 1. Stan obecny

### Co już działa

- `run_wfs_sync.py` ma działający `WFS_REGISTRY`.
- Możemy synchronizować potwierdzone źródła MPZP dla:
  - `2469011` Katowice
  - `2466011` Gliwice
  - `2416085` Szczekociny
  - `1261011` Kraków
- `run_wfs_sync.py` ma diagnostykę:
  - `--list-registry`
  - `--list-uncovered`
  - `--probe-app-url`
  - `--teryt`
- `APP probe` umie już odsiać publiczne ZIP/XML, które wyglądają na planistyczne, ale w praktyce zawierają tylko akty APP lub obwiednie planów.
- `force_retry.py` umożliwia replay kolejek po załadowaniu nowych stref.

### Co dziś jest blockerem

- Wiele gmin ma działki w `silver.dzialki`, ale nie ma jeszcze żadnego pewnego, publicznego źródła stref MPZP.
- Część portali wystawia tylko:
  - WMS bez WFS
  - WFS ewidencyjny bez warstw planistycznych
  - APP ZIP z aktami planistycznymi, ale bez stref przeznaczenia
  - niedziałające `e-mapa` typu `Błąd połączenia em_`

### Najważniejszy wniosek

Następny skok wartości nie wynika już z samego parsera. Największy zwrot da teraz systematyczne rozszerzanie `WFS_REGISTRY` o tylko te źródła, które faktycznie mają poligony stref planistycznych z oznaczeniami typu `MN`, `MW`, `U`, `MU`.

---

## 2. Cel Etapu

### Główny cel

Zamienić obecną, ręczną eksplorację źródeł MPZP w powtarzalny workflow:

1. wykryj gminy bez pokrycia,
2. zbadaj źródło,
3. potwierdź, że to prawdziwe strefy MPZP,
4. dopisz do `WFS_REGISTRY`,
5. uruchom sync,
6. przelicz pipeline,
7. sprawdź, czy przybyło realnych leadów.

### Definicja sukcesu

- każda nowa gmina trafiająca do `WFS_REGISTRY` ma:
  - publiczny endpoint,
  - stabilny format,
  - geometrię możliwą do reprojekcji,
  - pola pozwalające wyznaczyć `przeznaczenie`
- po każdym batchu registry:
  - rośnie `gold.planning_zones`
  - replay `force_retry.py` nie psuje istniejących leadów
  - rośnie lub utrzymuje się liczba realnych `gold.investment_leads`

---

## 3. Etapy Implementacji

## Etap A — Discovery i klasyfikacja źródeł

### Cel

Zamknąć ręczne „szukanie po omacku” i przejść na shortlistę opartą o dane z bazy.

### Zadania

- Używać `--list-uncovered` jako wejścia do każdej rundy.
- Priorytetyzować gminy po:
  - liczbie działek w `silver.dzialki`
  - prawdopodobieństwie istnienia publicznego SIP/WFS
  - znaczeniu biznesowym lokalizacji
- Dla każdej kandydatki sklasyfikować źródło jako jedno z:
  - `READY_WFS`
  - `READY_REST`
  - `APP_ONLY`
  - `WMS_ONLY`
  - `BROKEN_PORTAL`
  - `UNKNOWN`

### Deliverables

- uzupełniany backlog źródeł w tym dokumencie lub osobnym trackerze
- krótka notatka dla każdej gminy: URL, typ źródła, status, ryzyko

### Acceptance criteria

- każda gmina z top 10 `--list-uncovered` ma przypisany status źródła
- nie dodajemy do registry niczego, co nie przeszło klasyfikacji

---

## Etap B — Twarda walidacja źródła przed dodaniem do registry

### Cel

Eliminować fałszywe źródła zanim wejdą do kodu.

### Zadania

- Dla APP ZIP/XML używać:
  - `uv run python run_wfs_sync.py --probe-app-url '<URL>'`
- Dla WFS/WMS sprawdzać:
  - `GetCapabilities`
  - `DescribeFeatureType`
  - realne nazwy warstw
  - czy pola zawierają oznaczenie przeznaczenia
- Walidować, czy dane zawierają:
  - poligony lub multipolygony
  - atrybut oznaczenia, np. `oznaczenie`, `symbol`, `przeznaczenie`
  - sensowną informację o planie lub uchwale

### Deliverables

- potwierdzony zestaw parametrów dla nowej gminy:
  - `wfs_url`
  - `layer_name`
  - `source_srid`
  - `field_mapping`
  - `wfs_version`
  - `prefer_json`
  - `swap_xy`

### Acceptance criteria

- nowy wpis jest gotowy do uruchomienia `run_wfs_ingest` bez zgadywania nazw pól
- źródło nie jest `APP_ONLY`, `WMS_ONLY` ani `BROKEN_PORTAL`

---

## Etap C — Rozszerzanie `WFS_REGISTRY`

### Cel

Dopisywać tylko pewne, przetestowane źródła.

### Zadania

- Dodawać nowe `WFSRegistryEntry` w stylu istniejących wpisów.
- Przy każdym wpisie zostawić komentarz:
  - skąd jest źródło,
  - jaka warstwa jest właściwa,
  - jaki format zwraca,
  - na co uważać przy osi/CRS
- Po dopisaniu wpisu wykonać test lokalny tylko dla tej gminy.

### Deliverables

- kolejne wpisy w `WFS_REGISTRY`
- ewentualne drobne poprawki w `wfs_downloader.py`, jeśli nowy vendor wymaga obsługi niestandardowego formatu

### Acceptance criteria

- `uv run python run_wfs_sync.py --teryt <KOD>`
  kończy się sukcesem
- rekordy trafiają do `gold.planning_zones`
- nie psujemy istniejących wpisów registry

---

## Etap D — Replay pipeline po każdym batchu registry

### Cel

Sprawdzać nie tylko sam ingest, ale też realny wpływ na Gold.

### Zadania

- Po każdej rundzie registry uruchamiać:
  1. `uv run python run_wfs_sync.py`
  2. `uv run python force_retry.py`
- Porównywać przed/po:
  - `gold.planning_zones`
  - `gold.delta_results`
  - `gold.investment_leads`
- Gdy leadów nie przybywa, sprawdzać:
  - czy nowe strefy są buildable
  - czy odzyskane działki w ogóle leżą w gminach z nowym pokryciem
  - czy `coverage_pct` nie odpada na progu biznesowym

### Deliverables

- krótkie podsumowanie batchu: `source -> zones -> delta -> leads`

### Acceptance criteria

- każda runda daje mierzalny raport wpływu
- brak „cichych” ingestów bez sprawdzenia efektu końcowego

---

## Etap E — Hardening i automatyzacja

### Cel

Zmniejszyć ręczną pracę w kolejnych rundach.

### Zadania

- Utrzymać i rozwijać:
  - `--list-uncovered`
  - `--probe-app-url`
- Rozważyć kolejne rozszerzenia:
  - eksport backlogu uncovered gmin do markdown/json
  - prosty scoring źródeł
  - snapshot porównawczy `before/after` dla `planning_zones` i `leads`
- Dodać testy regresyjne dla parserów źródeł, jeśli trafią nowe vendory

### Deliverables

- bardziej przewidywalny workflow operacyjny

### Acceptance criteria

- nowy batch gmin można przejść bez ręcznego „reverse engineering od zera”

---

## 4. Priorytetowa kolejka gmin

Na dziś pierwsza kolejka do dalszej eksploracji:

1. `2403052` — Chybie
2. `2405011` — Knurów
3. `2472011` — Ruda Śląska
4. `1206105` — Cianowice / gmina Skała
5. `1206114` — Skawina
6. `1213062` — Broszkowice
7. `1216145` — Paleśnica

### Uzasadnienie

- mają już działki w `silver.dzialki`
- są najwyżej w `--list-uncovered`
- część z nich ma publiczne portale SIP, ale nie są jeszcze rozpoznane do końca

---

## 5. Co możemy wykonać już teraz

Poniższe kroki są dostępne na aktualnej implementacji, bez dalszych zmian w kodzie.

### Krok 1 — Zobaczyć, gdzie naprawdę brakuje pokrycia

```bash
cd /Users/michalszarek/worksapace/terrazoning/backend
uv run python run_wfs_sync.py --list-uncovered
```

### Krok 2 — Sprawdzić aktualne wpisy registry

```bash
cd /Users/michalszarek/worksapace/terrazoning/backend
uv run python run_wfs_sync.py --list-registry
```

### Krok 3 — Przetestować podejrzane APP ZIP/XML bez dotykania bazy

```bash
cd /Users/michalszarek/worksapace/terrazoning/backend
uv run python run_wfs_sync.py --probe-app-url '<PUBLIC_APP_URL>'
```

Przykład:

```bash
uv run python run_wfs_sync.py --probe-app-url 'https://psip.rudaslaska.pl/app/rs01/APP_RS01.zip'
```

### Krok 4 — Zsynchronizować tylko jedną, potwierdzoną gminę

```bash
cd /Users/michalszarek/worksapace/terrazoning/backend
uv run python run_wfs_sync.py --teryt 2466011
```

Możliwe już teraz:

- `2466011` Gliwice
- `2469011` Katowice
- `2416085` Szczekociny
- `1261011` Kraków

### Krok 5 — Odpalić pełny sync wszystkich potwierdzonych źródeł

```bash
cd /Users/michalszarek/worksapace/terrazoning/backend
uv run python run_wfs_sync.py
```

### Krok 6 — Wymusić replay resolvera i Delty po nowym syncu

```bash
cd /Users/michalszarek/worksapace/terrazoning/backend
uv run python force_retry.py
```

### Krok 7 — Sprawdzić, czy pojawiły się nowe leady

```bash
cd /Users/michalszarek/worksapace/terrazoning/backend
uv run python -m app.services.delta_engine
curl 'http://localhost:8000/api/v1/leads?include_count=true&limit=20'
```

---

## 6. Minimalny workflow na kolejną rundę

Najbardziej praktyczny przebieg następnej iteracji:

1. `uv run python run_wfs_sync.py --list-uncovered`
2. wybór 1-2 gmin z top listy
3. ręczna eksploracja publicznych portali tych gmin
4. `--probe-app-url` dla każdego podejrzanego APP ZIP/XML
5. dopisanie tylko pewnych źródeł do `WFS_REGISTRY`
6. `uv run python run_wfs_sync.py --teryt <KOD>`
7. `uv run python force_retry.py`
8. porównanie liczby `planning_zones`, `delta_results`, `investment_leads`

---

## 7. Zasady bezpieczeństwa implementacyjnego

- Nie dodajemy do `WFS_REGISTRY` źródeł tylko dlatego, że mają w nazwie `MPZP`.
- Nie traktujemy `APP` jako stref MPZP bez potwierdzenia atrybutów przeznaczenia.
- Nie dodajemy placeholderów „na później”.
- Każdy wpis registry musi przejść:
  - walidację źródła,
  - test jednostkowy w praktyce przez `--teryt`,
  - weryfikację efektu w `gold.planning_zones`

---

## 8. Rekomendacja na następną sesję

Najbardziej opłacalny następny ruch:

1. eksploracja `Knurów`
2. eksploracja `Chybie`
3. dopiero potem kolejne miasta z bardziej złożonym SIP

Powód:

- są wysoko w uncovered queue
- mają realne działki w `silver`
- mają największą szansę dać następny wzrost pokrycia przy relatywnie małym koszcie eksploracji
