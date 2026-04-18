# TerraZoning — How to Keep the System Up To Date

Stan na `2026-04-15`.

Ten dokument zbiera w jednym miejscu prosty plan utrzymania TerraZoning tak, żeby:
- nowe licytacje regularnie wpadały do systemu,
- geometrie i delty były przeliczane na bieżąco,
- `future_buildable` znajdowało nowe leady bez niepotrzebnie ciężkich rerunów,
- nie nadpisywać bogatszego stanu bazy zbyt agresywnym rolloutem bez kontroli.

## 1. Zasada główna

- Źródłem prawdy jest `Cloud SQL`.
- Produkcyjny dostęp do aplikacji jest przez `gcloud run services proxy`.
- Szerokie reruny rób świadomie i po status-checku.
- Po większych operacjach dobrze mieć backup lub przynajmniej porównanie liczników.

## 2. Codzienny rytm

To jest podstawowy przepływ, który utrzymuje system świeży i pozwala znajdować nowe leady:

1. `scrape-live`
2. `geo-resolve`
3. `delta`
4. `planning-signal-sync`
5. `future-buildability` dla `slaskie`
6. `future-buildability` dla `malopolskie`
7. `future-buildability` dla `podkarpackie`
8. status check

Repo command flow:

```bash
cd /Users/michalszarek/worksapace/terrazoning
make scrape-live PROVINCES="slaskie malopolskie podkarpackie" MAX_PAGES=3
make geo-resolve
make delta
make planning-signal-sync
make future-buildability PROVINCE=slaskie
make future-buildability PROVINCE=malopolskie
make future-buildability PROVINCE=podkarpackie
make future-buildability-status PROVINCE=slaskie
make future-buildability-status PROVINCE=malopolskie
make future-buildability-status PROVINCE=podkarpackie
make coverage-target-status PROVINCES="malopolskie podkarpackie" TARGET_PCT=70 LIMIT=12
```

## 3. Rekomendowana częstotliwość

### W ciągu dnia

- `scrape-live`: co `4h`
- `geo-resolve`: około `10 min` po scraperze
- `delta`: około `20 min` po resolverze

Cel:
- złapać nowe licytacje,
- przeliczyć bieżące okazje `current_buildable`,
- nie czekać do nocy na podstawowe odświeżenie danych.

### W nocy

- `planning-signal-sync`: raz na noc
- `future-buildability`: raz na noc, po `planning-signal-sync`

Cel:
- dociągać cięższe źródła planistyczne,
- aktualizować `future_buildable`,
- ograniczyć koszty i hałas z pełnych przeliczeń w ciągu dnia.

## 4. GCP-native tryb operatorski

Jeśli chcesz pracować w pełni przez GCP jobs, używaj:

```bash
cd /Users/michalszarek/worksapace/terrazoning
make gcp-job-scrape-live
make gcp-job-geo-resolve
make gcp-job-delta
make gcp-job-planning-signal-sync
make gcp-job-future-buildability GCP_JOB_ARGS="--province,slaskie"
make gcp-job-future-buildability GCP_JOB_ARGS="--province,malopolskie"
make gcp-job-future-buildability GCP_JOB_ARGS="--province,podkarpackie"
```

To jest preferowana ścieżka produkcyjna.

## 5. Rano: szybki smoke check

Zanim wejdziesz w większy batch, sprawdź:

```bash
cd /Users/michalszarek/worksapace/terrazoning
make cloudsql-health
make status
make future-buildability-status PROVINCE=slaskie
make future-buildability-status PROVINCE=malopolskie
```

Patrz przede wszystkim na:
- czy liczba leadów nie spadła nagle,
- czy backlog `near-threshold` nie urósł podejrzanie,
- czy DLQ / upstream blockers nie zaczęły dominować pipeline.

## 6. Kiedy robić pełny rollout

Pełny batch:

```bash
cd /Users/michalszarek/worksapace/terrazoning
make campaign-rollout-cloudsql
```

Rób go:
- co kilka dni,
- po większej serii zmian w źródłach,
- po poprawkach parserów, scoringu albo registry,
- po większym imporcie MPZP / POG / studium.

Nie odpalaj go po każdej drobnej zmianie jednej gminy.

## 7. Kiedy robić targeted rerun

Jeśli dodasz nowe źródło dla jednej gminy, preferowany tryb to rerun po `TERYT`:

```bash
cd /Users/michalszarek/worksapace/terrazoning
make planning-signal-sync TERYT=1206032
make future-buildability TERYT=1206032 BATCH_SIZE=50
```

Albo w GCP:

```bash
cd /Users/michalszarek/worksapace/terrazoning
make gcp-job-planning-signal-sync GCP_JOB_ARGS="--teryt,1206032"
make gcp-job-future-buildability GCP_JOB_ARGS="--teryt,1206032,--batch-size,50"
```

To jest najtańszy i najbardziej efektywny tryb iteracji.

## 8. Jak naprawdę znajdować nowe leady

Największy przyrost leadów dają zwykle:
- nowe `planning_signals`,
- lepsze pokrycie MPZP / POG / studium,
- targeted rerun dla gmin z dodatnim `overall_score`, ale jeszcze bez leada.

Najlepszy workflow:

1. sprawdź `future-buildability-status`,
2. wybierz gminy z najlepszym backlogiem,
3. dodaj źródło,
4. zrób rerun po `TERYT`,
5. sprawdź, czy gmina przeskoczyła do `supported` albo `formal`.

## 9. Czego nie robić

- Nie licz produkcji na lokalnym PostGIS jako źródle prawdy.
- Nie rób pełnego województwa po każdej drobnej zmianie.
- Nie nadpisuj bogatego stanu bazy szerokim rerunem bez porównania liczników.
- Nie ignoruj statusów po jobach, bo pipeline może „działać”, a mimo to dawać uboższy wynik.

## 10. Minimalna checklista operatorska

### Daily

```bash
make scrape-live PROVINCES="slaskie malopolskie podkarpackie" MAX_PAGES=3
make geo-resolve
make delta
make planning-signal-sync
make future-buildability PROVINCE=slaskie
make future-buildability PROVINCE=malopolskie
make future-buildability PROVINCE=podkarpackie
make future-buildability-status PROVINCE=slaskie
make future-buildability-status PROVINCE=malopolskie
make future-buildability-status PROVINCE=podkarpackie
```

### Nightly

- `planning-signal-sync`
- `future-buildability` dla trzech województw

### Weekly / co kilka dni

```bash
make campaign-rollout-cloudsql
```

### After new source

```bash
make planning-signal-sync TERYT=<teryt>
make future-buildability TERYT=<teryt> BATCH_SIZE=50
```

## 10a. Podkarpackie — regularny scope

Podkarpackie jest już częścią regularnego utrzymania, ale nadal warto mieć pod ręką osobne komendy operatorskie dla targeted rerunów:

```bash
make scrape-live PROVINCES="podkarpackie" MAX_PAGES=3
make report-podkarpackie
make future-buildability-status PROVINCE=podkarpackie
make future-buildability-backlog PROVINCE=podkarpackie BACKLOG_FORMAT=csv
make province-backlog-snapshot PROVINCE=podkarpackie BACKLOG_OUTPUT=runtime/podkarpackie_backlog_snapshot.csv
make campaign-podkarpackie
```

Jeśli potrzebny jest etap MPZP/WFS tylko dla tego województwa:

```bash
make sync-podkarpackie
```

Jeśli operator potrzebuje jednego pliku roboczego łączącego coverage backlog i future backlog:

```bash
make province-backlog-snapshot PROVINCE=podkarpackie BACKLOG_OUTPUT=runtime/podkarpackie_backlog_snapshot.csv

Jeśli operator chce szybko ocenić postęp do celu coverage dla województw:

```bash
make coverage-target-status PROVINCES="malopolskie podkarpackie" TARGET_PCT=70 LIMIT=12
```

Ta komenda pokazuje:
- liczbę gmin z działkami,
- liczbę gmin covered,
- aktualny procent coverage,
- ile gmin jeszcze brakuje do celu,
- rozkład uncovered backlogu (`manual_backlog`, `no_source_available`, `source_discovered_no_parcel_match`, `upstream_blocker`).

W praktyce:
- `source_discovered_no_parcel_match` oznacza, że publiczne źródło planów już istnieje, ale aktywne parcele nie wpadają jeszcze w żaden potwierdzony zasięg planu,
- `upstream_blocker` oznacza, że źródło jest rozpoznane, ale publiczny feed geometryczny jest chwilowo zepsuty albo zwraca błąd operatora.

Jeśli trzeba domknąć brakujące gminy coverage dla formalnie potwierdzonego backlogu bez publicznego parcel-safe feedu, można użyć kontrolowanego fallbacku opartego o geometrię już rozwiązanych parceli:

```bash
make formal-coverage-backfill TERYTS="1211092 1202024 1206105 1203034"
```

Ten helper:
- tworzy jedną konserwatywną strefę `gold.planning_zones` per `TERYT`,
- używa unii geometrii resolved parceli,
- zapisuje neutralne oznaczenie `MPZP`, które normalizuje się do `unknown`,
- ma służyć do domknięcia coverage, a nie do wymyślania buildable semantyki.
```

Jeśli operację prowadzimy już przez Cloud Run jobs zamiast lokalnych wywołań, używamy tych samych regularnych komend operatorskich:

```bash
make gcp-job-scrape-live GCP_JOB_ARGS="--provinces,podkarpackie,--max-pages,3"
make gcp-job-planning-signal-sync GCP_JOB_ARGS="--teryt,1816035"

Potwierdzony smoke dla rollout closure `2026-04-16`:
- `terrazoning-campaign-rollout-hcr9p`
- `terrazoning-campaign-rollout-qmh58`
- `terrazoning-campaign-rollout-zf2fw`

Każdy z trzech pełnych GCP rolloutów zakończył się sukcesem na obrazie `sha256:83e6843fb6bfa03fff383fa0d1ffec1e6b7f19c5f9d8b013fb8640ae907a93c5` i utrzymał stabilny stan `leads=2` dla Podkarpackiego.
make gcp-job-future-buildability GCP_JOB_ARGS="--province,podkarpackie,--batch-size,250"
make gcp-job-campaign-rollout GCP_JOB_ARGS="--province,podkarpackie,--autofix,--parallel"
```

`GCP_JOB_ARGS` zawiera tylko argumenty skryptu. Makefile automatycznie dokleja bazowe
`uv run --project ... python ...`, dzięki czemu ręczny rollout nie gubi entrypointu joba.

Jeśli któryś przebieg utknie na `covered_but_no_delta` albo `manual_backlog`, a listing ma bardzo mocny sygnał inwestycyjny, można użyć kontrolowanego toru operatorskiego:

```bash
make quarantine-candidates PROVINCE=podkarpackie LIMIT=8
make quarantine-promote PROVINCE=podkarpackie TERYT=1816145 SOURCE_HINT=warunkami-zabudowy MANUAL_PRZEZNACZENIE=MN
```

Aktualnie potwierdzony przypadek produkcyjny:
- `1816145`
- `181614502.267/3`
- lead utworzony przez `manual://quarantine_override`

Aktualnie potwierdzony automatyczny buildable lead:
- `1809054`
- `180905401.419`
- `confidence_band=supported`
- dominujący sygnał: `planning_resolution: mixed_residential (SUiKZP 1809054)`

Drugi potwierdzony automatyczny buildable lead:
- `1804082`
- `180408204.1147/1`
- `confidence_band=supported`
- dominujący sygnał: `planning_resolution: mixed_residential (SUiKZP 1804082)`

Ważna uwaga operatorska po incydencie z `2026-04-16`:
- pełny `campaign-rollout` musi po scoped replay uruchamiać również `future_buildability`, inaczej odtworzy tylko `current_buildable` i może wyczyścić `future_buildable` leady z live bazy;
- scoped resolution dla prowincji z pustym `target_listing_ids` nie może wpadać w `listing_ids=None`, bo to uruchamia globalny `geo_resolver` zamiast pustego, bezpiecznego przebiegu.

Aktualnie potwierdzony automatyczny fallback coverage:
- `1816072`
- `181607203.3/4`
- `181607203.609/2`
- `181607205.1484/1`
- `planning_zones=3`, `delta_results=3`, `przeznaczenie=ZL`
- bez `manual://quarantine_override`

Drugi potwierdzony automatyczny fallback coverage:
- `1816145`
- `181614502.267/3`
- `181614502.267/4`
- `181614502.267/5`
- `181614502.267/6`
- `181614502.273/3`
- `181614502.273/4`
- `181614502.273/5`
- źródło: `view_gml.php?plan=038` + APP metadata fallback
- `planning_zones=7`, `delta_results=7`, `przeznaczenie=ZL`
- bez `manual://quarantine_override`

Trzeci potwierdzony conservative project-coverage path:
- `1808042`
- `180804221.3083/1`
- `180804221.3083/2`
- `180804221.3538`
- źródło: publiczny ZIP APP z BIP Giedlarowej
- `planning_zones=1`, `delta_results=3`, `przeznaczenie=MPZP_PROJ`
- bez leadów, ale gmina wyszła z `no_source_available`

Czwarty potwierdzony conservative project-coverage path:
- `1805042`
- `180504205.26`
- `180504205.29`
- źródło: publiczny GISON WFS `ms:app.AktPlanowaniaPrzestrzennego.MPZP`
- `planning_zones=60`, `delta_results=0`, `planning_signals_created=2`
- status po rerunie: `covered_but_no_delta`, `why_no_lead=unknown_only`
- najbliższe aktywne parcele są ~`652.7-652.8 km` od najbliższego zasięgu planu

Potwierdzony formalny signal-only path bez geometrii:
- `1804082`
- źródło: `https://radymno.geoportal-krajowy.pl/mpzp`
- parser `planning_signal_sync` czyta osadzony `mpzpRegistry`
- `planning_signals_created=3`
- w tym:
  - `planning_resolution`
  - `formal_binding`
  - `designation_raw=MPZP`
  - `plan_name=MPZP registry 1804082`
- status operatorski:
  - `manual_backlog`
  - `known_sources=html_index`
  - `operator_status=needs_geometry_source`

Potwierdzony formalny signal-only path bez geometrii:
- `1809054`
- źródła:
  - `https://narol.geoportal-krajowy.pl/plan-ogolny`
  - `https://narol.geoportal-krajowy.pl/mpzp`
- parser `planning_signal_sync` czyta osadzony `mpzpRegistry`
- publiczny rejestr pokazuje:
  - `11` obowiązujących MPZP
  - ok. `187 ha` pokrycia planami
- status operatorski:
  - `manual_backlog`
  - `known_sources=html_index`
  - `operator_status=needs_geometry_source`

Potwierdzony formalny signal-only path bez geometrii:
- `1803042`
- źródła:
  - `https://debica.geoportal-krajowy.pl/`
  - `https://debica.geoportal-krajowy.pl/mpzp`
- `planning_signal_sync` utworzył `1` sygnał formalny
- status operatorski:
  - `manual_backlog`
  - `known_sources=html_index`
  - `operator_status=needs_geometry_source`

Potwierdzony formalny signal-only path bez geometrii:
- `1803052`
- źródła:
  - `https://jodlowa.geoportal-krajowy.pl/`
  - `https://jodlowa.geoportal-krajowy.pl/plan-ogolny`
- `planning_signal_sync` utworzył `1` sygnał formalny
- status operatorski:
  - `manual_backlog`
  - `known_sources=html_index`
  - `operator_status=needs_geometry_source`

Potwierdzony formalny signal-only path bez geometrii:
- `1816065`
- źródła:
  - `https://glogow-malopolski.geoportal-krajowy.pl/`
  - `https://glogow-malopolski.geoportal-krajowy.pl/mpzp`
- `planning_signal_sync` utworzył `1` sygnał formalny
- status operatorski:
  - `manual_backlog`
  - `known_sources=html_index`
  - `operator_status=needs_geometry_source`

Potwierdzony formalny signal-only path bez geometrii:
- `1805112`
- źródła:
  - `https://bip.tarnowiec.eu/planowanie-przestrzenne/238`
  - `https://bip.tarnowiec.eu/projekty-mpzp/290`
  - `https://tarnowiec.eu/aktualnosc-4123-przystapienie_do_sporzadzenia_planu.html`
- `planning_signal_sync` utworzył `2` sygnały formalne
- status operatorski:
  - `manual_backlog`
  - `known_sources=html_index`
  - `operator_status=needs_geometry_source`

Potwierdzony formalny signal-only path bez geometrii:
- `1807025`
- źródła:
  - `https://dukla.geoportal-krajowy.pl/plan-ogolny`
  - `https://www.dukla.pl/pl/dla-mieszkancow/mapy-i-plany-79/wnioski-do-planu-ogolnego-226`
  - `https://www.dukla.pl/files/_source/2025/01/ogloszenie%20na%20BIP%20i%20na%20strone%20gminy.pdf`
- `planning_signal_sync` utworzył `3` sygnały formalne
- status operatorski:
  - `manual_backlog`
  - `known_sources=html_index`
  - `operator_status=needs_geometry_source`

Potwierdzony formalny signal-only path bez geometrii:
- `1815012`
- źródło:
  - `https://iwierzyce.e-mapa.net/wykazplanow/`
- `planning_signal_sync` utworzył `2` sygnały formalne
- status operatorski:
  - `manual_backlog`
  - `known_sources=html_index`
  - `operator_status=needs_geometry_source`

Potwierdzony formalny signal-only path bez geometrii:
- `1811032`
- źródła:
  - `https://czermin-mielecki.geoportal-krajowy.pl/`
  - `https://czermin-mielecki.geoportal-krajowy.pl/plan-ogolny`
- `planning_signal_sync` utworzył `1` sygnał formalny
- status operatorski:
  - `manual_backlog`
  - `known_sources=html_index`
  - `operator_status=needs_geometry_source`

Potwierdzony formalny signal-only path bez geometrii:
- `1803065`
- źródła:
  - `https://pilzno.geoportal-krajowy.pl/`
  - `https://pilzno.geoportal-krajowy.pl/plan-ogolny`
  - `https://pilzno.geoportal-krajowy.pl/mpzp`
- `planning_signal_sync` utworzył `2` sygnały formalne
- status operatorski:
  - `manual_backlog`
  - `known_sources=html_index`
  - `operator_status=needs_geometry_source`

Jawny upstream blocker zamiast „braku źródła”:
- `1803042`
  - `https://gminadebica.e-mapa.net/wykazplanow/`
  - aktualny wynik: `Błąd połączenia em_`
- `1803052`
  - `https://jodlowa.e-mapa.net/wykazplanow/`
  - aktualny wynik: `Błąd połączenia em_`

## 11. Dostęp do aplikacji

Lokalny dostęp operatorski:

```bash
cd /Users/michalszarek/worksapace/terrazoning
make gcp-auth
make gcp-proxy
```

Potem otwórz:
- `http://localhost:5173`
- `http://localhost:8000/docs`

## 12. Guardrail przed większym rerunem

Przed szerokim rolloutem warto zapisać przynajmniej:
- `gold.investment_leads`
- split `future/current`
- `gold.planning_zones`

Jeśli po rerunie liczby spadną nienaturalnie, porównaj je od razu z poprzednim snapshotem, zanim uznasz wynik za poprawny.
