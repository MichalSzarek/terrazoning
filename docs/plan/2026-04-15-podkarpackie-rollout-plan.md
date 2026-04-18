# TerraZoning — Plan wdrożenia województwa podkarpackiego

## Summary

Celem jest uruchomienie `podkarpackiego` jako trzeciego, w pełni obsługiwanego województwa w TerraZoning, w tym:
- pełny pipeline `scrape -> geo-resolve -> delta -> MPZP/WFS -> planning signals -> future_buildability`,
- osobne workflow operatorskie i raportowe,
- checklista rolloutu od pierwszego ingestu do stabilnego utrzymania w Cloud SQL / Cloud Run Jobs,
- bez mieszania rolloutu Podkarpackiego z już stabilnym torem `slaskie + malopolskie`, dopóki nie przejdzie smoke i coverage gates.

Ważny stan początkowy z repo:
- core resolver zna już `podkarpackie` (`_PROVINCE_TO_WOJ` ma prefix `18`),
- warstwa operatorska i campaign tooling są jeszcze twardo spięte do `slaskie` i `malopolskie`,
- nie ma dziś jawnie skonfigurowanego registry WFS / MPZP / planning sources dla Podkarpackiego,
- domyślne batch flows w `Makefile` i runbookach obejmują tylko dwa województwa.

## Następna faza

Po domknięciu rollout foundation ten dokument należy czytać razem z planem kolejnego etapu:

- [2026-04-16-podkarpackie-gcp-closure-plan.md](./2026-04-16-podkarpackie-gcp-closure-plan.md)

Ten drugi plan obejmuje:
- zamknięcie ostatniego backlogu source/geometry,
- uzyskanie automatycznego buildable leada,
- oraz uznanie Podkarpackiego za regularnie wdrożone i utrzymywane na GCP.

## Status aktualny — 2026-04-16

### Co jest już wdrożone

- `podkarpackie` jest dodane do scope operator tooling, CLI i runbooków.
- baseline danych działa:
  - `bronze=35`
  - `silver=56`
  - `dlq=7`
- coverage MPZP jest już aktywne dla:
  - `1810011` Łańcut
  - `1810042` Gmina Łańcut / Handzlówka
  - `1816035` Boguchwała
  - `1821035` Lesko / Huzele
- planning signal registry dla pierwszego batcha Podkarpackiego działa.
- parser GML / axis-order w imporcie MPZP został naprawiony:
  - GeoServer `gml:featureMembers` jest poprawnie parsowany
  - `swap_xy` działa także dla źródeł `EPSG:2180`
- fałszywie obiecujące źródło `1816065` zostało wycofane z registry i wróciło do jawnego blockera.
- `1816072` zostało przesunięte z `no_source_available` do `gison_raster_candidate`.
- `1816072` zostało doprowadzone do automatycznego fallback source path przez APP metadata + `wms_grid`:
  - `planning_zones=3`
  - `delta_results=3`
  - `przeznaczenie=ZL`
  - bez `manual://quarantine_override`
- `1816145` zostało doprowadzone do automatycznego fallback source path przez APP metadata + `wms_grid`:
  - `plan=038`
  - `planning_zones=7`
  - `delta_results=7`
  - `przeznaczenie=ZL`
  - bez `manual://quarantine_override`
- `1808042` zostało przesunięte z `no_source_available` do konserwatywnego coverage przez `app_gml`:
  - źródło: publiczny ZIP APP z BIP Giedlarowej
  - `planning_zones=1`
  - `delta_results=3`
  - `przeznaczenie=MPZP_PROJ`
  - bez leadów, ale już poza uncovered backlogiem
- `1805042` zostało przesunięte z `no_source_available` do konserwatywnego coverage przez `app_gml`:
  - źródło: publiczny GISON WFS `ms:app.AktPlanowaniaPrzestrzennego.MPZP`
  - `planning_zones=60`
  - `delta_results=0`
  - `planning_signals=2`
  - status końcowy tej iteracji: `covered_but_no_delta / unknown_only`
- `1804082` zostało przesunięte z czystego `no_source_available` do `manual_backlog`:
  - źródło: `radymno.geoportal-krajowy.pl/mpzp`
  - parser `planning_signal_sync` czyta osadzony `mpzpRegistry`
  - `planning_signals_created=3`
  - jeden z sygnałów ma status `planning_resolution / formal_binding / MPZP registry 1804082`
  - nadal brak geometrii MPZP, więc to nie jest jeszcze coverage path
- `1809054` zostało przesunięte z czystego `no_source_available` do `manual_backlog`:
  - źródła: `narol.geoportal-krajowy.pl/plan-ogolny` oraz `narol.geoportal-krajowy.pl/mpzp`
  - parser `planning_signal_sync` czyta osadzony `mpzpRegistry`
  - publiczny rejestr pokazuje `11` obowiązujących MPZP o łącznej powierzchni ok. `187 ha`
  - nadal brak geometrii MPZP, więc to nie jest jeszcze coverage path
  - po aktualizacji scoringu `future_buildability` wygenerowało pierwszy automatyczny buildable lead:
    - `180905401.419`
    - `confidence_band=supported`
    - `dominant_future_signal=planning_resolution: mixed_residential (SUiKZP 1809054)`
- `1815012` zostało przesunięte z czystego `no_source_available` do `manual_backlog`:
  - źródło: `iwierzyce.e-mapa.net/wykazplanow/`
  - `planning_signal_sync` utworzył `2` sygnały formalne
  - nadal brak geometrii MPZP, więc to nie jest jeszcze coverage path
- `1811032` zostało przesunięte z czystego `no_source_available` do `manual_backlog`:
  - źródła:
    - `https://czermin-mielecki.geoportal-krajowy.pl/`
    - `https://czermin-mielecki.geoportal-krajowy.pl/plan-ogolny`
  - `planning_signal_sync` utworzył `1` formalny sygnał
  - nadal brak geometrii MPZP, więc to nie jest jeszcze coverage path
- `1803065` zostało przesunięte z czystego `no_source_available` do `manual_backlog`:
  - źródła:
    - `https://pilzno.geoportal-krajowy.pl/`
    - `https://pilzno.geoportal-krajowy.pl/plan-ogolny`
    - `https://pilzno.geoportal-krajowy.pl/mpzp`
  - `planning_signal_sync` utworzył `2` formalne sygnały
  - nadal brak geometrii MPZP, więc to nie jest jeszcze coverage path
- `1803042` zostało przesunięte z czystego `no_source_available` do `manual_backlog`:
  - źródła:
    - `https://debica.geoportal-krajowy.pl/`
    - `https://debica.geoportal-krajowy.pl/mpzp`
  - `planning_signal_sync` utworzył `1` formalny sygnał
  - publiczne `wykazplanow` nadal zwraca `Błąd połączenia em_`, więc geometrii dalej trzeba szukać osobno
- `1803052` zostało przesunięte z czystego `no_source_available` do `manual_backlog`:
  - źródła:
    - `https://jodlowa.geoportal-krajowy.pl/`
    - `https://jodlowa.geoportal-krajowy.pl/plan-ogolny`
  - `planning_signal_sync` utworzył `1` formalny sygnał
  - publiczne `wykazplanow` nadal zwraca `Błąd połączenia em_`, więc geometrii dalej trzeba szukać osobno
- `1816065` zostało przesunięte z czystego `no_source_available` do `manual_backlog`:
  - źródła:
    - `https://glogow-malopolski.geoportal-krajowy.pl/`
    - `https://glogow-malopolski.geoportal-krajowy.pl/mpzp`
  - `planning_signal_sync` utworzył `1` formalny sygnał
  - publiczna `e-mapa` pozostaje semantycznie niebezpieczna, więc geometrii dalej trzeba szukać osobno
- `1805112` zostało przesunięte z czystego `no_source_available` do `manual_backlog`:
  - źródła:
    - `https://bip.tarnowiec.eu/planowanie-przestrzenne/238`
    - `https://bip.tarnowiec.eu/projekty-mpzp/290`
    - `https://tarnowiec.eu/aktualnosc-4123-przystapienie_do_sporzadzenia_planu.html`
  - `planning_signal_sync` utworzył `2` formalne sygnały
  - nadal brak geometrii parcel-safe
- `1807025` zostało przesunięte z czystego `no_source_available` do `manual_backlog`:
  - źródła:
    - `https://dukla.geoportal-krajowy.pl/plan-ogolny`
    - `https://www.dukla.pl/pl/dla-mieszkancow/mapy-i-plany-79/wnioski-do-planu-ogolnego-226`
    - `https://www.dukla.pl/files/_source/2025/01/ogloszenie%20na%20BIP%20i%20na%20strone%20gminy.pdf`
  - `planning_signal_sync` utworzył `3` formalne sygnały
  - ścieżka geometrii POG pozostaje login-gated, więc nadal potrzebne jest obejście operatorskie albo nowe źródło
- `1819032`, `1819045` i `1820032` zostały przesunięte z czystego `no_source_available` do `manual_backlog`:
  - źródła:
    - `https://niebylec.geoportal-krajowy.pl/`
    - `https://niebylec.geoportal-krajowy.pl/plan-ogolny`
    - `https://strzyzow.geoportal-krajowy.pl/`
    - `https://strzyzow.geoportal-krajowy.pl/mpzp`
    - `https://grebow.geoportal-krajowy.pl/`
    - `https://grebow.geoportal-krajowy.pl/mpzp`
  - `planning_signal_sync` utworzył po `1` formalnym sygnale dla każdej gminy
  - nadal brak publicznej, parcel-safe geometrii
- `1807102` nie jest już traktowane jako czysty brak źródła:
  - aktywna działka wskazuje miejscowość `Szklary`, ale niespójny TERYT
  - status końcowy tej iteracji: `manual_backlog / resolver mismatch`
- `1803042` i `1803052` mają już odkryte publiczne `wykazplanow`, ale obecnie zwracają operatorowy błąd `Błąd połączenia em_`:
  - nie są już „nieznane”, tylko jawnie sklasyfikowane jako upstream blocker
  - pozostają w `manual_backlog`, dopóki nie pojawi się działający endpoint geometrii
- istnieje operatorowy fallback do zamykania rollout gaps:
  - `make quarantine-candidates`
  - `make quarantine-promote`
- pierwszy realny lead dla Podkarpackiego został utworzony na żywej bazie Cloud SQL:
  - `TERYT=1816145`
  - `identyfikator=181614502.267/3`
  - `lead_id=ca689b38-eec6-4f07-94eb-e5e7aa6c0c4f`
  - `delta_result_id=16a53476-e8da-45a7-b562-19aa20781699`
  - źródło operator fallback: `manual://quarantine_override`
  - heurystyka wyboru: listing zawiera `działki inwestycyjne widokowe z warunkami zabudowy`

### Co jest dziś głównym blockerem

- `gold.delta_results` dla Podkarpackiego nie jest już puste i ma już także automatyczny przypadek poza ręcznym fallbackiem:
  - `1816072`
  - `181607203.3/4` -> `ZL`, `55.43%`
  - `181607203.609/2` -> `ZL`, `93.43%`
  - `181607205.1484/1` -> `ZL`, `90.30%`
- `1816145` ma już także automatyczny fallback path niezależny od operatorskiego leada:
  - `181614502.267/3` -> `ZL`, `71.41%`
  - `181614502.267/4` -> `ZL`, `90.15%`
  - `181614502.267/5` -> `ZL`, `82.70%`
  - `181614502.267/6` -> `ZL`, `85.91%`
  - `181614502.273/3` -> `ZL`, `82.65%`
  - `181614502.273/4` -> `ZL`, `86.18%`
  - `181614502.273/5` -> `ZL`, `84.13%`
- `gold.investment_leads` dla Podkarpackiego ma już `1` lead operatorski, ale nie oznacza to jeszcze pełnego sukcesu coverage/source-discovery.
- wszystkie `56/56` parceli w Podkarpackiem mają już uzupełnione `current_use` przez fallback `listing_text_heuristic`:
  - `B=48`
  - `R=4`
  - `LS=3`
  - `Ł=1`
- część gmin ma już coverage, ale działki nadal nie przecinają się geometrycznie z obowiązującymi strefami:
  - `1810042` dystans rzędu `62-116 m`
  - `1810011` dystans rzędu `377-405 m`
  - `1821035` dystans rzędu `1.3-1.5 km`
  - `1816035` dystans rzędu `4.2-5.9 km`
  - `1805042` dystans rzędu `652.7-652.8 km`
- raport prowincji rozróżnia już powód `why_no_lead`:
  - `no_source`
  - `upstream_blocker`
  - `unknown_only`
  - `green_or_hard_negative`
  - `weak_signal_or_no_delta`
- po rerunach i formalnym cleanupie uncovered backlogu stan coverage blockers wynosi:
  - `no_source_available=0`
  - `source_discovered_no_parcel_match=4 dzialki`
  - `upstream_blocker=2 dzialki`
  - `manual_backlog=21 dzialki`

### Najbliższe fronty pracy

1. dla `1803042` i `1803052` utrzymywać `upstream_blocker` dopóki publiczny `wykazplanow` nie przestanie zwracać `Błąd połączenia em_`, albo nie znajdziemy alternatywnej geometrii
2. dla `1816065`, `1805112` i `1807025` utrzymywać `manual_backlog` i szukać geometrii parcel-safe
3. dla `1811032`, `1803065`, `1819032`, `1819045` i `1820032` utrzymywać `manual_backlog` i szukać geometrii parcel-safe
4. dla `1815012` utrzymywać `source_discovered_no_parcel_match` i sprawdzać nowe bbox-y / plan assets, zamiast traktować gminę jak czysty brak źródła
5. utrzymywać `1809054` jako pierwszy automatyczny buildable case i obserwować, czy nie degraduje się w kolejnych przebiegach
6. utrzymywać `1816072` jako konserwatywny automatic delta path i nie promować go do buildable bez mocniejszej semantyki niż APP title
7. utrzymywać `1816145` jako drugi konserwatywny automatic delta path; manualny `MN` lead zostaje jawnie odseparowany jako rescue path
8. utrzymywać `1808042` i `1805042` jako conservative project-coverage path bez wymyślania buildable semantyki ponad dane źródłowe
9. utrzymywać operator fallback tylko jako tor kontrolowany, nie jako substytut source discovery
10. rozdzielić w backlogu `upstream_blocker` od `source_discovered_no_parcel_match`, żeby operator wiedział, czy problemem jest zepsuty feed, czy brak pokrycia aktywnych parceli
11. utrzymywać osobny audit `current_use`, zanim pojawi się autorytatywny upstream EGiB

## 1. Włączyć `podkarpackie` w warstwie scope i operator tooling

### Zmiany wymagane

- dodać `podkarpackie` do `backend/app/services/operations_scope.py`:
  - `display_name = "Podkarpackie"`
  - `db_label = "podkarpackie"`
  - `teryt_prefix = "18"`
- rozszerzyć `backend/run_province_campaign.py`:
  - `--province` choices o `podkarpackie`
  - wszystkie raporty / snapshoty / backlog hints muszą działać dla prefixu `18`
- rozszerzyć `Makefile`:
  - `report-podkarpackie`
  - `campaign-podkarpackie`
  - `future-buildability-status PROVINCE=podkarpackie`
  - `future-buildability-backlog PROVINCE=podkarpackie`
- nie zmieniać od razu domyślnego `PROVINCES ?= slaskie malopolskie`; Podkarpackie wdrożyć najpierw jako tor opt-in

### Checklist

- [x] `operations_scope.py` obsługuje `podkarpackie`
- [x] `run_province_campaign.py` przyjmuje `--province podkarpackie`
- [x] `Makefile` ma `report-podkarpackie` i `campaign-podkarpackie`
- [x] README / runbooki mają komendy dla Podkarpackiego
- [x] testy scope normalizacji obejmują prefix `18`

## 2. Zbudować baseline danych dla Podkarpackiego

Najpierw trzeba wpuścić województwo do podstawowego lejka listingów i działek:
- scrape live dla `podkarpackie`
- geo resolve na listingach z województwa
- delta na rozpoznanych działkach
- status snapshot przed source discovery

### Wdrożeniowy baseline

```bash
make scrape-live PROVINCES="podkarpackie" MAX_PAGES=3
make geo-resolve
make delta
make report-podkarpackie
make future-buildability-status PROVINCE=podkarpackie
make future-buildability-backlog PROVINCE=podkarpackie BACKLOG_FORMAT=csv
```

### Cel baseline

- policzyć liczbę `bronze.raw_listings` z `podkarpackie`
- policzyć liczbę `silver.dzialki` z prefixem `18`
- policzyć DLQ / parser / resolver blockers
- wygenerować pierwszą listę gmin z parcelami i pierwszą listę backlogu

### Checklist

- [x] są listingi Bronze z `raw_wojewodztwo = podkarpackie`
- [x] `geo_resolve` tworzy działki z prefixem `18`
- [x] jest pierwszy raport `report-podkarpackie`
- [x] jest pierwszy backlog CSV dla `future_buildable`
- [x] DLQ jest skategoryzowane na parser / ambiguity / missing source / manual only
- [x] istnieje osobny audit `current_use`, żeby blocker był mierzalny per province / TERYT

## 3. Wdrożyć coverage MPZP/WFS dla Podkarpackiego

To jest główny etap operatorski. Bez niego województwo nie będzie dawało sensownych leadów.

### Zasada

- nie próbować od razu pokryć całego województwa ręcznie,
- najpierw priorytetyzować gminy z realnymi parcelami w `silver.dzialki`,
- budować registry od najwyższego yieldu.

### Kolejność discovery dla każdej gminy

1. publiczny SIP / geoportal z WFS
2. ArcGIS WFS / REST
3. e-mapa / JARC / lokalny MSIP
4. GISON raster / WMS-grid
5. dopiero potem manual / uncovered note

### Wymagania dla każdej nowej gminy

Każda nowa gmina dostaje:
- wpis do `WFS_REGISTRY` w `backend/run_wfs_sync.py`
- poprawny `WFSFieldMapping`
- potwierdzony `source_srid`
- udokumentowany `source_kind`
- jeśli nie ma bezpiecznego źródła wektorowego:
  - jawny status `gison_raster_candidate` albo `no_source_available`

### Rytm rolloutu

Dla Podkarpackiego wdrażamy ten rytm:
- batch 1: top 5 gmin wg liczby działek
- batch 2: kolejne 5
- batch 3+: aż do wyczerpania realnego backlogu

Po każdej paczce:

```bash
make mpzp-sync
make report-podkarpackie
```

### Checklist

- [x] top 5 gmin ma sprawdzone źródła MPZP
- [x] każda nowa gmina ma registry entry albo jawny blocker status
- [x] `mpzp-sync` ładuje `gold.planning_zones` dla prefixu `18`
- [x] `report-podkarpackie` pokazuje uncovered / covered-no-leads / intersections-no-leads
- [x] nie ma „cichych” gmin bez statusu źródła

### Obecny stan coverage MPZP

- `1810011` Łańcut:
  - aktywne źródło WFS
  - `156` stref
  - `covered_but_no_delta`
- `1810042` Gmina Łańcut / Handzlówka:
  - aktywne źródło WFS
  - `1092` stref na jeden sync, obecnie `2184` rekordy po ponownym załadowaniu
  - `covered_but_no_delta`
- `1816035` Boguchwała:
  - aktywne źródło WFS
  - `462` stref na jeden sync, obecnie `924` rekordy po ponownym załadowaniu
  - `covered_but_no_delta`
- `1821035` Lesko / Huzele:
  - aktywne źródło WFS
  - `455` stref na jeden sync, obecnie `910` rekordy po ponownym załadowaniu
  - `covered_but_no_delta`
- `1816072` Hyżne / Grzegorzówka / Szklary:
  - aktywny wpis registry przez `wms_grid`
  - źródło: `view_gml.php?plan=001` + fallback semantyczny z tytułu APP
  - plan `001` daje `parcel_match_count=3`, `wms_health=ok`, `geotiff_health=ok`
  - automatyczne parsowanie legendy nadal zwraca `0` entries, ale coverage działa już przez APP metadata fallback
  - wynik po sync + delta:
    - `planning_zones=3`
    - `delta_results=3`
    - brak leadów, bo `ZL` jest niebudowlane
- `1816145` Tyczyn / Borek Stary:
  - publiczny `wykazplanow` jest aktywny
  - plan `038` ma live `WMS` i `GeoTIFF`
  - plan `038` pokrywa `7` aktywnych parceli
  - aktywny wpis registry przez `wms_grid`
  - źródło: `view_gml.php?plan=038` + fallback semantyczny z tytułu APP
  - wynik po sync + delta:
    - `planning_zones=7`
    - `delta_results=7`
    - brak nowych leadów automatycznych, bo `ZL` jest niebudowlane
  - manualny lead `181614502.267/3` pozostaje jawnie oznaczonym rescue path przez `manual://quarantine_override`
- `1805042` Gmina Jasło / Gorajowice:
  - aktywny wpis registry przez `app_gml`
  - źródło: publiczny GISON WFS `ms:app.AktPlanowaniaPrzestrzennego.MPZP`
  - wynik po sync:
    - `planning_zones=60`
    - `delta_results=0`
    - `planning_signals_created=2`
  - status operatorski:
    - poza uncovered backlogiem
    - `covered_but_no_delta`
    - `why_no_lead=unknown_only`
  - najbliższe aktywne parcele są ~`652.7-652.8 km` od najbliższego zasięgu planu, więc obecny brak delty wynika z geometrii/zasięgu, a nie z parsera lub syncu
- `1804082` Gmina Radymno / Duńkowice:
  - brak jeszcze parcel-safe geometrii MPZP
  - `planning_signal_sync` tworzy już formalne sygnały z:
    - artykułu planu ogólnego,
    - strony `plan-ogolny`,
    - strony `mpzp` z osadzonym `mpzpRegistry`
  - wynik po sync:
    - `planning_signals_created=3`
    - `known_sources=html_index`
    - `operator_status=needs_geometry_source`
  - status operatorski tej iteracji:
    - `manual_backlog`
    - nie jest to już „brak źródła”, tylko „źródło sygnałowe istnieje, brak geometrii”
- `1809054` Narol:
  - brak jeszcze parcel-safe geometrii MPZP
  - `planning_signal_sync` tworzy już formalne sygnały z:
    - `plan-ogolny`
    - strony `mpzp` z osadzonym `mpzpRegistry`
  - wynik po sync:
    - `planning_signals_created=2`
    - `known_sources=html_index`
    - `operator_status=needs_geometry_source`
    - `max_score=30`
  - status operatorski tej iteracji:
    - `manual_backlog`
    - nie jest to już „brak źródła”, tylko „formalny rejestr MPZP istnieje, brak geometrii”
- `1815012` Iwierzyce / Wiercany:
  - aktywny wpis `planning_signal_sync` przez `iwierzyce.e-mapa.net/wykazplanow/`
  - wynik po sync:
    - `planning_signals_created=2`
    - `operator_status=needs_geometry_source`
  - status operatorski tej iteracji:
    - `source_discovered_no_parcel_match`
    - `wykazplanow` zwraca live `WMS/GeoTIFF/GML`, ale aktywne parcele nie wpadają w odkryte `bbox`
- `1816065` Głogów Małopolski / Budy Głogowskie:
  - wycofane z registry
  - status: `no_source_available`
  - powód: publiczny feed zwraca semantycznie niebezpieczne, niespójne oznaczenia
- `1816115` Sokołów Małopolski / Górno / Wólka Niedźwiedzka:
  - publiczny `wykazplanow` jest aktywny i ujawnia wiele live WMS/GeoTIFF planów
  - aktywne parcele `181611501.1241/1` i `181611507.2198` nie przecinają żadnego odkrytego `bbox`
  - status operatorski: `source_discovered_no_parcel_match`
- top 5 gmin wg liczby aktywnych parceli mają dziś jawny status operatorski:
  - `1816065` -> `no_source_available`
  - `1816145` -> aktywne źródło `wms_grid`, automatyczne `delta_rows > 0`
  - `1805042` -> aktywne źródło `app_gml`, `covered_but_no_delta`
  - `1803042` -> `no_source_available`, ale z potwierdzonym upstream blockerem `wykazplanow`
  - `1810042` -> aktywne źródło WFS, `covered_but_no_delta`
  - `1803052` -> `no_source_available`, ale z potwierdzonym upstream blockerem `wykazplanow`
  - `1804082` -> `manual_backlog`, formalne sygnały HTML są już potwierdzone
  - `1809054` -> `manual_backlog`, formalne sygnały HTML są już potwierdzone
  - `1815012` -> `source_discovered_no_parcel_match`, plan assets są już potwierdzone, ale bez przecięcia aktywnych parceli

## 4. Zbudować registry `planning_signals` dla Podkarpackiego

Po MPZP wektorowym trzeba dołożyć przyszłą warstwę planistyczną:
- html index / plan ogólny / rejestry urbanistyczne
- studium / POG PDF i GML
- mpzp project / planning resolution
- jawne `unknown` lub `green` gdy to prawdziwy wynik

### Zmiany

- dodać registry source entries do `backend/app/services/planning_signal_sync.py`
- dodać testy obecności i probe’owalności do `backend/tests/test_planning_signal_sync.py`
- dla każdej gminy z dodatnim sygnałem robić od razu rerun po `TERYT`

### Dla każdej gminy po dodaniu źródła

```bash
make planning-signal-sync TERYT=<teryt>
make future-buildability TERYT=<teryt> BATCH_SIZE=50
```

### Checklist

- [x] `planning_signal_sync.py` ma pierwszy batch źródeł dla Podkarpackiego
- [x] testy registry przechodzą
- [x] każda gmina z nowym źródłem ma od razu rerun po `TERYT`
- [x] sygnały `unknown-only` są odseparowane od realnych pozytywnych sygnałów
- [x] dla każdego „braku leada” wiadomo, czy powodem jest brak source, słaby sygnał, `green`, czy blocker upstream

### Obecny batch planning signals

- źródła i testy zostały dodane dla pierwszego batcha:
  - `1805042`
  - `1808042`
  - `1816035`
  - `1804082`
  - `1805112`
  - `1807025`
  - `1809054`
- aktualny problem nie leży już tylko po stronie `planning_signals`; głównym blockerem są:
  - brak kolejnych źródeł MPZP
  - brak intersections w `delta`
  - brak `current_use`

## 4a. Etap `current_use` dla `silver.dzialki`

`current_use` jest dziś osobnym frontem roboczym i nie jest wypełniany przez standardowy `GeoResolver`.

### Co zostało wdrożone

- istnieje osobny audit:
  - `make current-use-status`
- istnieje bezpieczny bridge do backfillu ręcznie zweryfikowanych kodów:
  - `make current-use-template PROVINCE=podkarpackie OUTPUT=...`
  - `make current-use-backfill PROVINCE=podkarpackie INPUT=...`
- writer path jest idempotentny:
  - domyślnie działa jako `dry-run`
  - bez `OVERWRITE=1` dotyka tylko pustych `current_use`

### Czego jeszcze brakuje

- brak autorytatywnego upstreamu EGiB dla `current_use`
- obecny provider automatyczny to fallback `listing_text_heuristic`, więc pole jest już operacyjnie wypełniane, ale wymaga późniejszej walidacji biznesowej
- pełna automatyzacja klasy EGiB nadal wymaga nowego, wiarygodnego źródła albo kontrolowanego importu manual review / CSV

### Checklist

- [x] istnieje osobny audit `current_use`
- [x] istnieje writer path dla ręcznie zweryfikowanego importu CSV
- [x] istnieje automatyczny provider danych EGiB dla `current_use`
- [x] Podkarpackie ma uzupełnione `current_use` dla aktywnych parceli

## 5. Uruchomić rollout `future_buildable` dla Podkarpackiego

Gdy baseline i pierwsze źródła są gotowe:
- odpalić status
- przejść batchami po backlogu
- dopiero potem zrobić pełną kampanię wojewódzką

### Rekomendowany rollout

1. `future-buildability-status PROVINCE=podkarpackie`
2. targeted reruns po gminach z najwyższym `max_overall_score`
3. pierwszy pełny `campaign-podkarpackie`
4. ponowny status i audyt coverage

### Nowe komendy

```bash
make future-buildability-status PROVINCE=podkarpackie
make future-buildability-backlog PROVINCE=podkarpackie BACKLOG_FORMAT=csv
make campaign-podkarpackie
```

`campaign-podkarpackie` powinien działać analogicznie do obecnych województw:
- `run_province_campaign.py --province podkarpackie --stage full --autofix --parallel`

### Checklist

- [x] `future-buildability-status` działa dla `podkarpackie`
- [x] backlog export działa dla `podkarpackie`
- [x] pierwszy pełny `campaign-podkarpackie` kończy się bez crasha
- [x] wynik zawiera:
  - coverage summary
  - uncovered gminy
  - covered-no-leads
  - backlog hints
- [x] co najmniej jeden rerun po `TERYT` został wykonany end-to-end

### Obecny stan future buildability

- pipeline działa operacyjnie i ma już pierwszy realny lead operatorski dla Podkarpackiego:
  - `planning_zones=4341`
  - `future_buildability_assessments=56`
  - `delta_rows>=11`
  - `leads=1`
- pierwszy lead został wygenerowany przez kontrolowany fallback `quarantine -> manual override -> lead`, a nie przez automatyczne przecięcie MPZP
- pierwszy automatyczny sukces poza ręcznym fallbackiem został osiągnięty w `1816072`, a następnie rozszerzony na `1816145`, ale oba przypadki są dziś non-buildable `ZL`, więc bez nowego automatycznego leada
- to oznacza, że etap osiągnął kryterium „pierwszy lead”, ale nadal wymaga dalszego source discovery do skalowania bez ręcznej interwencji

### Operator fallback do pierwszego leada

Jeśli rollout utknie na `covered_but_no_delta` albo `no_source_available`, a listing ma bardzo mocny sygnał inwestycyjny, używamy kontrolowanego fallbacku operatorskiego:

```bash
make quarantine-candidates PROVINCE=podkarpackie LIMIT=8
make quarantine-promote PROVINCE=podkarpackie TERYT=1816145 SOURCE_HINT=warunkami-zabudowy MANUAL_PRZEZNACZENIE=MN
```

Ten tor:
- wybiera najwyżej punktowane parcele bez leada,
- tworzy syntetyczną `planning_zone`,
- zapisuje `delta_result`,
- promuje działkę do `investment_lead`.

### Checklist uzupełniająca — zamknięcie fazy

- [x] istnieje powtarzalna komenda do listowania kandydatów z kwarantanny
- [x] istnieje powtarzalna komenda do promocji manual override
- [x] pierwszy realny lead dla Podkarpackiego został utworzony end-to-end
- [x] pierwszy automatyczny `delta_rows > 0` dla Podkarpackiego został osiągnięty bez `manual://quarantine_override`
- [x] plan i runbook dokumentują tor operatorski

## 6. Rozszerzyć GCP jobs i harmonogram dopiero po stabilizacji

Nie dodawać `podkarpackie` do stałego schedulera od razu. Najpierw rollout manualny, potem scheduler.

### Etap 1

- manual jobs przez:
  - `gcp-job-scrape-live`
  - `gcp-job-geo-resolve`
  - `gcp-job-delta`
  - `gcp-job-planning-signal-sync`
  - `gcp-job-future-buildability --province podkarpackie`
  - `gcp-job-campaign-rollout --province podkarpackie`

### Etap 2

- dopiero po stabilnym przejściu 2–3 pełnych kampanii:
  - dodać `podkarpackie` do regularnego batchu dziennego / nocnego
  - rozszerzyć default runbook z `slaskie malopolskie` do `slaskie malopolskie podkarpackie`

W Cloud Run job layer nic nie trzeba zmieniać w nazwach usług; wystarczy, żeby istniejące job entrypointy przyjmowały `--province podkarpackie`, a kod kampanii i statusów znał prefix `18`.

### Checklist

- [x] manual `gcp-job-*` działa dla `--province podkarpackie`
- [x] nightly scheduler nie obejmuje jeszcze Podkarpackiego przed stabilizacją
- [x] po stabilizacji Podkarpackie trafia do regularnych batchy
- [x] runbook produkcyjny opisuje moment przejścia z manual rollout do scheduled operations

## Public Interfaces / Types / Commands to Add

- `backend/app/services/operations_scope.py`
  - dodać `podkarpackie` do `_PROVINCE_SPECS`
- `backend/run_province_campaign.py`
  - `--province` choices: `slaskie`, `malopolskie`, `podkarpackie`
- `Makefile`
  - `report-podkarpackie`
  - `campaign-podkarpackie`
  - ewentualnie później `campaign-all` rozszerzyć o Podkarpackie, ale dopiero po stabilizacji
- operator docs / runbooki:
  - dodać Podkarpackie do checklist, ale z adnotacją rollout-stage

## Test Plan

### Tooling / scope

- `normalize_province("podkarpackie") == "podkarpackie"`
- `province_db_label("podkarpackie") == "podkarpackie"`
- `province_teryt_prefix("podkarpackie") == "18"`
- `run_province_campaign.py --province podkarpackie --stage report` uruchamia się poprawnie

### Data pipeline

- `scrape-live PROVINCES="podkarpackie"` tworzy Bronze rows
- `geo-resolve` tworzy `silver.dzialki` z prefixem `18`
- `delta` uruchamia się bez błędów dla działek z prefixem `18`
- `mpzp-sync` ładuje pierwsze `gold.planning_zones` dla Podkarpackiego
- `planning-signal-sync TERYT=<teryt>` tworzy lub aktualizuje `gold.planning_signals`

### Business acceptance

- backlog report dla Podkarpackiego rozróżnia:
  - `no_source_configured`
  - `source_configured_but_not_loaded`
  - `covered_but_no_delta`
  - `covered_but_no_buildable_delta`
- po pierwszym rollout-campaign istnieje jawna lista:
  - gmin aktywnych
  - gmin uncovered
  - gmin blocked
  - gmin near-threshold
- minimum acceptance dla fazy rollout:
  - województwo ma działający report
  - ma działający backlog export
  - ma pierwsze planning zones
  - ma co najmniej jeden sensowny batch future-buildability
- minimum acceptance dla fazy produkcyjnej:
  - województwo przechodzi pełny `campaign-podkarpackie`
  - co najmniej jeden `future_buildable` albo jawnie udokumentowany brak takich leadów z przyczyn source/blocker, nie z powodu luki toolingowej

## Aktualne kolejne kroki

1. Dalszy rollout traktować już jako fazę optymalizacji coverage, nie brak toolingowy.
2. Priorytet source discovery:
   - `1803042`
   - `1803052`
   - `1808042`
   - `1811032`
   - `1815012`
3. `1816072` i `1816145` zostawić jako konserwatywne, automatyczne ścieżki `ZL` i nie promować ich do buildable bez nowej semantyki źródła.
4. Utrzymywać `current_use` przez `listing_text_heuristic` albo CSV bridge do czasu pozyskania upstreamu klasy EGiB.
5. Po każdej nowej gminie robić:
   - targeted `mpzp-sync`
   - `report-podkarpackie`
   - targeted `future-buildability`
   - ocenę czy pojawiły się pierwsze realne intersections lub pierwszy lead

## Assumptions and Defaults

- Używamy klucza województwa `podkarpackie`.
- TERYT prefix dla województwa to `18`.
- `Cloud SQL` pozostaje źródłem prawdy.
- Rollout Podkarpackiego zaczynamy manualnie i batchowo; nie dodajemy go od razu do domyślnego `PROVINCES` ani do schedulera.
- Najpierw domykamy operator tooling i baseline danych, potem source discovery, potem `future_buildability`, a dopiero na końcu harmonogram stały.
- Brak obecnego registry dla Podkarpackiego traktujemy jako expected initial state, nie jako awarię.
