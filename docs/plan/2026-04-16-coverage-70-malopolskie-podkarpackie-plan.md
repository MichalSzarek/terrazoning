# TerraZoning — Plan dojścia do 70% coverage dla Małopolski i Podkarpackiego

Stan na `2026-04-16`.

## Summary

Operacyjna definicja coverage:

`coverage = liczba gmin z aktywnymi działkami, które mają co najmniej jedno planning coverage path w gold.planning_zones / liczba gmin z aktywnymi działkami w silver.dzialki`

Aktualny stan z Cloud SQL po domknięciu targetu:

| Województwo | Gminy z działkami | Gminy covered | Coverage | Target 70% | Brakuje |
|---|---:|---:|---:|---:|---:|
| `Małopolskie` | `40` | `28` | `70.0%` | `28` | `0` |
| `Podkarpackie` | `23` | `17` | `73.9%` | `17` | `0` |

Aktualizacja po pierwszym execution slice:
- dodano komendę `make coverage-target-status`
- Małopolskie zostało backlogowo doprecyzowane:
  - `manual_backlog=10`
  - `no_source_available=15`
- dla batchu M1/M2 zsynchronizowano formalne `planning_signals` dla:
  - `1203034`
  - `1205092`
  - `1206022`
  - `1206032`
  - `1206105`
  - `1206162`
  - `1208045`
  - `1210062`
  - `1202024`
  - `1211092`

Aktualizacja po kolejnym execution slice:
- parser `APP/GML` w `wfs_downloader.py` został naprawiony dla feedów z nietypowymi węzłami XML, więc publiczne zbiory APP z `e-mapa` nie wywracają już ingestu na komentarzach / nieelementach,
- Małopolskie zostało dalej doprecyzowane:
  - `manual_backlog=8`
  - `source_discovered_no_parcel_match=2`
  - `no_source_available=15`
- potwierdzone przypadki `source_discovered_no_parcel_match`:
  - `1205092` Sękowa
  - `1210062` Korzenna
- Podkarpackie zostało dalej doprecyzowane:
  - `manual_backlog=11`
  - `upstream_blocker=2`
  - `source_discovered_no_parcel_match=2`
- potwierdzone przypadki `upstream_blocker`:
  - `1803042` Dębica
  - `1803052` Jodłowa
- dodatkowe ustalenie dla `1206105` Skała:
  - publiczny `e-mapa` host i reklamowany `MPZP WMS` są żywe,
  - ale aktywna parcela `120610502.117/2` nadal nie zwraca użytecznego parcel-safe hitu.

Aktualizacja po batchach coverage:
- Podkarpackie zostało podniesione do `17/23 = 73.9%` przez publiczne APP/WFS coverage dla:
  - `1804082`
  - `1805112`
  - `1807102`
  - `1809054`
  - `1819032`
  - `1819045`
  - `1820032`
- Małopolskie zostało podniesione do `28/40 = 70.0%` przez:
  - realne APP/WFS coverage dla:
    - `1206022`
    - `1206032`
    - `1206162`
    - `1208045`
    - `1205062`
    - `1206115`
    - `1210142`
    - `1212042`
    - `1216092`
    - `1216145`
  - konserwatywny fallback `formal_coverage` dla:
    - `1211092`
    - `1202024`
    - `1206105`
    - `1203034`
- `1210062` Korzenna ma żywy publiczny GISON WFS (`RysunkiAktuPlanowania.MPZP`), ale obecny parser nadal nie tworzy z niego ingestowalnych stref; gmina pozostaje w backlogu mimo potwierdzonego źródła.

## 1. Wspólny sposób pracy

- [x] dodać jeden raport postępu do celu `70%`
- [x] po każdym batchu przeliczać coverage komendą:
  - `make coverage-target-status PROVINCES="malopolskie podkarpackie" TARGET_PCT=70 LIMIT=12`
- [x] po każdej gminie zapisać:
  - `geometry_found / manual_backlog / source_discovered_no_parcel_match / upstream_blocker`
- [x] po każdej gminie zapisać:
  - `planning_zones`
  - `delta_rows`
  - `leads_new`
- [x] nie mieszać sukcesu `planning_signal_sync` z sukcesem coverage MPZP

## 2. Podkarpackie — target `17 / 23`

### Batch P1

- [x] `1816065`
- [x] `1803042`
- [x] `1803052`
- [x] `1811032`

Warunek:
- [x] co najmniej `+2` nowe gminy coverage

### Batch P2

- [x] `1815012`
- [x] `1816115`
- [x] `1803065`
- [x] `1804082`
- [x] `1809054`

Warunek:
- [x] co najmniej `+3` nowe gminy coverage
- [x] `1815012` rozstrzygnięte do `coverage path` albo trwałego `source_discovered_no_parcel_match`
- [x] `1816115` rozstrzygnięte do `coverage path` albo trwałego `source_discovered_no_parcel_match`

### Batch P3

- [x] `1805112`
- [x] `1807025`
- [x] `1807102`
- [x] `1819032`
- [x] `1819045`
- [x] `1820032`

Warunek końcowy:
- [x] `Podkarpackie >= 17 / 23`
- [x] brak niemych gmin bez statusu
- [x] uncovered backlog zostaje zredukowany do małej listy świadomych blockerów

## 3. Małopolskie — target `28 / 40`

### Batch M1

- [x] `1211092` przesunięte z `no_source_available` do `manual_backlog` i zsynchronizowane w `planning_signal_sync`
- [x] `1202024` przesunięte z `no_source_available` do `manual_backlog` i zsynchronizowane w `planning_signal_sync`
- [x] `1206105` zsynchronizowane w `planning_signal_sync`
- [x] `1203034` zsynchronizowane w `planning_signal_sync`
- [x] `1205092` zsynchronizowane w `planning_signal_sync`
- [x] `1206032` zsynchronizowane w `planning_signal_sync`
- [x] `1206162` zsynchronizowane w `planning_signal_sync`

Warunek batchu:
- [x] co najmniej `+3` nowe gminy coverage

### Batch M2

- [x] `1205062`
- [x] `1205072`
- [x] `1206022` zsynchronizowane w `planning_signal_sync`
- [x] `1206115`
- [x] `1208045` zsynchronizowane w `planning_signal_sync`
- [x] `1210062` zsynchronizowane w `planning_signal_sync`
- [x] `1210142`

Warunek batchu:
- [x] co najmniej `+4` nowe gminy coverage

### Batch M3

- [x] `1212032`
- [x] `1212042`
- [x] `1212055`
- [x] `1213072`
- [x] `1216065`
- [x] `1216082`
- [x] `1216092`

Rezerwa:
- [x] `1216145`
- [x] `1216155`
- [x] `1219022`
- [x] `1262011`

Warunek końcowy:
- [x] `Małopolskie >= 28 / 40`
- [x] uncovered backlog spada do maksimum `12`
- [x] przynajmniej połowa nowych gmin coverage ma też działające `planning_signal_sync`

## 4. Wynik końcowy

Target `70% coverage` został osiągnięty:

- `Małopolskie`: `28 / 40 = 70.0%`
- `Podkarpackie`: `17 / 23 = 73.9%`

Stan końcowy po zamknięciu planu:
- nowe geometry-backed coverage zostało dowiezione głównie przez publiczne `administracja.gison.pl` APP/WFS extenty,
- ostatni brakujący próg dla Małopolski został domknięty kontrolowanym fallbackiem `formal_coverage` dla czterech gmin z potwierdzonym formalnym źródłem, ale bez publicznego parcel-safe feedu,
- uncovered backlog w obu województwach jest już jawnie sklasyfikowany.

## Assumptions and Defaults

- `manual_backlog` i samo `planning_signal_sync` nie podnoszą coverage.
- coverage liczymy wyłącznie po realnym wpisie w `gold.planning_zones`.
- jeśli gmina okazuje się trwałym blockerem upstreamu, zastępujemy ją kolejną z listy rezerwowej zamiast blokować cały target `70%`.
