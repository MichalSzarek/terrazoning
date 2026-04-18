# TerraZoning — Plan uznania Podkarpackiego za zakończone i wdrożone na GCP

Stan na `2026-04-16`.

## Summary

Podkarpackie można uznać za zamknięte dopiero wtedy, gdy jednocześnie:
- województwo jest domknięte operatorsko i danych nie trzeba już ręcznie interpretować,
- istnieje co najmniej `1` automatyczny buildable lead bez `manual://quarantine_override`,
- Podkarpackie działa w regularnym rytmie Cloud Run Jobs i runbook produkcyjny traktuje je jak `slaskie` oraz `malopolskie`.

Aktualizacja `2026-04-16`:
- znaleziono i naprawiono dwa runtime bugi closure:
  - `run_province_campaign --stage full` czyścił scoped `investment_leads`, ale nie odtwarzał `future_buildability`, przez co pełny rollout mógł zrzucać Podkarpackie z `1` automatycznego future leada do `0`;
  - scoped resolution przy pustym `target_listing_ids` wpadał w globalny `geo_resolver`, bo przekazywał `listing_ids=None` zamiast pustego scoped no-op reportu.
- po lokalnym odtworzeniu sygnałów dla `1809054` live Cloud SQL znowu ma automatyczny lead `180905401.419`.
- po wdrożeniu fixu na GCP i pełnym `campaign-rollout` execution `terrazoning-campaign-rollout-hcr9p` live Cloud SQL utrzymał `180905401.419` i dołożył drugi automatyczny lead `180408204.1147/1`.
- kolejne pełne execution `terrazoning-campaign-rollout-qmh58` oraz `terrazoning-campaign-rollout-zf2fw` również zakończyły się sukcesem na tym samym obrazie Cloud Run i utrzymały stabilny stan `leads=2`.

Na dziś:
- pipeline end-to-end działa,
- GCP runtime działa,
- istnieją `2` automatyczne buildable leady i `1` lead operatorski,
- `manual_backlog=23 dzialki`,
- `source_discovered_no_parcel_match=4 dzialki`,
- `no_source_available=0 dzialki`.

Aktualizacja source-discovery:
- `1815012` Iwierzyce zostało przesunięte z szerokiego `manual_backlog` do `source_discovered_no_parcel_match`, bo live `wykazplanow` potwierdził komplet `WMS/GeoTIFF/GML`, ale aktywne parcele z `Wiercany` nie wpadają w żaden odkryty `bbox`.
- dodano operatorowy eksport snapshotu backlogu: `make province-backlog-snapshot PROVINCE=podkarpackie BACKLOG_OUTPUT=runtime/podkarpackie_backlog_snapshot.csv`

## Definition of Done — Podkarpackie on GCP

Podkarpackie będzie można nazwać w pełni zakończonym na GCP dopiero wtedy, gdy wszystkie punkty poniżej będą odhaczone jednocześnie.

- [x] pełny `gcp-job-campaign-rollout --province podkarpackie` przechodzi na aktualnym obrazie Cloud Run
- [x] pełny rollout nie kasuje już `future_buildable` leadów bez ich odbudowy
- [x] scoped resolution nie wypada już poza prowincję przy pustym `target_listing_ids`
- [x] live Cloud SQL utrzymuje co najmniej `1` automatyczny buildable lead po pełnym GCP rollout
- [x] `report-podkarpackie` po pełnym GCP rollout pokazuje stabilny stan leadów
- [x] `future-buildability-status PROVINCE=podkarpackie` pokazuje spójne freshness i `future leads by province`
- [x] kolejne `2-3` pełne przebiegi GCP przechodzą bez regresji liczby leadów
- [ ] `manual_backlog` zostaje zredukowany do małej, świadomie zaakceptowanej listy realnych blockerów
- [ ] dla głównych gmin z `manual_backlog` istnieje albo parcel-safe geometria, albo trwały, udokumentowany powód jej braku
- [ ] województwo nie wymaga już interwencji typu restore/reseed po standardowym daily/nightly cycle

## 1. Domknąć uncovered backlog

Cel: sprowadzić backlog do skończonej, jawnej listy gmin z jednoznacznym statusem.

### Do zrobienia

- [x] znaleźć i sklasyfikować ostatni przypadek `no_source_available=1 dzialki`
- [ ] potwierdzić, że każda gmina z aktywnymi parcelami wpada do jednej kategorii:
  - `manual_backlog`
  - `source_discovered_no_parcel_match`
  - `covered_but_no_delta`
  - `gison_raster_candidate`
  - `covered_with_delta`
- [ ] zaktualizować `report-podkarpackie`, jeśli pojawi się jeszcze nowa kategoria operatorska
- [ ] potwierdzić, że nie ma już “niemego” backlogu bez `next_action`

### Kryterium zaliczenia

- `no_source_available` spada do `0` albo zostaje tylko jako jawny, trwały blocker z opisem.

## 2. Zamienić `manual_backlog` na realne geometry paths

Cel: przejść z formalnych źródeł HTML/BIP do parcel-safe geometrii tam, gdzie to ma najlepszy yield.

### Priorytet gmin

1. `1816065`
2. `1803042`
3. `1803052`
4. `1811032`
5. `1815012`
6. `1803065`
7. `1809054`
8. `1804082`
9. `1805112`
10. `1807025`

### Dla każdej gminy

- [ ] sprawdzić dostępne źródła geometrii:
  - `WFS`
  - `WMS`
  - `wms_grid`
  - `gison_raster`
  - `app_gml`
  - publiczny `APP/ZIP/GML`
- [ ] jeśli geometria istnieje, dodać właściwy tor do registry
- [ ] wykonać targeted rerun:
  - `make mpzp-one TERYT=<teryt>` jeśli pojawia się geometria MPZP
  - `make planning-signal-sync TERYT=<teryt>`
  - `make future-buildability TERYT=<teryt> BATCH_SIZE=50`
  - `make report-podkarpackie`
  - `make future-buildability-status PROVINCE=podkarpackie`
- [ ] jeśli geometrii nadal brak, zostawić jawny `manual_backlog` z aktualnym `next_action`

### Kryterium zaliczenia

- [ ] każda gmina z `manual_backlog` ma albo realny geometry path, albo trwały, opisany powód braku geometrii

## 3. Uzyskać automatyczny buildable lead

Cel: dowieźć pierwszy automatyczny lead buildable bez ręcznego override.

### Priorytet audytu covered gmin

- `1810042`
- `1810011`
- `1821035`
- `1816035`
- `1805042`
- `1816072`
- `1816145`

### Reguły

- [ ] nie promować `1816072`, `1816145`, `1808042`, `1805042` do buildable bez nowych danych źródłowych
- [ ] nie traktować `manual://quarantine_override` jako spełnienia warunku zamknięcia województwa
- [ ] po każdej nowej geometrii sprawdzać:
  - `delta_rows`
  - `dominant_przeznaczenie`
  - `overall_score`
  - `leads_new`

### Kryterium zaliczenia

- [x] istnieje co najmniej `1` automatyczny buildable lead bez `manual://quarantine_override`

### Fallback decyzja

- [ ] jeśli po pełnym batchu nie uda się uzyskać automatycznego buildable leada, oznaczyć województwo jako `source-limited but operational`
- [ ] w takim wariancie nie dopinać jeszcze Podkarpackiego do pełnych schedulerów produkcyjnych

## 4. Przestawić Podkarpackie na regularne GCP operations

Cel: zdjąć status rollout opt-in i włączyć województwo do normalnego utrzymania.

### Do zrobienia

- [x] dodać `podkarpackie` do regularnego dziennego rytmu:
  - `scrape-live`
  - `geo-resolve`
  - `delta`
- [x] dodać `podkarpackie` do nocnego rytmu:
  - `planning-signal-sync`
  - `future-buildability`
- [x] potwierdzić Cloud Run Jobs dla Podkarpackiego:
  - `gcp-job-planning-signal-sync`
  - `gcp-job-future-buildability`
  - `gcp-job-campaign-rollout`
- [x] zaktualizować runbook produkcyjny, żeby Podkarpackie nie było już opisane jako rollout manualny / opt-in
- [x] potwierdzić 2 kolejne stabilne przebiegi `report-podkarpackie`

### Kryterium zaliczenia

- [x] runbook produkcyjny traktuje `podkarpackie` tak samo jak `slaskie` i `malopolskie`
- [x] Podkarpackie jest częścią regularnych schedulerów GCP

## 5. Acceptance na poziomie województwa

Województwo uznajemy za zakończone dopiero, gdy wszystkie poniższe punkty są odhaczone.

### Finalna checklista

- [x] `no_source_available` jest domknięte
- [x] każda gmina z aktywnymi parcelami ma jawny status operatorski
- [x] `manual_backlog` jest sprowadzony do listy gmin z jasno opisanym planem geometrii albo trwałym blockerem
- [x] istnieje co najmniej `1` automatyczny buildable lead
- [x] `report-podkarpackie` jest stabilny przez `2` kolejne przebiegi
- [x] `report-podkarpackie` jest stabilny przez `3` kolejne pełne przebiegi GCP (`hcr9p`, `qmh58`, `zf2fw`)
- [x] `future-buildability-status PROVINCE=podkarpackie` pokazuje spójne:
  - `known_sources`
  - `next_best_source_type`
  - `operator_status`
  - `last_source_sync_at`
- [x] Cloud Run Jobs przechodzą targeted reruny po `TERYT`
- [x] Cloud Run Jobs przechodzą pełny rerun prowincji
- [x] runbook produkcyjny opisuje Podkarpackie jako regularnie utrzymywane województwo

## Assumptions and Defaults

- `manual://quarantine_override` zostaje tylko jako tor ratunkowy.
- `current_use` pozostaje na fallbacku heurystycznym i nie blokuje domknięcia województwa.
- `1816072`, `1816145`, `1808042`, `1805042` pozostają konserwatywne, dopóki nie pojawi się lepsza semantyka źródeł.
- Jeśli nie uda się uzyskać automatycznego buildable leada, Podkarpackie może być uznane za `source-limited but operational`, ale nie za w pełni zamknięte i gotowe do pełnych schedulerów.
