# TerraZoning — Recent Changes (April 2026)

Ten dokument podsumowuje najważniejsze zmiany z ostatnich kilku kroków implementacyjnych oraz ich efekt na danych i UX.

---

## 1. DeltaEngine — normalizacja symboli planistycznych

### Zmiana

W `delta_engine.py` dodano normalizację symboli MPZP:
- `19.MN -> MN`
- `MN.1 -> MN`
- `U/MN-3 -> U/MN`
- `1MNU -> MNU`

Rozszerzono też bazowy słownik buildable o warianty mieszane:
- `MNU`
- `MU`
- `U/MW`
- `MW/U`

### Efekt

- Małopolskie przestało blokować się na `Typology Mismatch`
- po tej zmianie pojawiły się pierwsze realne leady z Zakopanego

---

## 2. DeltaEngine — filtry jakości przestrzennej

### Zmiana

Lead jest generowany tylko, gdy:
- `coverage_pct >= 30%`
- lub `intersection_area_m2 >= 500`

Do `delta_results` trafiają już metryki przestrzenne używane później w rankingu:
- `coverage_pct`
- `intersection_area_m2`

### Efekt

- odfiltrowane zostały drobne, przypadkowe przecięcia
- ranking leadów stał się bliższy realnej użyteczności inwestycyjnej

---

## 3. Parser cen — poprawa precyzji

### Zmiana

W `scraper/scraper/extractors/price.py` parser cen zaczął priorytetowo szukać:
- `cena wywołania`
- `cena wywoławcza`
- `suma oszacowania`
- `wartość oszacowania`
- `za cenę nie niższą niż`
- `trzy czwarte sumy oszacowania`

Jednocześnie ignorowane są konteksty poboczne:
- `najniższe postąpienie`
- `wadium`
- `rękojmia`
- wzmianki o udziałach i ułamkach

### Efekt

- poprawiono `37` rekordów Bronze z błędnych, skrajnie niskich cen
- odzyskano `46` cen, które wcześniej były `NULL`
- stan końcowy Bronze:
  - `85/85` rekordów z ceną
  - `0` rekordów z ceną `< 1000 zł`

---

## 4. DeltaEngine — refresh `price_per_m2_zl`

### Zmiana

Backfill cen na leadach przestał działać tylko dla braków. Teraz `delta_engine.py` odświeża `price_per_m2_zl`, jeśli cena źródłowa w Bronze zmieniła się po reparsie.

### Efekt

- wszystkie `15/15` leadów mają już `price_per_m2_zl`
- ranking cenowy w kokpicie przestał bazować na artefaktach typu `98 zł`

Przykład po poprawce:
- gliwickie leady mają już cenę `9 789,16 zł`
- `price_per_m2_zl` wróciło do zakresu `4.22–118.70 zł/m²`

---

## 5. Frontend — metryki inwestorskie

### Zmiana

W UI dodano:
- `Cena wejścia`
- `zł/m²`
- `Pow. budowlana`
- `Pokrycie MPZP`
- `Sygnał ceny`
- `Snapshot Inwestycyjny`

Zmiany objęły m.in.:
- `LeadDetail.tsx`
- `LeadList.tsx`
- `InvestorSnapshot.tsx`
- `investorMetrics.ts`

### Efekt

Kokpit stał się narzędziem decyzyjnym dla inwestora, a nie tylko listą technicznych obiektów GIS.

---

## 6. Frontend — sortowanie i filtr jakości ceny

### Zmiana

Dodano sortowanie listy leadów po:
- `confidence`
- `zł/m²`
- `cenie wejścia`
- `powierzchni budowlanej`

Dodano filtr jakości ceny:
- `wszystkie`
- `wiarygodne`
- `do weryfikacji`
- `brak ceny`

Domyślny widok ustawiono na:
- `priceFilter = reliable`

### Efekt

- inwestor startuje od czyściejszego zestawu leadów
- `13` leadów wygląda dziś cenowo wiarygodnie
- `2` pozostają w koszyku `do weryfikacji`

---

## 7. Frontend — widoczność leadów na mapie

### Zmiana

Mapa dostała dodatkowe warstwy widoczności:
- centroidowe markery
- halo na niskim zoomie
- większy `hit area`
- wyraźniejsze zaznaczenie wybranego leada

### Efekt

Leady nie są już mikro-kropkami widocznymi dopiero przy głębokim zoomie.

---

## 8. Human-in-the-Loop / Kwarantanna

### Zmiana

Dodano obsługę `quarantine_parcels`:
- `GET /api/v1/quarantine_parcels`
- `POST /api/v1/quarantine_parcels/{dzialka_id}/manual_override`

Frontend ma osobną sekcję działek w kwarantannie oraz możliwość ręcznego `manual_przeznaczenie`.

### Efekt

System dostał praktyczny tryb MVP dla gmin bez pełnego coverage MPZP i dla przypadków wymagających ręcznej decyzji analityka.

---

## 9. Coverage MPZP

### Zmiana

W ostatnich krokach dopięto kolejne źródła oraz fallbacki:
- klasyczny WFS
- `wms_grid`
- lokalne integracje dla trudniejszych JST

### Efekt

Stan `gold.planning_zones` urósł do:
- `39 655`

Jednocześnie potwierdziliśmy, że część gmin nadal wystawia tylko:
- APP / dokumenty
- granice planów
- puste warstwy schematyczne

To jest dziś główna blokada dalszego wzrostu leadów.

---

## 10. Najważniejszy efekt biznesowy

W ostatnich kilku krokach system przeszedł z etapu:
- „mamy leady, ale część cen i rankingu wygląda podejrzanie”

do etapu:
- „mamy stabilne leady, komplet cen w Bronze, komplet `price_per_m2` w Gold i inwestorski kokpit z sensownym domyślnym widokiem”

Najważniejsze liczby po tej serii zmian:
- `bronze.raw_listings = 85`
- `silver.dzialki = 93`
- `gold.planning_zones = 39 655`
- `gold.investment_leads = 15`
- `15/15` leadów z `price_per_m2_zl`
- `13/15` leadów w koszyku `wiarygodne ceny`

