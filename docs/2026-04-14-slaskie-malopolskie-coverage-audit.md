# TerraZoning — Audyt Coverage Śląskie / Małopolskie

Stan na `2026-04-14 18:29 CEST`.

Ten dokument podsumowuje:
- obecne pokrycie danych dla `Śląskiego` i `Małopolskiego`,
- realny stan lejka `future_buildable`,
- największe luki i blokery,
- rekomendowaną kolejność dalszej pracy.

## 1. Executive Summary

TerraZoning jest już operacyjny jako radar inwestycyjny dla dwóch województw:
- `current_buildable` działa stabilnie i generuje leady,
- `future_buildable` działa produkcyjnie na Cloud SQL i generuje już wielokrotne leady `supported`,
- największym dalszym lewarem nie jest dziś scraper ani GeoResolver, tylko:
  - dalsze source discovery dla gmin bez `planning_signals`,
  - eliminacja upstream blockerów,
  - domykanie gmin z niskim, ale dodatnim `overall_score`.

Aktualne liczniki:

| Metryka | Wartość |
|---|---:|
| `current_buildable` leady | `20` |
| `future_buildable` leady | `26` |
| Gminy z parcelami w `Małopolsce` | `37` |
| Gminy z parcelami w `Śląskiem` | `22` |
| Gminy z `planning_signals` w `Małopolsce` | `17` |
| Gminy z `planning_signals` w `Śląskiem` | `14` |
| Gminy bez `planning_signals` w `Małopolsce` | `20` |
| Gminy bez `planning_signals` w `Śląskiem` | `8` |
| Gminy z `future_buildable` w `Małopolsce` | `10` |
| Gminy z `future_buildable` w `Śląskiem` | `8` |
| Gminy z backlogiem `future_buildable` w `Małopolsce` | `14` |
| Gminy z backlogiem `future_buildable` w `Śląskiem` | `10` |

## 2. Coverage Wojewódzki

### Małopolskie (`12`)

| Metryka | Wartość |
|---|---:|
| Gminy z parcelami | `37` |
| Gminy z `planning_signals` | `17` |
| Gminy bez `planning_signals` | `20` |
| Gminy z `future_buildable` | `10` |
| Gminy z `current_buildable` | `2` |
| Gminy z backlogiem `future_buildable` | `14` |

Najważniejsze aktywne gminy z `future_buildable`:
- `1201065`
- `1203034`
- `1205092`
- `1206032`
- `1206105`
- `1206114`
- `1206152`
- `1210062`
- `1215082`
- `1262011`

Największe luki coverage:
- `1213062` — dużo parceli (`25`), są `planning_signals`, ale nadal brak leada `future_buildable`
- `1201022`, `1201092`, `1205011`, `1205062`, `1206115`, `1208022`, `1211052`, `1211092`, `1213072`, `1216082`, `1216092`, `1216145`, `1218014`, `1261049` — brak albo bardzo słaby sygnał planistyczny

### Śląskie (`24`)

| Metryka | Wartość |
|---|---:|
| Gminy z parcelami | `22` |
| Gminy z `planning_signals` | `14` |
| Gminy bez `planning_signals` | `8` |
| Gminy z `future_buildable` | `8` |
| Gminy z `current_buildable` | `6` |
| Gminy z backlogiem `future_buildable` | `10` |

Najważniejsze aktywne gminy z `future_buildable`:
- `2404042`
- `2405011`
- `2406092`
- `2412014`
- `2414021`
- `2416085`
- `2417032`
- `2469011`

Największe luki coverage:
- `2403052` — dużo parceli (`21`), ale to realny `upstream blocker`
- `2466011` — sygnały istnieją, ale najwyższy case wpada w `green`, więc brak awansu jest poprawny
- `2409025` — sygnały istnieją, ale nadal za słabe
- `2414042` — dodatni, ale jeszcze nie domknięty

## 3. Gminy z Największym Yieldem

### Już dowiezione `future_buildable`

Najmocniejsze skupiska:
- `2416085` — `4` leady
- `2417032` — `3` leady
- `1215082` — `2` leady
- `2405011` — `2` leady
- `2406092` — `2` leady

Pojedyncze, ale ważne sukcesy:
- `1201065`
- `1203034`
- `1205092`
- `1206032`
- `1206105`
- `1206114`
- `1206152`
- `1210062`
- `1262011`
- `2404042`
- `2412014`
- `2414021`
- `2469011`

### Najwyższy pozostały backlog

To są dziś najważniejsze niedomknięte przypadki:

| TERYT | Identyfikator | Max `overall_score` | Stan |
|---|---|---:|---|
| `2466011` | `246601161.15/58` | `100` | brak leada z poprawnego powodu — `studium_zone = green` |
| `1206162` | `120616204.690/3` | `20` | dodatni sygnał, ale za mało corroboration |
| `1208045` | `120804506.3/4` | `20` | dodatni sygnał, ale za mało corroboration |
| `1213062` | wiele parceli | `20` | dużo parceli, nadal brak dodatniego, wystarczająco mocnego sygnału |
| `1216155` | `121615509.528/1` | `20` | coverage jest, ale brak awansu |
| `1218095` | `121809503.3827/1` | `20` | źródła żyją, ale dają słabe/`unknown` sygnały |
| `2403052` | wiele parceli | `20` | twardy blocker upstream |

## 4. Ocena Jakości Aplikacji z Punktu Widzenia Produktu

### Co działa dobrze

- pipeline `scrape -> resolve -> delta -> planning signals -> future_buildability` działa end-to-end,
- frontend działa na Cloud SQL jako domyślnym źródle danych,
- `future_buildable` nie jest już eksperymentem: ma realne leady `supported`,
- guardraile działają konserwatywnie i potrafią zablokować nawet bardzo wysoki score, jeśli formalne źródło mówi `green`,
- coverage w `Śląskiem` jest już sensownie szerokie,
- coverage w `Małopolsce` wyszło z fazy „0 leadów”.

### Co nadal ogranicza system

- duża część backlogu to nie problem algorytmu, tylko brak źródeł planistycznych,
- część JST publikuje tylko dokumenty `unknown` albo słabe strony informacyjne bez semantyki działkowej,
- `2403052` i podobne przypadki są poza możliwościami obecnej architektury bez obejścia upstreamu,
- `1213062` jest dziś największą „dziurą biznesową”, bo ma dużo parceli, ale brak mocnego sygnału,
- `future_buildable` nie jest jeszcze pełnym „daily autopilotem” dla wszystkich gmin obu województw.

## 5. Priorytety Dalszej Pracy

### Priorytet A — high-yield discovery

Najpierw:
- `1213062`
- `1206162`
- `1208045`
- `1216155`
- `1218095`

### Priorytet B — blokery

Oddzielnie:
- `2403052`
- przypadki `unknown-only`
- JST z portalami, które zwracają wyłącznie ogłoszenia bez semantyki

### Priorytet C — maintenance

- regularne `campaign-slaskie`
- regularne `campaign-malopolskie`
- po każdej większej fali źródeł:
  - `planning-signal-sync`
  - `future-buildability`
  - `future-buildability-status`

## 6. Wniosek Operacyjny

Z perspektywy coverage:
- `Śląskie` jest już w fazie optymalizacji yield,
- `Małopolskie` jest w fazie aktywnego rozszerzania coverage, ale nie w fazie zerowej.

Z perspektywy produktu:
- TerraZoning jest już wartościowy jako narzędzie do wyszukiwania `future_buildable`,
- ale dalszy wzrost będzie pochodził głównie z dalszego source discovery i utrzymania registry, a nie z „jeszcze jednego tuningu score”.
