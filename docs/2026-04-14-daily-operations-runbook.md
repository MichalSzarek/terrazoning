# TerraZoning — Daily Operations Runbook

Stan na `2026-04-14`.

Ten dokument opisuje:
- jak aplikacja powinna być uruchamiana na co dzień,
- co powinien robić developer,
- co powinien robić użytkownik / operator,
- które komendy są „codziennym workflow”, a które tylko awaryjnym narzędziem.

## 1. Zasada Główna

Domyślnym środowiskiem operacyjnym jest teraz `Cloud SQL`.

To oznacza:
- `make run` jest standardem dziennym,
- `make run-local` jest tylko trybem developerskim / eksperymentalnym,
- wszystkie kampanie, statusy i rollouty powinny być liczone na Cloud SQL, nie na lokalnym PostGIS.

## 2. Dzienny Workflow Developera

### A. Start aplikacji

Standardowy start:

```bash
cd /Users/michalszarek/worksapace/terrazoning
make run
```

To uruchamia:
- backend przez `run_backend_cloudsql.sh`,
- frontend dopiero po healthchecku backendu.

### B. Szybki smoke-check rano

```bash
make cloudsql-health
make status
make future-buildability-status PROVINCE=slaskie
make future-buildability-status PROVINCE=malopolskie
```

Cel:
- sprawdzić połączenie z Cloud SQL,
- zobaczyć, czy pipeline nie stoi,
- zobaczyć, gdzie jest nowy backlog.

### C. Dzienna kampania danych

Minimalny operacyjny batch:

```bash
make scrape-live PROVINCES="slaskie malopolskie" MAX_PAGES=3
make geo-resolve
make delta
make planning-signal-sync
make future-buildability PROVINCE=slaskie
make future-buildability PROVINCE=malopolskie
make future-buildability-status PROVINCE=slaskie
make future-buildability-status PROVINCE=malopolskie
```

Jeżeli chcesz pełny gotowy batch:

```bash
make campaign-rollout-cloudsql
```

### D. Kiedy używać kampanii wojewódzkiej

Jeśli chcesz przejść pełnym automatem przez województwo:

```bash
make campaign-slaskie
make campaign-malopolskie
```

Używaj tego gdy:
- doszły nowe źródła MPZP / POG / Studium,
- chcesz odświeżyć całą prowincję,
- robisz batch operatorski, a nie pojedynczy fix.

### E. Kiedy robić targeted rerun

Jeśli dodasz nowe źródło dla jednej gminy:

```bash
make planning-signal-sync TERYT=1206032
make future-buildability TERYT=1206032 BATCH_SIZE=50
```

To jest preferowany tryb po zmianie registry.  
Nie ma sensu za każdym razem odpalać pełnego województwa.

## 3. Dzienny Workflow Użytkownika / Operatora

### A. Otwieranie aplikacji

1. Uruchom:

```bash
cd /Users/michalszarek/worksapace/terrazoning
make run
```

2. Otwórz frontend:
- `http://localhost:5173`

### B. Przegląd leadów bieżących

Dla działek „już budowlanych”:
- `strategia = aktualnie budowlane`
- `status = aktywne`
- `cena = wszystkie` albo `wiarygodne`, zależnie od celu

### C. Przegląd leadów `future_buildable`

Dla działek „dziś niebudowlane, jutro potencjalnie budowlane”:
- `strategia = przyszłe budowlane`
- `pewność = wspierane` albo `wszystkie (bez speculative)`
- opcjonalnie `cheap only = on`

To jest domyślny tryb inwestorski pod strategię `future_buildable`.

### D. Jak czytać wynik

W detalu leada patrz najpierw na:
- `confidence_band`
- `overall_score`
- `dominant_future_signal`
- `future_signal_score`
- `cheapness_score`
- `signal_breakdown`

Interpretacja:
- `formal` = najmocniejszy sygnał
- `supported` = dobry kandydat inwestorski
- `speculative` = research queue, nie gotowa okazja

## 4. Co Powinno Być Wywoływane Regularnie

### Codziennie

```bash
make scrape-live PROVINCES="slaskie malopolskie" MAX_PAGES=3
make geo-resolve
make delta
make planning-signal-sync
make future-buildability PROVINCE=slaskie
make future-buildability PROVINCE=malopolskie
```

### Po dodaniu nowych źródeł

```bash
make planning-signal-sync TERYT=<teryt>
make future-buildability TERYT=<teryt> BATCH_SIZE=50
make future-buildability-status PROVINCE=slaskie
make future-buildability-status PROVINCE=malopolskie
```

### Co kilka dni / po większej serii zmian

```bash
make campaign-rollout-cloudsql
```

### Gdy scraper lub parser się rozjechał

```bash
make reparse-bronze
make geo-resolve
make delta
```

### Gdy coverage MPZP wygląda podejrzanie

```bash
make mpzp-registry
make mpzp-uncovered
make mpzp-sync
```

## 5. Workflow Deweloperski po Dodaniu Nowego Źródła

1. Sprawdź źródło lokalnie małym probe.
2. Dodaj tylko źródła, które naprawdę dają dodatni sygnał.
3. Zapisz URL do registry w `planning_signal_sync.py`.
4. Dodaj assertion do `tests/test_planning_signal_sync.py`.
5. Uruchom:

```bash
cd backend
uv run pytest tests/test_planning_signal_sync.py -q
uv run pytest tests/test_future_buildability_engine.py -q
```

6. Potem targeted rerun:

```bash
make planning-signal-sync TERYT=<teryt>
make future-buildability TERYT=<teryt> BATCH_SIZE=50
```

7. Na końcu sprawdź:

```bash
make future-buildability-status PROVINCE=slaskie
make future-buildability-status PROVINCE=malopolskie
```

## 6. Plan dla Developera

### Krótki plan dzienny

1. Uruchom `make run`
2. Zrób `make status`
3. Zrób `make future-buildability-status PROVINCE=slaskie`
4. Zrób `make future-buildability-status PROVINCE=malopolskie`
5. Wybierz najwyższy backlog:
   - najpierw `max_overall_score >= 40`
   - potem `20-30`
6. Dodaj nowe źródła tylko dla tych gmin
7. Rób targeted reruny, nie pełne województwo
8. Raz dziennie albo po większej serii:
   - `make campaign-rollout-cloudsql`

### Priorytet implementacyjny

Najpierw:
- gminy z dodatnimi sygnałami, ale bez leada
- gminy z `20-40`
- dopiero potem nowe, „ślepe” discovery

Nie marnuj iteracji na:
- gminy z `unknown-only`
- gminy z twardym upstream blockerem
- case’y, które mają już formalny `green` / hard negative

## 7. Plan dla Użytkownika / Inwestora

### Co robić codziennie

1. Wejdź do aplikacji
2. Ustaw `strategia = przyszłe budowlane`
3. Ustaw `pewność = wspierane`
4. Włącz `cheap only`, jeśli szukasz tylko okazji cenowych
5. Sortuj po okazjach inwestorskich
6. Otwieraj tylko leady z:
   - sensownym `overall_score`
   - dodatnim `dominant_future_signal`
   - czytelnym `signal_breakdown`

### Kiedy ufać wynikowi

Największe zaufanie:
- `supported` z wieloma formalnymi źródłami
- dodatni `dominant_future_signal`
- sensowna cena / `cheapness_score`

Niższe zaufanie:
- brak ceny
- słabe, pojedyncze źródło
- `speculative`

## 8. Gdzie Jesteśmy Dziś

System jest gotowy do codziennego użycia w trybie:
- `Cloud SQL`
- `make run`
- codzienny batch danych
- targeted reruny po nowym source discovery

Na dziś nie trzeba już „ręcznie odpalać wszystkiego po kolei” bez planu.  
Najlepszy praktyczny workflow to:

```bash
make run
make status
make future-buildability-status PROVINCE=slaskie
make future-buildability-status PROVINCE=malopolskie
```

a potem tylko:
- targeted discovery,
- targeted sync,
- targeted rerun.
