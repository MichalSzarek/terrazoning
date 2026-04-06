# KRYTYCZNY DOKUMENT STARTOWY (PROJECT BLUEPRINT)
## System Arbitrażu Ziemi (Polska) - Faza 1

### 1. Cel Aplikacji (The North Star)
Celem systemu jest zautomatyzowanie procesu arbitrażu informacyjnego na polskim rynku ziemi (tzw. "Master Plan Hacking"). System musi nieprzerwanie monitorować źródła podaży (np. licytacje komornicze), gdzie ziemia jest wyceniana jako rolna/leśna, i zestawiać jej geometrię (z ULDK) z danymi planistycznymi (stare Studium vs. nowe Projektowane Plany Ogólne/MPZP z Geoportalu). Wynikiem końcowym jest kokpit dla inwestora, który w czasie poniżej 3 sekund ładuje interaktywną mapę i wyświetla listę działek z najwyższym `confidence_score` (prawdopodobieństwem zmiany przeznaczenia na zyskowne).

### 2. Protokół Komunikacji Agentów
Agenci nie piszą kodu w ciemno. Obowiązuje następujący przepływ pracy (Workflow):
1. **Architect** definiuje kontrakt API (OpenAPI/Swagger) i strukturę bazy danych.
2. **GIS Specialist** oraz **Backend Lead** implementują logikę na backendzie i optymalizują zapytania pod EPSG:2180.
3. **Extraction Expert** pisze i testuje skrypty scrapujące, wrzucając "surowe" dane do bazy (Bronze Layer).
4. **Frontend Lead** konsumuje API backendowe i buduje widoki w React/Mapbox.
5. **Red-Teamer** blokuje Merge Requesty, jeśli kod ignoruje specyfikę polskich danych urzędowych, zakłada 100% dostępność rządowych API lub generuje niepotrzebne koszty w GCP.
6. **IaC Lead** na bieżąco aktualizuje infrastrukturę (Terragrunt), by wspierała nowe moduły.

### 3. Etapy Realizacji (Roadmap)

#### Etap 1: Fundament Danych (Ingestion & Storage)
* **Kto:** IaC Lead, Backend Lead, GIS Specialist.
* **Zadanie:** Postawienie infrastruktury bazowej (GCP Cloud SQL PostgreSQL + PostGIS). Stworzenie tabel dla "Bronze" (surowe scrape'y), "Silver" (znormalizowane TERYT i poligon) i "Gold" (przeanalizowane pod kątem planów). Zdefiniowanie polityk Workload Identity.

#### Etap 2: Rurociąg Pozyskiwania (The Scraper)
* **Kto:** Extraction Expert, Backend Lead.
* **Zadanie:** Stworzenie jobów (np. w oparciu o Cloud Run Jobs), które pobierają ogłoszenia/licytacje. Kluczowe jest napisanie logiki NLP/Regex wyciągającej numery działek, obręby i Księgi Wieczyste (KW). Skrypty muszą obsługiwać rotację proxy i zapisywać oryginalny HTML/PDF jako dowód (Evidence Chain).

#### Etap 3: "Mózg" Przestrzenny (Geo-Resolver & Delta Logic)
* **Kto:** GIS Specialist, Architect, Red-Teamer.
* **Zadanie:** Najtrudniejszy etap. Należy zbudować mechanizm, który z numeru TERYT odpytuje ULDK i zapisuje poligon w EPSG:2180. Następnie (zamiast w locie odpytywać WMS), pobiera dane wektorowe z lokalnych WFS/GML POG/MPZP i robi zapytania analityczne w bazie (`ST_Intersects`). Należy wykryć "Deltę": np. pokrycie działki nowym planem "zabudowa" wynosi > 50%.

#### Etap 4: Kokpit Inwestorski (Triage & Visualization)
* **Kto:** Frontend Lead, Backend Lead.
* **Zadanie:** Budowa aplikacji webowej. Backend wystawia przefiltrowane `Leads` (działki o najwyższym wyniku z Etapu 3). Frontend Lead odpowiada za błyskawiczne wyrenderowanie mapy wektorowej, nałożenie poligonów działek oraz wyświetlenie przejrzystego "łańcucha dowodowego" (od surowego ogłoszenia, przez wynik ULDK, po status z MPZP/POG).

### 4. Wytyczne Techniczne i Bezpieczeństwa (Strict Rules)
* **Żadnego zgadywania geometrii:** Środek ciężkości (centroid) to za mało. Działki rolnicze bywają długimi "paskami". Tylko pełne przecięcia poligonów.
* **Tolerancja na błędy:** Jeśli ULDK nie działa, system nie może zablokować Ingestion Pipeline. Działka trafia do kolejki DLQ (Dead Letter Queue) do późniejszego rozwiązana.
* **Terytorium (TERYT):** Traktujemy numer TERYT jako ostateczny klucz obcy (Primary Key/Foreign Key) w systemie identyfikacji przestrzennej.