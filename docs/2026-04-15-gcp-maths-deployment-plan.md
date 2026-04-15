# TerraZoning on GCP `maths` — Deployment Plan

## Cel

Przenieść TerraZoning z lokalno-devowego modelu uruchamiania do zarządzanego runtime w projekcie GCP `maths`, bez migracji danych do innej bazy.

Stan docelowy:
- `terrazoning-api` na Cloud Run
- `terrazoning-frontend` na Cloud Run
- ingestion i reruny jako Cloud Run Jobs
- istniejąca baza `terrazoning` na współdzielonej instancji Cloud SQL
- bucket `maths-terrazoning-evidence-*`
- opcjonalny bootstrap przez bezpośrednie URL Cloud Run
- docelowo `HTTPS LB + IAP + same-origin /api`

## Decyzje architektoniczne

### Co zostaje
- Cloud SQL instance w projekcie `maths`
- osobna baza `terrazoning`
- lokalny `make run` jako workflow developerski

### Co zmieniamy
- runtime aplikacji przestaje zależeć od lokalnego `cloud-sql-proxy`
- frontend i backend mają własne obrazy kontenerowe
- joby operatorskie stają się Cloud Run Jobs
- schedulery w GCP przejmują cykliczne scrape/sync/rebuild

### Czego nie robimy
- nie hostujemy TerraZoning na `maths-vm` jako docelowym runtime
- nie łączymy bazy `terrazoning` z bazą aplikacji `maths`
- nie zostawiamy produkcji na Vite proxy / localhost assumptions

## Tryby wdrożenia

### Tryb 1 — bootstrap

Cel: szybko podnieść działający runtime bez czekania na DNS/IAP.

- frontend: publiczny URL Cloud Run
- backend: publiczny URL Cloud Run
- frontend bake’uje bezpośredni URL backendu przez `VITE_API_BASE_URL`

Ten tryb jest dobry do:
- pierwszego smoke testu
- porównania danych z lokalnym Cloud SQL workflow
- szybkiego startu CI/CD

### Tryb 2 — docelowy

Cel: jedno wejście przez LB + IAP.

- `https://<terrazoning-domain>/` -> frontend
- `https://<terrazoning-domain>/api/*` -> backend
- frontend działa na relatywnym `/api`
- backend ingress ustawiony na `internal-and-cloud-load-balancing`
- frontend ingress ustawiony na `internal-and-cloud-load-balancing`

W tym trybie w GitHub Actions trzeba ustawić:
- `TERRAZONING_SAME_ORIGIN_API=true`

## Wdrożone artefakty

### W `maths-iac`
- nowa baza `terrazoning`
- bucket `terrazoning_evidence`
- service account `terrazoning-runtime-sa`
- sekrety:
  - `terrazoning-database-url`
  - `terrazoning-gcs-bucket`
- Cloud Run services:
  - `terrazoning-api`
  - `terrazoning-frontend`
- Cloud Run Jobs:
  - `terrazoning-scrape-live`
  - `terrazoning-geo-resolve`
  - `terrazoning-delta`
  - `terrazoning-planning-signal-sync`
  - `terrazoning-future-buildability`
  - `terrazoning-campaign-rollout`
- Cloud Scheduler jobs dla ingestu i nightly refresh
- opcjonalny scaffold dla `LB + IAP`

### W repo TerraZoning
- `Dockerfile.backend`
- `Dockerfile.frontend`
- `cloudbuild.backend.yaml`
- `cloudbuild.frontend.yaml`
- `.github/workflows/deploy-backend.yml`
- `.github/workflows/deploy-frontend.yml`
- GCP `make` targets do deployu, smoke testów i job execution

## GitHub variables i secrets

### GitHub Secrets
- `GCP_WORKLOAD_IDENTITY_PROVIDER`
- `GCP_DEPLOY_SERVICE_ACCOUNT`

### GitHub Variables
- `GCP_PROJECT_ID`
- `GCP_REGION`
- `ARTIFACT_REGISTRY_REPOSITORY`
- `TERRAZONING_API_SERVICE`
- `TERRAZONING_FRONTEND_SERVICE`
- `TERRAZONING_BACKEND_IMAGE`
- `TERRAZONING_FRONTEND_IMAGE`
- `TERRAZONING_JOB_SCRAPE_LIVE`
- `TERRAZONING_JOB_GEO_RESOLVE`
- `TERRAZONING_JOB_DELTA`
- `TERRAZONING_JOB_PLANNING_SIGNAL_SYNC`
- `TERRAZONING_JOB_FUTURE_BUILDABILITY`
- `TERRAZONING_JOB_CAMPAIGN_ROLLOUT`
- `CLOUD_BUILD_SOURCE_STAGING_DIR`
- `CLOUD_BUILD_SERVICE_ACCOUNT`

### Opcjonalne vars dla frontu
- `TERRAZONING_SAME_ORIGIN_API=true`
  - ustaw po przejściu na `LB + IAP`
- `TERRAZONING_API_BASE_URL`
  - override tylko jeśli frontend ma iść do niestandardowego URL API
- `TERRAZONING_MAP_STYLE_URL`
- `TERRAZONING_FUTURE_BUILDABILITY_ENABLED`

## Kolejność wdrożenia

1. Zastosuj `data`, `iam`, `security`, potem `compute` w `maths-iac`
2. Zweryfikuj sekrety i bucket
3. Wdróż backend
4. Wdróż frontend
5. Uruchom smoke checks
6. Uruchom jeden job province-level
7. Włącz schedulery
8. Na końcu dopiero przejdź na `LB + IAP`

## Kryteria akceptacji

- `/api/v1/health` działa na Cloud Run
- frontend pobiera dane z live API z tej samej bazy Cloud SQL
- joby dają się uruchomić pojedynczo i per-TERYT
- nightly refresh nie wymaga lokalnego środowiska
- po przejściu na `LB + IAP` frontend działa na relatywnym `/api`
