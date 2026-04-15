# TerraZoning on GCP `maths` — Deploy and Ops Runbook

## 1. IaC apply

W `maths-iac`:

```bash
cd /Users/michalszarek/worksapace/maths-iac/environments/dev/data
terragrunt apply

cd /Users/michalszarek/worksapace/maths-iac/environments/dev/iam
terragrunt apply

cd /Users/michalszarek/worksapace/maths-iac/environments/dev/security
terragrunt apply

cd /Users/michalszarek/worksapace/maths-iac/environments/dev/compute
terragrunt apply
```

Jeśli chcesz zostać chwilowo bez LB/IAP:
- zostaw puste:
  - `terrazoning_lb_domain`
  - `terrazoning_iap_client_id`
  - `terrazoning_iap_client_secret`

## 2. Backend deploy

Lokalnie z repo TerraZoning:

```bash
cd /Users/michalszarek/worksapace/terrazoning
make gcp-deploy-backend
make gcp-smoke-api
```

Albo przez GitHub Actions:
- push do `main`
- workflow `Deploy TerraZoning backend and jobs`

## 3. Frontend deploy

### Bootstrap direct-to-API

Domyślny workflow frontu bierze URL backendu z Cloud Run i bake’uje go do frontu.

To jest poprawne dla pierwszego uruchomienia, gdy nie ma jeszcze load balancera.

```bash
cd /Users/michalszarek/worksapace/terrazoning
make gcp-deploy-frontend
make gcp-smoke-frontend
make gcp-service-urls
```

## 3a. Lokalny dostęp przez Cloud Run proxy

Aktualny model operatorski to proxy-only. Jeśli nie chcesz używać publicznych URL-i Cloud Run w przeglądarce, korzystaj z lokalnego proxy:

```bash
cd /Users/michalszarek/worksapace/terrazoning
make gcp-auth
make gcp-proxy
```

Potem otwórz:
- `http://localhost:5173`
- `http://localhost:8000/docs`

Jeśli chcesz rozdzielić proxy na dwa terminale:

### Frontend (port 5173)

```bash
make gcp-proxy-frontend
```

Potem otwórz:
- `http://localhost:5173`

### API (port 8000)

```bash
make gcp-proxy-api
```

Potem otwórz:
- `http://localhost:8000/docs`

To jest preferowana ścieżka lokalnego dostępu, dopóki nie ma finalnego `LB + IAP`.

### Docelowy same-origin przez LB

W GitHub repo ustaw:

- `TERRAZONING_SAME_ORIGIN_API=true`

Wtedy frontend nie bake’uje bezpośredniego URL API i zostaje przy relatywnym `/api`.

## 4. Joby operatorskie

### Manual execute

```bash
make gcp-job-scrape-live
make gcp-job-geo-resolve
make gcp-job-delta
make gcp-job-planning-signal-sync
make gcp-job-future-buildability
make gcp-job-campaign-rollout
```

### Z argumentami

`GCP_JOB_ARGS` przekazuj jako pojedynczy string argumentów rozdzielonych przecinkami, np.:

```bash
make gcp-job-planning-signal-sync GCP_JOB_ARGS="--teryt,2414021"
make gcp-job-future-buildability GCP_JOB_ARGS="--province,slaskie,--batch-size,250"
make gcp-job-campaign-rollout GCP_JOB_ARGS="--province,slaskie,--autofix,--parallel"
```

## 5. Scheduler cadence

### Incremental
- scrape: co 4h
- geo resolve: +10 min
- delta: +20 min

### Nightly
- planning signal sync: 01:00
- future buildability: 02:20

## 6. Smoke checklist

### Backend
```bash
make gcp-smoke-api
```

Powinno przejść:
- `/api/v1/health`

### Frontend
```bash
make gcp-smoke-frontend
```

Powinno przejść:
- `GET /`

### Jobs
```bash
gcloud run jobs describe terrazoning-future-buildability --project maths-489717 --region europe-west1
```

## 7. Cutover do LB + IAP

Kiedy DNS i OAuth client będą gotowe:

1. Uzupełnij w `maths-iac`:
   - `terrazoning_lb_domain`
   - `terrazoning_iap_client_id`
   - `terrazoning_iap_client_secret`
2. `terragrunt apply` compute
3. Ustaw w GitHub repo:
   - `TERRAZONING_SAME_ORIGIN_API=true`
4. Wdróż frontend ponownie
5. Zweryfikuj:
   - app działa za IAP
   - frontend używa `/api`
   - backend direct URL nie jest potrzebny do codziennej pracy

## 8. Rollback

### Frontend rollback
- redeploy poprzedni image tag Cloud Run

### Backend rollback
- redeploy poprzedni image tag
- job sync cofnij do tego samego image

### Infra rollback
- jeśli problem dotyczy LB/IAP, wyłącz cutover i wróć do bootstrap direct Cloud Run URLs

## 9. GitHub Actions setup

### Tabela konfiguracji GitHub

| name | type | required | current value | used by |
|---|---|---|---|---|
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | secret | tak | `projects/478521031206/locations/global/workloadIdentityPools/github/providers/github-actions` | `.github/workflows/deploy-backend.yml`, `.github/workflows/deploy-frontend.yml` |
| `GCP_DEPLOY_SERVICE_ACCOUNT` | secret | tak | `github-deploy-sa@maths-489717.iam.gserviceaccount.com` | `.github/workflows/deploy-backend.yml`, `.github/workflows/deploy-frontend.yml` |
| `GCP_PROJECT_ID` | variable | tak | `maths-489717` | workflow backend/frontend, `Makefile`, Cloud Build substitutions |
| `GCP_REGION` | variable | tak | `europe-west1` | workflow backend/frontend, `Makefile`, Cloud Build substitutions |
| `ARTIFACT_REGISTRY_REPOSITORY` | variable | tak | `python-apps` | workflow backend/frontend, `cloudbuild.backend.yaml`, `cloudbuild.frontend.yaml` |
| `TERRAZONING_API_SERVICE` | variable | tak | `terrazoning-api` | workflow backend/frontend, `Makefile`, smoke checks |
| `TERRAZONING_FRONTEND_SERVICE` | variable | tak | `terrazoning-frontend` | workflow frontend, `Makefile`, smoke checks |
| `TERRAZONING_BACKEND_IMAGE` | variable | tak | `terrazoning-backend` | workflow backend, `cloudbuild.backend.yaml` |
| `TERRAZONING_FRONTEND_IMAGE` | variable | tak | `terrazoning-frontend` | workflow frontend, `cloudbuild.frontend.yaml` |
| `TERRAZONING_JOB_SCRAPE_LIVE` | variable | tak | `terrazoning-scrape-live` | workflow backend, `cloudbuild.backend.yaml`, `Makefile` |
| `TERRAZONING_JOB_GEO_RESOLVE` | variable | tak | `terrazoning-geo-resolve` | workflow backend, `cloudbuild.backend.yaml`, `Makefile` |
| `TERRAZONING_JOB_DELTA` | variable | tak | `terrazoning-delta` | workflow backend, `cloudbuild.backend.yaml`, `Makefile` |
| `TERRAZONING_JOB_PLANNING_SIGNAL_SYNC` | variable | tak | `terrazoning-planning-signal-sync` | workflow backend, `cloudbuild.backend.yaml`, `Makefile` |
| `TERRAZONING_JOB_FUTURE_BUILDABILITY` | variable | tak | `terrazoning-future-buildability` | workflow backend, `cloudbuild.backend.yaml`, `Makefile` |
| `TERRAZONING_JOB_CAMPAIGN_ROLLOUT` | variable | tak | `terrazoning-campaign-rollout` | workflow backend, `cloudbuild.backend.yaml`, `Makefile` |
| `CLOUD_BUILD_SOURCE_STAGING_DIR` | variable | tak | `gs://maths-cloudbuild-source-478521031206/source` | workflow backend/frontend, `Makefile`, `gcloud builds submit` |
| `CLOUD_BUILD_SERVICE_ACCOUNT` | variable | tak | `projects/maths-489717/serviceAccounts/478521031206-compute@developer.gserviceaccount.com` | workflow backend/frontend, `Makefile`, Cloud Build runtime |
| `TERRAZONING_FUTURE_BUILDABILITY_ENABLED` | variable | nie | `true` | workflow frontend, `cloudbuild.frontend.yaml`, frontend feature flags |
| `TERRAZONING_SAME_ORIGIN_API` | variable | nie | `false` | workflow frontend, wybór między direct API URL a relatywnym `/api` |
| `TERRAZONING_API_BASE_URL` | variable | nie | `http://localhost:8000` | workflow frontend, jawny override API URL dla trybu proxy-only |
| `TERRAZONING_MAP_STYLE_URL` | variable | nie | `https://tiles.openfreemap.org/styles/liberty` | workflow frontend, `cloudbuild.frontend.yaml`, mapa w UI |

### Interpretacja opcji

- `TERRAZONING_SAME_ORIGIN_API=false`
  - przy braku `LB + IAP` frontend nie używa relatywnego `/api`
- `TERRAZONING_API_BASE_URL=http://localhost:8000`
  - frontend jest buildowany pod lokalny backend proxy
- `TERRAZONING_MAP_STYLE_URL`
  - przypina styl mapy do OpenFreeMap Liberty

## 10. Migracja pełnej lokalnej bazy do Cloud SQL

Jeśli lokalna baza developerska jest pełniejsza niż Cloud SQL, bezpieczna ścieżka operatorska jest taka:

1. backup Cloud SQL
2. dump lokalnej bazy
3. restore lokalnego dumpa do Cloud SQL
4. smoke check API + statusy prowincji

Przykładowy zestaw komend:

```bash
PGPASSWORD=terrazoning pg_dump -h 127.0.0.1 -U terrazoning -d terrazoning \
  --format=custom --no-owner --no-privileges \
  --file /tmp/terrazoning-local.dump

./scripts/run_backend_cloudsql.sh exec backend -- sh -lc '
  pg_dump "$DATABASE_URL" --format=custom --no-owner --no-privileges \
    --file /tmp/terrazoning-cloud-before.dump
'

./scripts/run_backend_cloudsql.sh exec backend -- sh -lc '
  psql "$DATABASE_URL" -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid();" >/dev/null &&
  pg_restore --clean --if-exists --no-owner --no-privileges \
    --dbname "$DATABASE_URL" /tmp/terrazoning-local.dump
'
```

Po tej operacji zweryfikuj:

```bash
make status
make future-buildability-status PROVINCE=slaskie
make future-buildability-status PROVINCE=malopolskie
make gcp-smoke-api
```
