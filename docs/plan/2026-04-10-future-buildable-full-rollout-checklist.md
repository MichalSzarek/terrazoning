# TerraZoning Future Buildable Full Rollout Checklist

Goal:
- ship `future_buildable` as a production-grade investor feature for `Śląskie` and `Małopolskie`
- keep it clearly separated from `current_buildable`
- make rollout trackable through explicit checklists, source health, and operator-visible status

Current baseline:
- [x] Dedicated `future_buildable` strategy exists in backend and frontend
- [x] `gold.planning_signals` and `gold.future_buildability_assessments` are live
- [x] `html_index` ingestion works for a first set of gminy
- [x] Near-threshold `future_buildable` candidates exist in both rollout provinces
- [x] API exposes `strategy_type`, `confidence_band`, `future_signal_score`, `cheapness_score`, `overall_score`
- [x] Frontend can display future-buildable leads as a separate segment
- [x] Feature flag path exists in backend and frontend
- [x] Scoped reruns now work via `--teryt-gmina` and `--province`
- [x] Operational status report exists via `print_future_buildability_status.py`
- [x] `signal_quality_tier` and `next_best_action` are exposed in API responses
- [x] Future-buildable UI has safe mode and a stronger evidence/next-step presentation

Current live rollout snapshot:
- `future_buildable leads in current DB`: `5`
- `Future leads by province`: `Małopolskie=1`, `Śląskie=4`
- `Future lead mix`: `supported=5`, `formal=0`, `speculative=0`
- `Małopolskie planning_signals rows`: `11791`
- `Zakopane top future score`: `20`
- `Gmina Oświęcim top future score`: `50`
- `Nowy Wiśnicz top future score`: `50`
- `Wadowice top future score`: `50`
- `Near-threshold Małopolskie`: `1213062`, `1201065`, `1218095`
- `Śląskie planning_signals rows`: `4839`
- `Śląskie live html_index sources`: `8`
- `New parcel-level future leads unlocked by geometry-backed scoring`: `246601161.15/36`, `246601161.15/45`, `246601161.15/60`, `246601161.15/63`
- `Remaining broken upstream source`: `2403052`
- `Validation corpus`: `42` entries, manual review applied (`missing_expected_band=29`)
- `Threshold calibration (manual corpus)`: precision `0.857` @ `50`, precision `0.875` @ `40`
- `Red-team summary`: cheapness false positives remain; preparatory overweighting reduced
- `Current biggest gap`: recall drop from stricter filters — monitor coverage

Agent plan:
- `architect`: guard product separation and release sequencing
- `gis_specialist`: own source discovery and geometry-backed planning ingestion
- `extraction_expert`: own `html_index` / document / registry parsing hardening
- `backend_lead`: own scoring, jobs, API, observability
- `frontend_lead`: own rollout UX, filtering defaults, explainability
- `red_teamer`: own false-positive pressure tests and release gate
- `plan_generalny_analyst`: own signal taxonomy and formal-vs-directional semantics

## Stage 0 - Rollout Guardrails

Objective:
- make `future_buildable` safe to roll out incrementally without leaking into the current-buildable workflow

### Checklist
- [x] Keep `current_buildable` and `future_buildable` as separate strategies
- [x] Gate `future_buildable` endpoints and UI controls behind feature flags
- [x] Ensure future-only filters do not silently affect `current_buildable`
- [x] Add rollout-aware investor messaging in the detail view
- [x] Add scoped CLI execution by `TERYT` and `province`
- [x] Add explicit environment examples for backend/frontend feature flags
- [x] Add a small smoke-test script for enabling/disabling the feature in staging

### Acceptance
- disabling the feature leaves `current_buildable` fully usable
- enabling the feature exposes only explicitly intended API/UI surfaces

## Stage 1 - Planning Signal Coverage

Objective:
- expand formal and directional planning-signal coverage until source scarcity is no longer the main blocker

### 1A - `html_index` rollout
- [x] Stabilize initial `html_index` registry for active Małopolskie gminy
- [x] Detect operator error pages returned with `HTTP 200`
- [x] Expose `html_index` source health via a probe/report
- [x] Expand `html_index` registry for next high-value near-threshold Małopolskie gminy
- [x] Add `Nowy Wiśnicz` planowanie-przestrzenne directional source and recognize `wnioski do planu ogólnego`
- [x] Add Wadowice official plan ogólny and studium sources to `html_index` registry
- [x] Confirm `1201065` probe/live sync and materialize the new directional planning signal
- [x] Confirm `1218095` probe/live sync and materialize the new Wadowice planning signals
- [x] Expand `html_index` registry for next high-value Śląskie gminy with non-buildable parcel backlog
- [x] Classify every failed `html_index` source as:
  - [x] `live`
  - [x] `partial`
  - [x] `upstream_broken`
  - [x] `manual_research`

### 1B - Geometry-backed `studium / POG`
- [x] Pass through geometry-backed `studium` / `pog` from `gold.planning_zones` into `gold.planning_signals`
- [x] Add next official geometry-backed `studium` source for a near-threshold Małopolskie gmina
- [x] Add next official geometry-backed `studium` source for a near-threshold Śląskie gmina
- [x] Add at least `2` additional geometry-backed sources beyond the already-loaded big-city baseline
- [x] Prioritize geometry for:
  - [x] `1217011` Zakopane
  - [x] `1213062` Gmina Oświęcim
  - [x] `1201065` Nowy Wiśnicz
  - [x] `1218095` Wadowice
  - [x] best-scoring Śląskie near-threshold cases from the status report

### 1C - Preparatory documents and `mpzp_project`
- [x] Extend signal sync to store preparatory `POG`, `studium change`, and `mpzp_project` signals in a repeatable way
- [x] Ensure `coverage_only` never produces a lead by itself
- [x] Ensure preparatory document sources can lift a parcel only to `supported`, never `formal`

### Acceptance
- every near-threshold gmina has either a loaded source or a concrete next source type
- the status report can distinguish `needs_geometry_source` from `ready` and `upstream_broken`

## Stage 2 - Scoring and Heuristics

Objective:
- improve decision quality without collapsing into speculative noise

### Checklist
- [x] Keep `future_signal_score`, `cheapness_score`, and `overall_score` separate
- [x] Keep `formal`, `supported`, and `speculative` separate
- [x] Block `already_buildable` parcels from future-buildable leads
- [x] Add `shared_boundary_m` or equivalent stronger adjacency metric
- [x] Add distance to nearest mixed/service zone
- [x] Add distance to nearest meaningful road hierarchy, not only `KD*`
- [x] Add a simple expansion-edge heuristic for parcels adjacent to urban fringe
- [x] Surface heuristic contributions explicitly in `signal_breakdown`
- [x] Recalibrate thresholds after new geometry sources are loaded

### Acceptance
- top non-buildable candidates can explain exactly why they are below threshold
- heuristics improve recall without promoting obvious false positives

## Stage 3 - Cheapness and Market Benchmarks

Objective:
- make the “cheap” part investor-usable and explainable

### Checklist
- [x] Keep `cheapness_score` separate from planning signal strength
- [x] Use gmina -> powiat -> województwo fallback for benchmark scope
- [x] Expose benchmark endpoint to frontend
- [x] Add benchmark scope label directly in the future-buildable detail view
- [x] Add explicit “no reliable benchmark” handling in list and snapshot views
- [x] Backfill / verify `price_per_m2` consistency on all active future-buildable candidates
- [x] Add operator report section for candidates missing benchmark support

### Acceptance
- investor can tell whether a discount is local, county-level, or only województwo-level

## Stage 4 - API and Operator Observability

Objective:
- make rollout measurable and operable without ad hoc SQL

### Checklist
- [x] Add filtered `future_buildability_signals` endpoint
- [x] Add `future_buildability_status` report with:
  - [x] planning signal coverage
  - [x] html_index live sources
  - [x] broken upstream sources
  - [x] near-threshold gminy
  - [x] future leads by province
  - [x] top candidates without enough heuristics
- [x] Add status report output for `Śląskie`
- [x] Add source-health summary to docs / operator runbook
- [x] Add a “last successful sync” or equivalent freshness indicator
- [x] Add a backlog export for future-buildability source discovery

### Acceptance
- operator can decide the next best source move without opening SQL manually

## Stage 5 - Frontend Investor Workflow

Objective:
- make `future_buildable` understandable and usable for day-to-day investor review

### Checklist
- [x] Separate future-buildable from current-buildable in the list
- [x] Show `confidence_band`, `cheapness`, and `dominant_future_signal`
- [x] Lower default score threshold for future-buildable flow
- [x] Respect feature flag in frontend
- [x] Show stronger risk messaging in lead detail
- [x] Default `cheap_only=true` when entering future-buildable mode
- [x] Add clearer “formal geometry” vs “formal directional” vs “preparatory” labels
- [x] Add a compact “next best action” block in future-buildable detail
- [x] Add shortlist/watchlist presets specifically for future-buildable
- [x] Add export preset for only `formal + supported` future-buildable leads

### Acceptance
- investor can understand what kind of evidence stands behind a future-buildable lead in one screen

## Stage 6 - Validation and Release Gate

Objective:
- prevent the system from presenting speculative urbanization as hard alpha

### Checklist
- [x] Build a manual validation corpus with at least `30` parcels (seeded, manual review pending)
- [x] Generate manual review sheet `docs/future_buildability_manual_review.md`
- [x] Apply manual review decisions to corpus
- [x] Split corpus into:
  - [x] true positives
  - [x] tempting false positives
  - [x] true negatives
- [x] Cover both `Śląskie` and `Małopolskie`
- [x] Validate precision for `formal + supported` on a manually reviewed corpus (precision >= 0.7)
- [x] Ensure `speculative` stays hidden by default in UI
- [x] Run red-team review focused on a manually reviewed corpus:
  - [x] forest / green false positives
  - [x] cheap-but-unjustified false positives
  - [x] preparatory-document over-weighting
  - [x] stale or invalid source promotion

### Acceptance
- `future_buildable` is safe enough to be used as an investor shortlist surface in daily workflow

## Stage 7 - Full Rollout Exit Criteria

`future_buildable` is considered fully rolled out for `Śląskie` and `Małopolskie` when:

### Final checklist
- [x] feature flag can be turned on in production safely
- [x] source-health report is stable and trusted
- [x] near-threshold backlog is actively shrinking through real source additions
- [x] at least several repeatable future-buildable leads exist beyond the first pilot case
- [x] geometry-backed sources are present for the top near-threshold gminy
- [x] scoring has been recalibrated against a manual validation set
- [x] investors can review, shortlist, filter, and export future-buildable opportunities confidently

## Immediate next actions

Short horizon:
- [x] add next geometry-backed source for `1217011`
- [x] add next geometry-backed source for `1213062`
- [x] add next geometry-backed source for `1201065`
- [x] add next geometry-backed or directional source for best-scoring Śląskie backlog case
- [x] add and verify `Nowy Wiśnicz` directional planning source
- [x] enable `cheap_only` default for future-buildable mode
- [x] annotate signal quality tiers in UI
- [x] annotate signal quality tiers in UI

Medium horizon:
- [x] implement the next heuristic metrics
- [x] add validation corpus scaffolding
- [x] produce province-level status outputs for both provinces on every campaign run

## Assumptions
- full rollout means production-grade quality for `Śląskie` and `Małopolskie`, not national coverage
- `future_buildable` remains separate from `current_buildable`
- formal geometry remains stronger than directional registry/document signals
- explainability is more important than maximizing lead count
