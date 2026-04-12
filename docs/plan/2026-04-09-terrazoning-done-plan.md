# TerraZoning Done Plan

Goal: define the remaining feature/fix checklist required to consider TerraZoning "done" as a strong investor-facing MVP, with explicit coverage status for Śląskie and Małopolskie and checkboxes for each stage.

Current baseline:
- `Śląskie`: `bronze=45`, `silver=45`, `dlq=0`, `zones=26255`, `leads=12`
- `Małopolskie`: `bronze=40`, `silver=49`, `dlq=0`, `zones=23300`, `leads=15`

Coverage backlog:
- `Śląskie`: `5` uncovered gminy, `6` uncovered parcels
- `Małopolskie`: `8` uncovered gmin, `10` uncovered parcels

Implementation progress notes:
- [x] Reviewed available personas: `backend_lead`, `gis_specialist`, `extraction_expert`, `frontend_lead`, `architect`, `iac_lead`, `red_teamer`
- [x] Reviewed local skill inventory in `ai-agents/skills` (`spatial`, `extraction`, `validation`, `infrastructure`)
- [x] First production `gison_raster` municipality (`1211052` Jabłonka / Zubrzyca Górna) is live and produced additional leads
- [x] Second production `gison_raster` municipality (`2417042` Jeleśnia / plan 009) is live
- [x] Third production `gison_raster` municipality (`1218014` Andrychów / plan 06) is live via GeoTIFF-backed sampling
- [x] Fourth production `gison_raster` municipality (`2404112` Mykanów / Grabowa / plan 082) is live and generated an additional lead
- [x] Fifth production `gison_raster` municipality (`1201092` Żegocina / Bełdno / plan 001) is live and generated an additional lead
- [x] Sixth production `gison_raster` municipality (`1201022` Bochnia / Chełm / plan 003) is live and reduced the uncovered backlog
- [x] Seventh production `gison_raster` municipality (`2404042` Kamienica Polska / plan 003) is live and reduced the uncovered backlog
- [x] Eighth production `gison_raster` municipality (`1215082` Zawoja / plan Z01) is live and generated an additional lead
- [x] Ninth production `gison_raster` municipality (`2410042` Pawłowice / Warszowice / plan 011) is live and generated an additional lead
- [x] `2405011` Knurów is now live via `wms_grid` backed by the public GISON parcel popup
- [x] `2403052` Chybie has been upgraded from `no_source_available` to `gison_raster_candidate` after live GISON WMS/WFS verification
- [x] A bulk iGeoMap/vector sweep over remaining uncovered `12*` and `24*` TERYT prefixes found no additional `lay63` / `app.StrefaPlanistyczna` sources to add safely
- [x] `2410055` Pszczyna / Łąka is now live via `wms_grid` and generated an additional lead from the parcel-linked GISON popup
- [x] `2412014` Czerwionka-Leszczyny / Dębieńsko is now tracked as covered via `wms_grid` using a conservative `MPZP_UNK` fallback when the public popup omits the zoning symbol
- [x] `run_wfs_sync.py` now exposes explicit raster source states (`ready`, `source_discovered`, `bbox_axis_suspect`, `legend_missing_semantics`, `manual_override_required`)
- [x] `gison_raster_ingestor.py` now preserves pre-existing WMS query params like `map=...`, unblocking GISON `mapserv` probes
- [x] dead or malformed raster WMS responses now surface as controlled probe errors (`Raster payload is not a decodable image`) instead of crashing `probe-gison-wms`
- [x] Andrychów (`1218014`) was promoted from candidate to production `gison_raster` registry entry
- [x] Operator workflow for `gison_raster` is documented in `docs/plan/2026-04-09-gison-raster-operator-workflow.md`
- [x] Per-gmina raster sync failures are isolated and do not abort the whole `run_wfs_sync.py` run
- [x] `probe-gison-index` now reports asset health for candidate plans (`wms_health`, `geotiff_health`)
- [x] Uncovered gminy now expose `coverage_category` and `next_action` in operator reports
- [x] Province reports now classify DLQ rows into stable operator buckets and print `DLQ by category`
- [x] Leads API now exposes explicit `price_signal`, `quality_signal`, and `missing_metrics`
- [x] Province reports now print `Lead quality` and `Top opportunities` sections filtered away from suspicious price artifacts
- [x] Watchlist criteria are now persisted server-side via `/api/v1/watchlist`, not only in browser-local state
- [x] Watchlist now supports browser-level desktop alerts for newly matching leads
- [x] Shortlist export now includes workflow metadata (`status`, `reviewed_at`, `notes`, quality flags)
- [x] Quarantine manual override flow has been smoke-tested end-to-end on a real parcel with cleanup
- [x] `reset_queues()` now auto-cleans stale DLQ rows before replay work
- [x] Active Śląskie DLQ was reduced from `11` to `0`; exhausted cases now live in explicit manual backlog
- [x] Address-locality extraction now handles `street, postal-code, locality` patterns and recovered Frelichów from active DLQ
- [x] GeoResolver now augments single-parcel matches with official notice enrichment for multi-parcel notices
- [x] `make refresh-all`, `make campaign-slaskie`, and `make campaign-malopolskie` have all completed end-to-end on live data
- [x] Live probes plus portal-query validation promoted `Knurów`, `Pszczyna / Łąka`, and `Czerwionka-Leszczyny / Dębieńsko`; the remaining Śląskie backlog is now a smaller controlled source-discovery exception list

## Stage 1 - Coverage MPZP to Operationally Sufficient

Objective:
- reduce uncovered municipalities until MPZP coverage is no longer the primary growth blocker for leads

### Status

Śląskie uncovered now:
- [ ] `2406092` Długi Kąt / Truskolasy — `2`
- [ ] `2401075` Słowik — `1`
- [ ] `2404082` Baby — `1`
- [ ] `2414042` Bojszowy — `1`
- [ ] `2415041` Wodzisław Śląski — `1`

Małopolskie uncovered now:
- [ ] `1206105` Cianowice — `2`
- [ ] `1216145` Paleśnica — `2`
- [ ] `1203034` Kroczymiech — `1`
- [ ] `1205062` Szalowa — `1`
- [ ] `1205072` Moszczenica — `1`
- [ ] `1205092` Ropica Górna — `1`
- [ ] `1206022` Stręgoborzyce — `1`
- [ ] `1206032` Grzegorzowice Wielkie — `1`

### Completion checklist
- [x] Bring Śląskie uncovered backlog down from `11` gminy to `<= 5`
- [x] Bring Małopolskie uncovered backlog down from `12` gminy to `<= 8`
- [x] Mark every remaining uncovered gmina as either:
  - [x] `no_source_available`
  - [x] `gison_raster_candidate`
  - [x] `manual_backlog`
- [x] Ensure every uncovered gmina has a documented next action
- [x] Re-run province reports and record before/after coverage numbers

### Acceptance
- coverage is no longer the biggest blocker in both provinces
- most active parcels in Silver have a real path to MPZP analysis

## Stage 2 - Finish `gison_raster` as a Real Ingestion Path

Objective:
- turn current raster probing into a production-ready fallback for municipalities with "facade WFS" or WMS-only planning data

### Current state

Already working:
- [x] parcel-aware `--probe-gison-index`
- [x] bbox-axis suspicion detection
- [x] `swap_bbox_axes` in raster config
- [x] manual legend override support for plan-specific raster sources
- [x] explicit raster source-state classification in `run_wfs_sync.py`
- [x] first live `gison_raster` success for `1211052` Jabłonka
- [x] Jabłonka produced new leads

Partially blocked:
- [ ] more gminy still require legend interpretation or manual overrides

### Completion checklist
- [x] Add manual legend override support as a documented pattern for plan-specific raster sources
- [x] Make `run_wfs_sync.py` classify raster source states clearly:
  - [x] `ready`
  - [x] `source_discovered`
  - [x] `bbox_axis_suspect`
  - [x] `legend_missing_semantics`
  - [x] `manual_override_required`
- [x] Finish Jeleśnia `009` as the next live `gison_raster` municipality
- [x] Add at least `2` more production-grade raster municipalities beyond Jabłonka
- [x] Ensure raster sync failures do not crash the whole per-gmina run
- [x] Document operator workflow for probing, validating, and promoting raster sources
- [x] Promote another production-ready raster municipality from the uncovered backlog (`2404112`)
- [x] Promote another production-ready raster municipality from the uncovered backlog (`1201092`)
- [x] Promote another production-ready raster municipality from the uncovered backlog (`1201022`)
- [x] Promote another production-ready raster municipality from the uncovered backlog (`1215082`)
- [x] Promote another production-ready raster municipality from the uncovered backlog (`2410042`)

### Acceptance
- at least `3` municipalities use `gison_raster` in production
- raster municipalities can be synced without hand-editing code each time

## Stage 3 - Data Quality and Investor Metrics

Objective:
- ensure every lead has reliable, investor-usable metrics

### Current state

Already in place:
- [x] `coverage_pct`
- [x] `buildable_area_m2`
- [x] `price_zl`
- [x] `price_per_m2`
- [x] quality filters for low-coverage lead noise
- [x] UI sorting/filtering by investor metrics

Still needed:
- [x] finish hardening price extraction edge cases
- [x] ensure no lead is silently missing key metrics
- [x] ensure suspicious prices are clearly marked, not ranked as best opportunities

### Completion checklist
- [x] Every lead exposes:
  - [x] price
  - [x] price per m²
  - [x] total parcel area
  - [x] buildable area
  - [x] MPZP coverage
  - [x] designation
  - [x] confidence / quality signal
- [x] Add a final quality rule for leads with suspicious price inputs
- [x] Backfill or explicitly mark all leads with missing financial metrics
- [x] Produce a top opportunities query/report without obvious data artifacts
- [x] Validate that ranking no longer surfaces bad-price anomalies as top results

### Acceptance
- investor can trust the ranking and metrics enough to shortlist directly from the app

## Stage 4 - Investor Workflow Completion

Objective:
- complete the workflow from discovery to review to action

### Current state

Already working:
- [x] shortlist
- [x] status changes
- [x] notes
- [x] CSV export
- [x] quarantine parcels with manual override
- [x] watchlist-like local filtering

Still needed:
- [x] durable saved filters / server-side watchlists
- [x] new since last review inbox
- [x] stronger alerting flow for newly matching leads

### Completion checklist
- [x] Add persistent saved filters or server-side watchlists
- [x] Add explicit new since last review or equivalent review inbox
- [x] Add alerting for new leads matching investor criteria
- [x] Show watchlist/alert state in UI without relying only on local browser state
- [x] Ensure shortlist/export works with status and notes included
- [x] Verify quarantine-to-lead flow end-to-end on real analyst workflow

### Acceptance
- investor can manage, revisit, filter, export, and promote leads without leaving the app

## Stage 5 - Resolver and DLQ Hardening

Objective:
- reduce unresolved cases to a controlled, explainable backlog

### Current state
- `Śląskie DLQ = 0`
- `Małopolskie DLQ = 0`

### Completion checklist
- [x] Reduce Małopolskie DLQ from `1` to `0` or explicitly mark as manual-only
- [x] Reduce Śląskie DLQ materially from `11`
- [x] Classify every DLQ row into one explicit bucket:
  - [x] parser issue
  - [x] resolver ambiguity
  - [x] missing planning source
  - [x] manual-only case
- [x] Ensure stale or already-linked DLQ rows are auto-cleaned
- [x] Add a simple operator report of top DLQ blockers by category

### Acceptance
- DLQ is no longer a mystery bucket; every row has a clear reason and next action

## Stage 6 - Final MVP Exit Criteria

TerraZoning can be considered "done" as a strong MVP when all conditions below are true.

### Final checklist
- [x] Coverage backlog is reduced to a controlled list of exceptions
- [x] `gison_raster` is production-grade, not just experimental
- [x] Lead ranking is trustworthy for investor decision support
- [x] Investor workflow is complete enough for daily use
- [x] DLQ is controlled and categorized
- [x] Province reports clearly show:
  - [x] coverage state
  - [x] uncovered backlog
  - [x] leads
  - [x] DLQ
  - [x] covered-but-no-leads
- [x] `make refresh-all` and campaign flows run end-to-end without manual debugging
- [x] app remains valuable even without expanding beyond Śląskie and Małopolskie

## Assumptions
- "Done" means investor-valuable MVP, not full national coverage
- Śląskie and Małopolskie are the target markets that define completion
- Remaining uncovered gminy are acceptable only if they are explicitly categorized and deprioritized, not silently unsupported
- Stage 1 numeric target is now satisfied in Śląskiem; the residual backlog is a controlled exception list centered on five municipalities with documented source blockers
- The next highest-ROI implementation path is:
  1. continue reducing the last uncovered coverage exceptions in Śląskiem
  2. expand Małopolskie source discovery around the remaining eight uncovered gmin
  3. keep improving investor workflow polish on top of the stabilized data pipeline
