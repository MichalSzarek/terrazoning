# AGENTS.md — TerraZoning AI Agent Routing

This repository uses the sibling [`../ai-agents`](../ai-agents/README.md) repo as an external reference pack for Codex and other coding agents.

The goal of this file is not to duplicate the full persona pack. Its job is to tell the agent:
- which external persona or workflow to load,
- in what order to load context,
- and which context pack to use for concrete TerraZoning tasks such as the rollout of `podkarpackie`.

## Source Of Truth

For TerraZoning work, the source of truth is:
1. this `AGENTS.md`,
2. TerraZoning docs, plans, runbooks, and current code,
3. the selected external persona from `../ai-agents/personas/`,
4. the selected workflow playbook from `../ai-agents/workflows/`.

External personas help with reasoning and execution discipline. They do **not** override TerraZoning's code, docs, database model, runbooks, or accepted operational behavior.

## External AI Pack

Reference root:
- `../ai-agents/README.md`

Available general personas relevant to TerraZoning:
- `../ai-agents/personas/ai_agent_guard.md`
- `../ai-agents/personas/domain_steward.md`
- `../ai-agents/personas/tech_architect.md`
- `../ai-agents/personas/backend_lead.md`
- `../ai-agents/personas/frontend_lead.md`
- `../ai-agents/personas/extraction_expert.md`
- `../ai-agents/personas/gis_specialist.md`
- `../ai-agents/personas/plan_generalny_analyst.md`
- `../ai-agents/personas/iac_lead.md`
- `../ai-agents/personas/qa_automation_engineer.md`
- `../ai-agents/personas/red_teamer.md`

Available workflows relevant to TerraZoning:
- `../ai-agents/workflows/auction_monitoring_loop.md`
- `../ai-agents/workflows/bulk_screening_batch.md`
- `../ai-agents/workflows/parcel_assessment_pipeline.md`

## Context Load Order

When Codex starts non-trivial TerraZoning work, load context in this order:

1. `AGENTS.md`
2. the relevant TerraZoning plan, runbook, or issue artifact
3. one or more matching personas from `../ai-agents/personas/`
4. one matching workflow from `../ai-agents/workflows/` when the task is process-heavy
5. only then the code files being changed

Do not dump the whole `ai-agents` repo into context by default. Load the smallest useful set.

## Persona Routing

Use these personas for TerraZoning tasks:

| Task | Persona |
|---|---|
| Repo/process readiness, instruction gaps, missing specs | `../ai-agents/personas/ai_agent_guard.md` |
| Business alignment, anti-bloat review, rule simplification | `../ai-agents/personas/domain_steward.md` |
| Technical planning before code | `../ai-agents/personas/tech_architect.md` |
| Backend, FastAPI, jobs, orchestration, Makefile, reports | `../ai-agents/personas/backend_lead.md` |
| Frontend UX, investor cockpit, filters, layout | `../ai-agents/personas/frontend_lead.md` |
| Scraping, extraction, parser robustness, confidence capture | `../ai-agents/personas/extraction_expert.md` |
| WFS, MPZP, CRS, PostGIS, geometry correctness | `../ai-agents/personas/gis_specialist.md` |
| Studium / POG / plan ogólny / planning signal semantics | `../ai-agents/personas/plan_generalny_analyst.md` |
| GCP, Cloud Run, Cloud SQL, Terragrunt, CI/CD, IAM | `../ai-agents/personas/iac_lead.md` |
| Test design, acceptance coverage, CI verification | `../ai-agents/personas/qa_automation_engineer.md` |
| Adversarial review of risky logic or edge-case abuse | `../ai-agents/personas/red_teamer.md` |

## Workflow Routing

Use these workflow playbooks when the task is shaped like a repeated operating loop rather than a single code edit:

| Task | Workflow |
|---|---|
| Ingestion from auction sources, refresh cadence, scraper operations | `../ai-agents/workflows/auction_monitoring_loop.md` |
| Province or municipality batch rollout, backlog slicing, coverage passes | `../ai-agents/workflows/bulk_screening_batch.md` |
| Parcel scoring, evidence chain, assessment pipeline, lead generation | `../ai-agents/workflows/parcel_assessment_pipeline.md` |

## TerraZoning Guardrails

These rules should stay visible in every meaningful implementation:

- `Cloud SQL` is the production source of truth.
- `EPSG:2180` remains the internal spatial calculation CRS.
- Evidence chain and source provenance are mandatory for important outputs.
- `future_buildable` must not blur together current legal status and directional planning signals.
- `coverage_only` and heuristic-only signals must not be allowed to masquerade as formal planning support.
- New rollout provinces start as **opt-in**, not immediately as default scheduled scope.
- Wide reruns should be staged and verified before being folded into daily or nightly schedulers.

## Podkarpackie Rollout Context Pack

For the rollout of `podkarpackie`, the canonical plan is:
- `docs/plan/2026-04-15-podkarpackie-rollout-plan.md`

Before implementing that rollout, Codex should load:

1. `AGENTS.md`
2. `docs/plan/2026-04-15-podkarpackie-rollout-plan.md`
3. `../ai-agents/personas/tech_architect.md`
4. `../ai-agents/personas/gis_specialist.md`
5. `../ai-agents/personas/plan_generalny_analyst.md`
6. `../ai-agents/personas/backend_lead.md`
7. `../ai-agents/personas/qa_automation_engineer.md`
8. `../ai-agents/workflows/bulk_screening_batch.md`
9. `../ai-agents/workflows/parcel_assessment_pipeline.md`

Load `../ai-agents/personas/iac_lead.md` only when the rollout reaches:
- Cloud Run Jobs changes,
- scheduler expansion,
- CI/CD or IaC updates,
- Cloud SQL / GCP access changes.

## Recommended Phase Ownership For Podkarpackie

Use this sequence when implementing the Podkarpackie plan:

1. `tech_architect`
   - break the rollout into safe slices,
   - confirm file targets,
   - protect the rule that `podkarpackie` starts as opt-in.

2. `backend_lead`
   - extend operator tooling,
   - wire `operations_scope.py`,
   - extend `run_province_campaign.py`,
   - add `Makefile` targets and reporting paths.

3. `gis_specialist`
   - own WFS and MPZP discovery,
   - validate `source_srid`,
   - protect geometry correctness,
   - keep `WFS_REGISTRY` additions explicit and auditable.

4. `plan_generalny_analyst`
   - own planning signal registry semantics,
   - normalize Podkarpackie source labels into canonical classes,
   - make negative signals explicit.

5. `extraction_expert`
   - join when the rollout requires new hostile or irregular source parsing,
   - especially for PDF-heavy planning inputs or unstable HTML indexes.

6. `iac_lead`
   - join only when Podkarpackie graduates from manual rollout to scheduled production operations.

7. `qa_automation_engineer`
   - verify each phase against the rollout checklist,
   - add or demand tests before broadening scope.

## Prompt Recipes For Codex

### Example: scope and operator tooling

```text
Read AGENTS.md, docs/plan/2026-04-15-podkarpackie-rollout-plan.md, ../ai-agents/personas/tech_architect.md, ../ai-agents/personas/backend_lead.md, and ../ai-agents/personas/qa_automation_engineer.md. Implement Phase 1 for podkarpackie: operations scope, campaign tooling, Makefile targets, and tests.
```

### Example: MPZP / WFS rollout batch

```text
Read AGENTS.md, docs/plan/2026-04-15-podkarpackie-rollout-plan.md, ../ai-agents/personas/gis_specialist.md, ../ai-agents/workflows/bulk_screening_batch.md, and ../ai-agents/workflows/parcel_assessment_pipeline.md. Implement the first Podkarpackie MPZP/WFS coverage batch and report blockers explicitly.
```

### Example: planning signals

```text
Read AGENTS.md, docs/plan/2026-04-15-podkarpackie-rollout-plan.md, ../ai-agents/personas/plan_generalny_analyst.md, ../ai-agents/personas/backend_lead.md, and ../ai-agents/personas/qa_automation_engineer.md. Implement the first planning_signals registry slice for podkarpackie and verify the rerun path by TERYT.
```

### Example: productionization after stabilization

```text
Read AGENTS.md, docs/plan/2026-04-15-podkarpackie-rollout-plan.md, ../ai-agents/personas/iac_lead.md, ../ai-agents/personas/backend_lead.md, and ../ai-agents/personas/qa_automation_engineer.md. Extend GCP jobs and the production runbook for podkarpackie only if the rollout gates are already satisfied.
```

## Review Pattern

For larger changes, the recommended review loop is:

1. `domain_steward` for complexity and rule alignment
2. `tech_architect` for shape and boundaries
3. implementation persona(s)
4. `qa_automation_engineer` for evidence and acceptance coverage

Use `ai_agent_guard` when the repo context, plan quality, or instruction surface looks under-specified.

## Non-Goals

This file does not:
- replace TerraZoning runbooks,
- replace architecture decisions already encoded in the repo,
- force every task to load every persona,
- authorize uncontrolled multi-agent edits.

It exists to help Codex load the right external context, at the right time, for the right slice of TerraZoning work.
