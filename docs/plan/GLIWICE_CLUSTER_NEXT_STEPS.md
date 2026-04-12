# Gliwice Cluster Next Steps

> Scope: `Gliwice (2466011)` + `powiat gliwicki (2405xxxx)`
> Date: 2026-04-08

## Why This Focus Makes Sense

The national pipeline is now healthy enough that the next meaningful step is not
"more Poland", but one operationally complete micro-market.

Gliwice is the best first candidate because:

- we already have live MPZP coverage for `2466011` in [run_wfs_sync.py](/Users/michalszarek/worksapace/terrazoning/backend/run_wfs_sync.py),
- we already have at least one real lead in this area,
- the remaining blocker is concrete and local: urban cadastral ambiguity,
- the key unresolved case is repeated, which makes it worth solving properly.

## Current Cluster State

At the time of this note:

- `silver.dzialki = 2` inside the cluster
- `gold.investment_leads = 1`
- resolved gminy:
  - `2466011` Gliwice
  - `2405011` Knurów
- MPZP coverage exists only for:
  - `2466011` Gliwice
- the remaining DLQ pain is concentrated in duplicated listings for:
  - `parcel=44`
  - `obreb=Gliwice`
  - `kw=GL1G/00023264/2`

## LLM Assessment

For the current `Gliwice / 44 / GL1G/00023264/2` case, LLM fallback is **not**
the primary unlock.

Why:

- parcel number `44` is already extracted,
- city `Gliwice` is already extracted,
- KW is already extracted,
- the listing text does **not** expose an obręb name or an explicit district,
- ULDK returns many valid matches for parcel `44` across Gliwice regions.

That means the system is no longer blocked on NLP. It is blocked on
**cadastral disambiguation**.

LLM would likely return the same thing the regex pipeline already has:

- `parcel_number = 44`
- `precinct_or_city = Gliwice`
- `kw_number = GL1G/00023264/2`

That still leaves the resolver with dozens of possible cadastral regions.

## What We Can Execute Today

Use the dedicated cluster tool:

```bash
cd /Users/michalszarek/worksapace/terrazoning/backend
uv run python run_gliwice_cluster.py --show-dlq
```

Optional:

```bash
uv run python run_gliwice_cluster.py --sync-mpzp
uv run python run_gliwice_cluster.py --replay
uv run python run_gliwice_cluster.py --sync-mpzp --replay --show-dlq
```

## Recommended Next Engineering Steps

### 1. Finish the Gliwice cadastral tie-breaker

Goal: resolve `Gliwice / 44` without guessing.

Options, in order:

1. municipal SIP / parcel search endpoint for Gliwice
2. KW-to-parcel source outside ULDK
3. address-based tie-break if the listing ever exposes street or district
4. analyst-assisted shortlist workflow if multiple parcel candidates remain

### 2. Expand cluster MPZP coverage

The cluster is not "done" until we have real planning zones for more than just
Gliwice city. Priority gminy:

1. `2405011` Knurów
2. `2405021` Pyskowice
3. `2405063` Sośnicowice
4. `2405042` Pilchowice

Only add entries to `WFS_REGISTRY` after confirming a live municipal endpoint.

### 3. Keep LLM as a narrow fallback, not the main fix

LLM remains useful for:

- address noise,
- locality-vs-street confusion,
- rural listings where the text does mention a village but regex misses it.

LLM is **not** the right hammer for:

- city-level parcel ambiguity where the source text simply lacks an obręb,
- repeated parcel numbers appearing across many urban cadastral regions,
- missing MPZP coverage.

## Success Criteria For This Cluster

We can call the Gliwice cluster "operationally solved" when:

- `Gliwice / 44` is either resolved or downgraded into a deliberate analyst queue,
- `Knurów` has confirmed MPZP coverage or a documented negative result,
- replaying `run_gliwice_cluster.py --sync-mpzp --replay` is stable and repeatable,
- the cluster yields more than one real lead without test artifacts.
