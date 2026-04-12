# Future Buildability Operator Runbook

This page summarizes the day-to-day operator commands for the `future_buildable` rollout.

## Status And Freshness

Use the status report to inspect source health, freshness, and the near-threshold backlog:

```bash
cd backend
uv run python print_future_buildability_status.py --province malopolskie
```

The report prints:
- `freshness` for planning signals and future-buildability replays
- `source health summary` for `html_index` probes
- `broken upstream sources`
- `gminy near-threshold`
- `future leads by province`

## Backlog Export

Export the source-discovery backlog as CSV or JSON:

```bash
cd backend
uv run python export_future_buildability_backlog.py --province malopolskie --format csv
uv run python export_future_buildability_backlog.py --province slaskie --format json --output /tmp/future-buildability-backlog.json
```

The export includes:
- `teryt_gmina`
- `parcel_count`
- `max_overall_score`
- `known_sources`
- `next_best_source_type`
- `operator_status`
- `html_index_status`
- `html_index_error`
- freshness timestamps for the gmina

## Smoke Test

Validate the rollout guardrails before enabling the feature in staging:

```bash
cd backend
uv run python smoke_future_buildability_rollout.py --expected-state enabled
uv run python smoke_future_buildability_rollout.py --expected-state disabled
```

The smoke test verifies:
- `current_buildable` stays available
- `future_buildable` endpoints are reachable only when enabled
- feature-flag state matches the current environment

## Rollout Notes

- `future_buildable` stays separate from `current_buildable`.
- `speculative` should stay hidden by default in the UI.
- Use `make future-buildability-status`, `make future-buildability-backlog`, and `make future-buildability-smoke` for repeatable operator workflows.
