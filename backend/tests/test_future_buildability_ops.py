from datetime import datetime, timezone
from types import SimpleNamespace

from export_future_buildability_backlog import parse_args as parse_backlog_args
from print_future_buildability_status import (
    _build_backlog_entry,
    _format_timestamp,
    _has_reliable_benchmark,
    _next_best_source_type,
    _operator_status,
)
from smoke_future_buildability_rollout import parse_args as parse_smoke_args
from run_future_buildability import parse_args


def test_run_future_buildability_parse_args_accepts_scope_filters() -> None:
    args = parse_args(
        [
            "--batch-size",
            "250",
            "--dzialka-id",
            "550e8400-e29b-41d4-a716-446655440000",
            "--teryt-gmina",
            "1217011",
            "--province",
            "slaskie",
        ]
    )

    assert args.batch_size == 250
    assert args.dzialka_ids == ["550e8400-e29b-41d4-a716-446655440000"]
    assert args.teryt_gminy == ["1217011"]
    assert args.provinces == ["slaskie"]


def test_future_buildability_status_classifies_next_source_and_operator_state() -> None:
    assert _next_best_source_type(set(), None) == "wykazplanow"
    assert _next_best_source_type({"html_index"}, "live") == "studium_or_pog_geometry"
    assert _next_best_source_type({"planning_zone_passthrough", "html_index"}, "live") == "mpzp_project_or_resolution"
    assert _next_best_source_type({"html_index"}, "upstream_broken") == "manual_research"

    assert _operator_status(set(), None) == "ready"
    assert _operator_status({"html_index"}, "live") == "needs_geometry_source"
    assert _operator_status({"planning_zone_passthrough", "html_index"}, "live") == "live"
    assert _operator_status({"html_index"}, "upstream_broken") == "upstream_broken"


def test_future_buildability_status_formats_timestamps_and_backlog_rows() -> None:
    timestamp = datetime(2026, 4, 10, 12, 30, tzinfo=timezone.utc)
    assert _format_timestamp(timestamp) == "2026-04-10T12:30:00+00:00"
    assert _format_timestamp(None) == "-"

    entry = _build_backlog_entry(
        threshold_row={
            "teryt_gmina": "1217011",
            "parcel_count": 3,
            "max_overall_score": 50,
            "last_assessment_at": timestamp,
        },
        source_row={
            "known_sources": "html_index, planning_zone_passthrough",
            "last_source_sync_at": timestamp,
        },
        html_probe=SimpleNamespace(status="live", error=None),
    )

    assert entry["teryt_gmina"] == "1217011"
    assert entry["parcel_count"] == 3
    assert entry["next_best_source_type"] == "mpzp_project_or_resolution"
    assert entry["operator_status"] == "live"
    assert entry["last_assessment_at"] == "2026-04-10T12:30:00+00:00"
    assert entry["last_source_sync_at"] == "2026-04-10T12:30:00+00:00"


def test_has_reliable_benchmark_requires_sample_and_median() -> None:
    assert _has_reliable_benchmark(5, 123.45) is True
    assert _has_reliable_benchmark(4, 123.45) is False
    assert _has_reliable_benchmark(5, None) is False
    assert _has_reliable_benchmark(0, 123.45) is False


def test_backlog_export_and_smoke_scripts_accept_rollout_flags() -> None:
    backlog_args = parse_backlog_args(
        [
            "--province",
            "malopolskie",
            "--format",
            "json",
            "--output",
            "/tmp/future-buildability.json",
        ]
    )
    assert backlog_args.province == "malopolskie"
    assert backlog_args.format == "json"
    assert str(backlog_args.output) == "/tmp/future-buildability.json"

    smoke_args = parse_smoke_args(
        [
            "--expected-state",
            "disabled",
            "--limit",
            "5",
        ]
    )
    assert smoke_args.expected_state == "disabled"
    assert smoke_args.limit == 5
