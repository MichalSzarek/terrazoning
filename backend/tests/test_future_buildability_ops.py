from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

from export_future_buildability_backlog import parse_args as parse_backlog_args
from print_future_buildability_status import (
    _build_backlog_entry,
    _format_timestamp,
    _has_reliable_benchmark,
    _next_best_source_type,
    _operator_status,
)
from run_campaign_rollout import parse_args as parse_campaign_rollout_args
import run_province_campaign as province_campaign
from run_province_campaign import (
    DeltaGapSnapshot,
    ProvinceStatusSnapshot,
    _classify_covered_why_no_lead,
    _classify_uncovered_why_no_lead,
    parse_args as parse_province_campaign_args,
)
from smoke_future_buildability_rollout import parse_args as parse_smoke_args
from run_future_buildability import parse_args
from app.services.future_buildability_engine import FutureBuildabilityReport


@dataclass
class _StubSyncReport:
    completed_gminy: int
    failed_gminy: int
    total_features_upserted: int
    per_gmina: list[dict[str, object]]


@dataclass
class _StubResetReport:
    pass


@dataclass
class _StubGeoReport:
    total_processed: int
    resolved: int
    sent_to_dlq: int


@dataclass
class _StubDeltaReport:
    dzialki_analyzed: int
    delta_results_created: int
    leads_created: int
    leads_updated: int


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
            "podkarpackie",
            "--format",
            "json",
            "--output",
            "/tmp/future-buildability.json",
        ]
    )
    assert backlog_args.province == "podkarpackie"
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


def test_campaign_parsers_accept_podkarpackie_scope() -> None:
    province_args = parse_province_campaign_args(
        [
            "--province",
            "podkarpackie",
            "--stage",
            "report",
            "--parallel",
        ]
    )
    assert province_args.province == "podkarpackie"
    assert province_args.stage == "report"
    assert province_args.parallel is True

    rollout_args = parse_campaign_rollout_args(
        [
            "--province",
            "podkarpackie",
            "--autofix",
        ]
    )
    assert rollout_args.provinces == ["podkarpackie"]
    assert rollout_args.autofix is True


def test_full_campaign_rebuilds_future_buildability_after_replay(monkeypatch) -> None:
    province = "podkarpackie"
    dzialka_id = uuid4()

    async def fake_status_snapshot(province_name: str, *, parallel: bool):
        return ProvinceStatusSnapshot(
            province=province_name,
            display_name="Podkarpackie",
            bronze_listings=1,
            bronze_pending=0,
            silver_dzialki=1,
            silver_dlq=0,
            gold_planning_zones=1,
            gold_investment_leads=0,
        )

    async def fake_fetch_uncovered_gminy(limit: int, province: str):
        return []

    async def fake_delta_gap_snapshot(province_name: str):
        return DeltaGapSnapshot()

    async def fake_run_wfs_sync(province: str):
        return _StubSyncReport(
            completed_gminy=1,
            failed_gminy=0,
            total_features_upserted=1,
            per_gmina=[],
        )

    async def fake_run_scoped_replay(province_name: str, *, autofix: bool):
        return (
            _StubResetReport(),
            0,
            _StubGeoReport(total_processed=1, resolved=1, sent_to_dlq=0),
            _StubDeltaReport(dzialki_analyzed=1, delta_results_created=1, leads_created=0, leads_updated=0),
            [],
        )

    async def fake_province_dzialka_ids(province_name: str):
        assert province_name == province
        return [dzialka_id]

    async def fake_future_buildability_engine(*, batch_size: int, dzialka_ids):
        assert batch_size == 100
        assert dzialka_ids == [dzialka_id]
        return FutureBuildabilityReport(
            dzialki_analyzed=1,
            assessments_created=1,
            assessments_updated=0,
            leads_created=1,
            leads_updated=0,
            duration_s=0.5,
            errors=[],
        )

    async def fake_future_status_payload(*, province: str):
        return {"province": province, "future_leads": 1}

    monkeypatch.setattr(province_campaign, "_status_snapshot", fake_status_snapshot)
    monkeypatch.setattr(province_campaign, "_fetch_uncovered_gminy", fake_fetch_uncovered_gminy)
    monkeypatch.setattr(province_campaign, "_delta_gap_snapshot", fake_delta_gap_snapshot)
    monkeypatch.setattr(province_campaign, "run_wfs_sync", fake_run_wfs_sync)
    monkeypatch.setattr(province_campaign, "_run_scoped_replay", fake_run_scoped_replay)
    monkeypatch.setattr(province_campaign, "_province_dzialka_ids", fake_province_dzialka_ids)
    monkeypatch.setattr(province_campaign, "run_future_buildability_engine", fake_future_buildability_engine)
    monkeypatch.setattr(province_campaign, "_write_coverage_backlog_snapshot", lambda **kwargs: None)
    monkeypatch.setattr(
        province_campaign,
        "build_future_buildability_status_payload",
        fake_future_status_payload,
    )
    monkeypatch.setattr(province_campaign, "_write_future_status_snapshot", lambda **kwargs: None)

    import asyncio

    result = asyncio.run(
        province_campaign._run_campaign(
            province=province,
            stage="full",
            autofix=False,
            parallel=False,
        )
    )

    assert result.delta_report is not None
    assert result.future_report is not None
    assert result.future_report["leads_created"] == 1


def test_scoped_resolution_does_not_fall_back_to_global_geo_when_scope_is_empty(monkeypatch) -> None:
    async def fake_sweep_stale_dlq_rows(*, province: str):
        return 0

    async def fake_reset_queues(*, province: str, destructive_gold_reset: bool):
        return SimpleNamespace(
            target_listing_ids=[],
            bronze_rows_requeued=0,
        )

    async def fake_run_geo_resolver(*, batch_size: int, listing_ids):
        raise AssertionError("run_geo_resolver should not be called for an empty province scope")

    async def fake_pending_listing_ids(province: str):
        return []

    monkeypatch.setattr(province_campaign, "sweep_stale_dlq_rows", fake_sweep_stale_dlq_rows)
    monkeypatch.setattr(province_campaign, "reset_queues", fake_reset_queues)
    monkeypatch.setattr(province_campaign, "run_geo_resolver", fake_run_geo_resolver)
    monkeypatch.setattr(province_campaign, "_pending_listing_ids", fake_pending_listing_ids)

    import asyncio

    reset_report, stale_removed, geo_report, autofix_actions = asyncio.run(
        province_campaign._run_scoped_resolution("podkarpackie", autofix=True)
    )

    assert reset_report.target_listing_ids == []
    assert stale_removed == 0
    assert geo_report.total_processed == 0
    assert autofix_actions == []


def test_province_campaign_why_no_lead_helpers_are_deterministic() -> None:
    assert _classify_uncovered_why_no_lead(
        backlog_status="no_source_configured",
        coverage_category="gison_raster_candidate",
    ) == "no_source"
    assert _classify_uncovered_why_no_lead(
        backlog_status="source_configured_but_not_loaded",
        coverage_category="manual_backlog",
    ) == "upstream_blocker"

    assert _classify_covered_why_no_lead(
        signal_rows=3,
        positive_signal_rows=0,
        unknown_signal_rows=3,
        hard_negative_signal_rows=0,
        delta_rows=0,
        max_overall_score=25.0,
    ) == "unknown_only"
    assert _classify_covered_why_no_lead(
        signal_rows=2,
        positive_signal_rows=0,
        unknown_signal_rows=0,
        hard_negative_signal_rows=1,
        delta_rows=0,
        max_overall_score=10.0,
    ) == "green_or_hard_negative"
    assert _classify_covered_why_no_lead(
        signal_rows=2,
        positive_signal_rows=1,
        unknown_signal_rows=0,
        hard_negative_signal_rows=0,
        delta_rows=0,
        max_overall_score=42.0,
    ) == "weak_signal_or_no_delta"
