"""Province-oriented campaign runner for TerraZoning operations."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from sqlalchemy import case, func, select

from app.core.database import AsyncSessionLocal
from app.models.bronze import RawListing
from app.models.gold import DeltaResult, FutureBuildabilityAssessment, InvestmentLead, PlanningSignal, PlanningZone
from app.models.silver import DlqParcel, Dzialka, ListingParcel
from app.services.delta_engine import DeltaReport, run_delta_engine
from app.services.future_buildability_engine import FutureBuildabilityReport, run_future_buildability_engine
from app.services.geo_resolver import ResolutionReport, run_geo_resolver
from app.services.manual_backlog_store import list_manual_backlog
from app.services.operations_scope import (
    classify_dlq_error,
    classify_backlog_status,
    classify_lead_quality,
    classify_price_signal,
    compute_investment_score,
    coverage_alias_teryt,
    normalize_province,
    provinces,
    province_db_label,
    province_display_name,
    sql_coverage_alias,
    sql_listing_province_filter,
    sql_teryt_prefix_filter,
)
from app.services.planning_signal_utils import HARD_NEGATIVE_DESIGNATIONS, POSITIVE_DESIGNATIONS
from print_future_buildability_status import build_future_buildability_status_payload
from force_retry import ResetReport, clear_gold_for_dzialki, reset_queues, sweep_stale_dlq_rows
from run_wfs_sync import UncoveredGmina, WFSSyncReport, _fetch_uncovered_gminy, run_wfs_sync

logger = logging.getLogger("run_province_campaign")

_BACKLOG_STATUS_ORDER = {
    "no_source_configured": 0,
    "source_configured_but_not_loaded": 1,
    "covered_but_no_delta": 2,
    "covered_but_no_buildable_delta": 3,
}
_NO_SOURCE_COVERAGE_CATEGORIES = {
    "no_source_available",
    "gison_raster_candidate",
    "source_discovered_no_parcel_match",
}
_UPSTREAM_BLOCKER_COVERAGE_CATEGORIES = {
    "manual_backlog",
}


def _classify_uncovered_why_no_lead(*, backlog_status: str, coverage_category: str | None) -> str:
    if backlog_status == "source_configured_but_not_loaded":
        return "upstream_blocker"
    if (coverage_category or "") in _UPSTREAM_BLOCKER_COVERAGE_CATEGORIES:
        return "upstream_blocker"
    if (coverage_category or "") in _NO_SOURCE_COVERAGE_CATEGORIES:
        return "no_source"
    return "no_source"


def _classify_covered_why_no_lead(
    *,
    signal_rows: int,
    positive_signal_rows: int,
    unknown_signal_rows: int,
    hard_negative_signal_rows: int,
    delta_rows: int,
    max_overall_score: float | None,
) -> str:
    has_any_signal = signal_rows > 0
    has_positive_signal = positive_signal_rows > 0
    has_unknown_only = has_any_signal and unknown_signal_rows == signal_rows and not has_positive_signal and hard_negative_signal_rows == 0
    if has_unknown_only:
        return "unknown_only"
    if hard_negative_signal_rows > 0:
        return "green_or_hard_negative"
    if delta_rows <= 0 or (max_overall_score or 0.0) < 60.0:
        return "weak_signal_or_no_delta"
    return "weak_signal_or_no_delta"


@dataclass
class ProvinceStatusSnapshot:
    province: str
    display_name: str
    bronze_listings: int
    bronze_pending: int
    silver_dzialki: int
    silver_dlq: int
    gold_planning_zones: int
    gold_investment_leads: int
    top_resolved_gminy: list[dict[str, object]] = field(default_factory=list)
    top_lead_gminy: list[dict[str, object]] = field(default_factory=list)
    dlq_by_error: list[dict[str, object]] = field(default_factory=list)
    dlq_by_category: list[dict[str, object]] = field(default_factory=list)
    manual_backlog_count: int = 0
    manual_backlog_preview: list[dict[str, object]] = field(default_factory=list)
    lead_quality_summary: list[dict[str, object]] = field(default_factory=list)
    top_opportunities: list[dict[str, object]] = field(default_factory=list)


@dataclass
class DeltaGapSnapshot:
    uncovered_gminy: list[dict[str, object]] = field(default_factory=list)
    covered_no_leads: list[dict[str, object]] = field(default_factory=list)
    intersections_no_leads: list[dict[str, object]] = field(default_factory=list)
    covered_via_alias: list[dict[str, object]] = field(default_factory=list)
    backlog_hints: list[dict[str, object]] = field(default_factory=list)
    coverage_category_summary: list[dict[str, object]] = field(default_factory=list)


@dataclass
class ProvinceCampaignResult:
    province: str
    stage: str
    before: ProvinceStatusSnapshot
    after: ProvinceStatusSnapshot
    uncovered: list[dict[str, object]] = field(default_factory=list)
    delta_gap: DeltaGapSnapshot | None = None
    sync_report: dict[str, object] | None = None
    reset_report: dict[str, object] | None = None
    geo_report: dict[str, object] | None = None
    delta_report: dict[str, object] | None = None
    future_report: dict[str, object] | None = None
    future_status_reports: dict[str, dict[str, object]] = field(default_factory=dict)
    autofix_actions: list[str] = field(default_factory=list)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TerraZoning province campaign")
    parser.add_argument("--province", required=True, choices=provinces())
    parser.add_argument(
        "--stage",
        default="full",
        choices=["discover", "sync", "resolve", "replay", "report", "full"],
    )
    parser.add_argument("--autofix", action="store_true", help="Enable conservative self-heal actions")
    parser.add_argument("--parallel", action="store_true", help="Gather independent report queries concurrently")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    return parser.parse_args(argv)


async def _status_snapshot(province: str, *, parallel: bool) -> ProvinceStatusSnapshot:
    province_key = normalize_province(province)
    if province_key is None:
        raise ValueError(f"Unsupported province: {province!r}")
    display_name = province_display_name(province_key) or province_key
    db_label = province_db_label(province_key)

    async with AsyncSessionLocal() as db:
        async def bronze_listings() -> int:
            return int(
                (
                    await db.execute(
                        select(func.count())
                        .select_from(RawListing)
                        .where(sql_listing_province_filter(RawListing.raw_wojewodztwo, province_key))
                    )
                ).scalar_one()
            )

        async def bronze_pending() -> int:
            return int(
                (
                    await db.execute(
                        select(func.count())
                        .select_from(RawListing)
                        .where(sql_listing_province_filter(RawListing.raw_wojewodztwo, province_key))
                        .where(RawListing.is_processed == False)  # noqa: E712
                    )
                ).scalar_one()
            )

        async def silver_dzialki() -> int:
            return int(
                (
                    await db.execute(
                        select(func.count())
                        .select_from(Dzialka)
                        .where(sql_teryt_prefix_filter(Dzialka.teryt_gmina, province_key))
                    )
                ).scalar_one()
            )

        async def silver_dlq() -> int:
            return int(
                (
                    await db.execute(
                        select(func.count())
                        .select_from(DlqParcel)
                        .join(RawListing, RawListing.id == DlqParcel.listing_id)
                        .where(sql_listing_province_filter(RawListing.raw_wojewodztwo, province_key))
                    )
                ).scalar_one()
            )

        async def planning_zones() -> int:
            return int(
                (
                    await db.execute(
                        select(func.count())
                        .select_from(PlanningZone)
                        .where(sql_teryt_prefix_filter(PlanningZone.teryt_gmina, province_key))
                    )
                ).scalar_one()
            )

        async def investment_leads() -> int:
            return int(
                (
                    await db.execute(
                        select(func.count())
                        .select_from(InvestmentLead)
                        .join(Dzialka, InvestmentLead.dzialka_id == Dzialka.id)
                        .where(sql_teryt_prefix_filter(Dzialka.teryt_gmina, province_key))
                    )
                ).scalar_one()
            )

        async def top_resolved() -> list[dict[str, object]]:
            rows = (
                await db.execute(
                    select(Dzialka.teryt_gmina, func.count())
                    .where(sql_teryt_prefix_filter(Dzialka.teryt_gmina, province_key))
                    .group_by(Dzialka.teryt_gmina)
                    .order_by(func.count().desc(), Dzialka.teryt_gmina.asc())
                    .limit(8)
                )
            ).all()
            return [{"teryt_gmina": code, "dzialki": count} for code, count in rows]

        async def top_leads() -> list[dict[str, object]]:
            rows = (
                await db.execute(
                    select(Dzialka.teryt_gmina, func.count())
                    .join(InvestmentLead, InvestmentLead.dzialka_id == Dzialka.id)
                    .where(sql_teryt_prefix_filter(Dzialka.teryt_gmina, province_key))
                    .group_by(Dzialka.teryt_gmina)
                    .order_by(func.count().desc(), Dzialka.teryt_gmina.asc())
                    .limit(8)
                )
            ).all()
            return [{"teryt_gmina": code, "leads": count} for code, count in rows]

        async def dlq_by_error() -> list[dict[str, object]]:
            rows = (
                await db.execute(
                    select(DlqParcel.last_error, func.count())
                    .join(RawListing, RawListing.id == DlqParcel.listing_id)
                    .where(sql_listing_province_filter(RawListing.raw_wojewodztwo, province_key))
                    .group_by(DlqParcel.last_error)
                    .order_by(func.count().desc())
                    .limit(8)
                )
            ).all()
            return [{"error": error or "UNKNOWN", "count": count} for error, count in rows]

        async def dlq_by_category() -> list[dict[str, object]]:
            rows = (
                await db.execute(
                    select(
                        DlqParcel.last_error,
                        DlqParcel.attempt_count,
                        RawListing.raw_obreb,
                        RawListing.raw_numer_dzialki,
                    )
                    .join(RawListing, RawListing.id == DlqParcel.listing_id)
                    .where(sql_listing_province_filter(RawListing.raw_wojewodztwo, province_key))
                )
            ).all()
            counts: Counter[str] = Counter()
            for error, attempt_count, raw_obreb, raw_numer_dzialki in rows:
                category, _ = classify_dlq_error(
                    last_error=error,
                    attempt_count=int(attempt_count or 1),
                    raw_obreb=raw_obreb,
                    raw_numer_dzialki=raw_numer_dzialki,
                )
                counts[category] += 1
            return [
                {"category": category, "count": count}
                for category, count in counts.most_common()
            ]

        def manual_backlog_summary() -> tuple[int, list[dict[str, object]]]:
            records = list_manual_backlog(province=province_key)
            preview = [
                {
                    "listing_id": record.listing_id,
                    "title": record.title,
                    "source_url": record.source_url,
                    "raw_teryt_input": record.raw_teryt_input,
                    "next_action": record.next_action,
                }
                for record in records[:5]
            ]
            return len(records), preview

        async def lead_quality_summary() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
            rows = (
                await db.execute(
                    select(
                        InvestmentLead.id,
                        InvestmentLead.confidence_score,
                        InvestmentLead.max_coverage_pct,
                        InvestmentLead.dominant_przeznaczenie,
                        InvestmentLead.price_per_m2_zl,
                        InvestmentLead.status,
                        Dzialka.identyfikator,
                        Dzialka.teryt_gmina,
                        Dzialka.area_m2,
                        RawListing.price_zl,
                        (
                            select(func.max(DeltaResult.intersection_area_m2))
                            .where(DeltaResult.dzialka_id == InvestmentLead.dzialka_id)
                            .where(DeltaResult.is_upgrade == True)  # noqa: E712
                            .scalar_subquery()
                        ).label("max_buildable_area_m2"),
                    )
                    .join(Dzialka, InvestmentLead.dzialka_id == Dzialka.id)
                    .outerjoin(RawListing, RawListing.id == InvestmentLead.listing_id)
                    .where(sql_teryt_prefix_filter(Dzialka.teryt_gmina, province_key))
                )
            ).all()

            quality_counts: Counter[str] = Counter()
            opportunities: list[dict[str, object]] = []

            for (
                lead_id,
                confidence_score,
                max_coverage_pct,
                dominant_przeznaczenie,
                price_per_m2_zl,
                status,
                identyfikator,
                teryt_gmina,
                area_m2,
                price_zl,
                max_buildable_area_m2,
            ) in rows:
                price_signal = classify_price_signal(
                    price_zl=price_zl,
                    price_per_m2_zl=price_per_m2_zl,
                )
                quality_signal, missing_metrics = classify_lead_quality(
                    price_zl=price_zl,
                    price_per_m2_zl=price_per_m2_zl,
                    area_m2=area_m2,
                    max_buildable_area_m2=max_buildable_area_m2,
                    max_coverage_pct=max_coverage_pct,
                    dominant_przeznaczenie=dominant_przeznaczenie,
                )
                quality_counts[quality_signal] += 1
                opportunities.append(
                    {
                        "lead_id": str(lead_id),
                        "identyfikator": identyfikator,
                        "teryt_gmina": teryt_gmina,
                        "confidence_score": float(confidence_score),
                        "max_coverage_pct": (
                            float(max_coverage_pct)
                            if max_coverage_pct is not None else None
                        ),
                        "dominant_przeznaczenie": dominant_przeznaczenie,
                        "price_zl": float(price_zl) if price_zl is not None else None,
                        "price_per_m2_zl": (
                            float(price_per_m2_zl)
                            if price_per_m2_zl is not None else None
                        ),
                        "status": status,
                        "price_signal": price_signal,
                        "quality_signal": quality_signal,
                        "investment_score": compute_investment_score(
                            confidence_score=confidence_score,
                            price_zl=price_zl,
                            price_per_m2_zl=price_per_m2_zl,
                            max_buildable_area_m2=max_buildable_area_m2,
                            max_coverage_pct=max_coverage_pct,
                            price_signal=price_signal,
                            quality_signal=quality_signal,
                        ),
                        "missing_metrics": list(missing_metrics),
                    }
                )

            opportunities.sort(
                key=lambda row: (
                    -(row["investment_score"]),
                    0 if row["price_signal"] == "reliable" else 1,
                    row["price_per_m2_zl"] is None,
                    row["price_per_m2_zl"] or float("inf"),
                    -(row["confidence_score"]),
                    -(row["max_coverage_pct"] or 0.0),
                )
            )
            return (
                [
                    {"quality_signal": quality_signal, "count": count}
                    for quality_signal, count in quality_counts.most_common()
                ],
                opportunities[:5],
            )

        bronze_count = await bronze_listings()
        pending_count = await bronze_pending()
        silver_count = await silver_dzialki()
        dlq_count = await silver_dlq()
        planning_count = await planning_zones()
        lead_count = await investment_leads()
        top_resolved_rows = await top_resolved()
        top_leads_rows = await top_leads()
        dlq_errors = await dlq_by_error()
        dlq_categories = await dlq_by_category()
        manual_backlog_count, manual_backlog_preview = manual_backlog_summary()
        lead_quality_rows, top_opportunities = await lead_quality_summary()

    return ProvinceStatusSnapshot(
        province=province_key,
        display_name=display_name,
        bronze_listings=bronze_count,
        bronze_pending=pending_count,
        silver_dzialki=silver_count,
        silver_dlq=dlq_count,
        gold_planning_zones=planning_count,
        gold_investment_leads=lead_count,
        top_resolved_gminy=top_resolved_rows,
        top_lead_gminy=top_leads_rows,
        dlq_by_error=dlq_errors,
        dlq_by_category=dlq_categories,
        manual_backlog_count=manual_backlog_count,
        manual_backlog_preview=manual_backlog_preview,
        lead_quality_summary=lead_quality_rows,
        top_opportunities=top_opportunities,
    )


async def _delta_gap_snapshot(province: str) -> DeltaGapSnapshot:
    province_key = normalize_province(province)
    if province_key is None:
        raise ValueError(f"Unsupported province: {province!r}")

    async with AsyncSessionLocal() as db:
        uncovered = []
        for row in await _fetch_uncovered_gminy(limit=12, province=province_key):
            status, operator_hint = classify_backlog_status(
                in_registry=row.in_registry,
                has_planning_zones=False,
                has_delta_rows=False,
            )
            row_dict = asdict(row)
            row_dict["covered_via"] = coverage_alias_teryt(row.teryt)
            row_dict["backlog_status"] = status
            row_dict["operator_hint"] = operator_hint
            uncovered.append(row_dict)

        covered_no_leads_rows = (
            await db.execute(
                select(
                    Dzialka.teryt_gmina,
                    sql_coverage_alias(Dzialka.teryt_gmina).label("covered_via"),
                    func.count(func.distinct(Dzialka.id)).label("dzialki"),
                    func.count(func.distinct(PlanningZone.id)).label("planning_zones"),
                    func.count(func.distinct(DeltaResult.id)).label("delta_rows"),
                    func.string_agg(
                        func.distinct(
                            func.nullif(
                                func.coalesce(RawListing.raw_obreb, RawListing.raw_gmina),
                                "",
                            )
                        ),
                        " | ",
                    ).label("localities"),
                )
                .outerjoin(ListingParcel, ListingParcel.dzialka_id == Dzialka.id)
                .outerjoin(RawListing, RawListing.id == ListingParcel.listing_id)
                .outerjoin(PlanningZone, PlanningZone.teryt_gmina == sql_coverage_alias(Dzialka.teryt_gmina))
                .outerjoin(DeltaResult, DeltaResult.dzialka_id == Dzialka.id)
                .outerjoin(InvestmentLead, InvestmentLead.dzialka_id == Dzialka.id)
                .where(sql_teryt_prefix_filter(Dzialka.teryt_gmina, province_key))
                .group_by(Dzialka.teryt_gmina, sql_coverage_alias(Dzialka.teryt_gmina))
                .having(func.count(func.distinct(PlanningZone.id)) > 0)
                .having(func.count(func.distinct(InvestmentLead.id)) == 0)
                .order_by(func.count(Dzialka.id).desc(), Dzialka.teryt_gmina.asc())
                .limit(12)
            )
        ).all()

        covered_codes = [str(row[0]) for row in covered_no_leads_rows]
        signal_summary_by_teryt: dict[str, dict[str, object]] = {}
        if covered_codes:
            signal_rows = (
                await db.execute(
                    select(
                        PlanningSignal.teryt_gmina,
                        func.count(PlanningSignal.id).label("signal_rows"),
                        func.sum(
                            case(
                                (PlanningSignal.designation_normalized.in_(tuple(POSITIVE_DESIGNATIONS)), 1),
                                else_=0,
                            )
                        ).label("positive_signal_rows"),
                        func.sum(
                            case(
                                (func.coalesce(PlanningSignal.designation_normalized, "unknown") == "unknown", 1),
                                else_=0,
                            )
                        ).label("unknown_signal_rows"),
                        func.sum(
                            case(
                                (PlanningSignal.designation_normalized.in_(tuple(HARD_NEGATIVE_DESIGNATIONS)), 1),
                                else_=0,
                            )
                        ).label("hard_negative_signal_rows"),
                    )
                    .where(PlanningSignal.teryt_gmina.in_(covered_codes))
                    .group_by(PlanningSignal.teryt_gmina)
                )
            ).mappings().all()
            assessment_rows = (
                await db.execute(
                    select(
                        Dzialka.teryt_gmina,
                        func.max(FutureBuildabilityAssessment.overall_score).label("max_overall_score"),
                    )
                    .join(FutureBuildabilityAssessment, FutureBuildabilityAssessment.dzialka_id == Dzialka.id)
                    .where(Dzialka.teryt_gmina.in_(covered_codes))
                    .group_by(Dzialka.teryt_gmina)
                )
            ).mappings().all()
            signal_summary_by_teryt = {
                str(row["teryt_gmina"]): {
                    "signal_rows": int(row["signal_rows"] or 0),
                    "positive_signal_rows": int(row["positive_signal_rows"] or 0),
                    "unknown_signal_rows": int(row["unknown_signal_rows"] or 0),
                    "hard_negative_signal_rows": int(row["hard_negative_signal_rows"] or 0),
                    "max_overall_score": None,
                }
                for row in signal_rows
            }
            for row in assessment_rows:
                key = str(row["teryt_gmina"])
                summary = signal_summary_by_teryt.setdefault(
                    key,
                    {
                        "signal_rows": 0,
                        "positive_signal_rows": 0,
                        "unknown_signal_rows": 0,
                        "hard_negative_signal_rows": 0,
                        "max_overall_score": None,
                    },
                )
                score = row["max_overall_score"]
                summary["max_overall_score"] = float(score) if score is not None else None

    covered_no_leads: list[dict[str, object]] = []
    intersections_no_leads: list[dict[str, object]] = []
    covered_via_alias: list[dict[str, object]] = []
    backlog_hints: list[dict[str, object]] = []

    for code, covered_via, dzialki, planning_zones, delta_rows, localities in covered_no_leads_rows:
        delta_rows_int = int(delta_rows or 0)
        signal_summary = signal_summary_by_teryt.get(
            str(code),
            {
                "signal_rows": 0,
                "positive_signal_rows": 0,
                "unknown_signal_rows": 0,
                "hard_negative_signal_rows": 0,
                "max_overall_score": None,
            },
        )
        status, operator_hint = classify_backlog_status(
            in_registry=True,
            has_planning_zones=True,
            has_delta_rows=delta_rows_int > 0,
        )
        row = {
            "teryt_gmina": code,
            "covered_via": covered_via,
            "dzialki": dzialki,
            "planning_zones": planning_zones,
            "delta_rows": delta_rows_int,
            "localities": localities or "",
            "backlog_status": status,
            "operator_hint": operator_hint,
            "why_no_lead": _classify_covered_why_no_lead(
                signal_rows=int(signal_summary["signal_rows"] or 0),
                positive_signal_rows=int(signal_summary["positive_signal_rows"] or 0),
                unknown_signal_rows=int(signal_summary["unknown_signal_rows"] or 0),
                hard_negative_signal_rows=int(signal_summary["hard_negative_signal_rows"] or 0),
                delta_rows=delta_rows_int,
                max_overall_score=signal_summary.get("max_overall_score"),
            ),
            "max_overall_score": signal_summary.get("max_overall_score"),
        }
        covered_no_leads.append(row)
        backlog_hints.append(row)
        if covered_via and covered_via != code:
            covered_via_alias.append(row)
        if delta_rows_int > 0:
            intersections_no_leads.append(
                {
                    "teryt_gmina": code,
                    "covered_via": covered_via,
                    "delta_rows": delta_rows_int,
                    "dzialki": dzialki,
                    "localities": localities or "",
                    "backlog_status": status,
                    "operator_hint": operator_hint,
                    "why_no_lead": row["why_no_lead"],
                    "max_overall_score": row.get("max_overall_score"),
                }
            )

    for row in uncovered:
        backlog_hints.append(
            {
                "teryt_gmina": row["teryt"],
                "dzialki": row["dzialki_count"],
                "planning_zones": 0,
                "delta_rows": 0,
                "localities": row.get("localities", ""),
                "backlog_status": row["backlog_status"],
                "operator_hint": row["operator_hint"],
                "coverage_category": row.get("coverage_category"),
                "next_action": row.get("next_action"),
                "in_registry": row["in_registry"],
                "covered_via": coverage_alias_teryt(str(row["teryt"])),
                "why_no_lead": _classify_uncovered_why_no_lead(
                    backlog_status=str(row["backlog_status"]),
                    coverage_category=row.get("coverage_category"),
                ),
            }
        )

    backlog_hints.sort(
        key=lambda row: (
            _BACKLOG_STATUS_ORDER.get(str(row["backlog_status"]), 99),
            -int(row.get("dzialki", 0)),
            str(row["teryt_gmina"]),
        )
    )

    coverage_category_counts: Counter[str] = Counter()
    for row in backlog_hints:
        coverage_category = row.get("coverage_category")
        if coverage_category:
            coverage_category_counts[str(coverage_category)] += int(row.get("dzialki", 0))

    return DeltaGapSnapshot(
        uncovered_gminy=uncovered,
        covered_no_leads=covered_no_leads,
        intersections_no_leads=intersections_no_leads,
        covered_via_alias=covered_via_alias,
        backlog_hints=backlog_hints,
        coverage_category_summary=[
            {"coverage_category": category, "dzialki": count}
            for category, count in coverage_category_counts.most_common()
        ],
    )


def _runtime_path(*parts: str) -> Path:
    root = Path(__file__).resolve().parent
    runtime_dir = root / ".runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    return runtime_dir.joinpath(*parts)


def _write_coverage_backlog_snapshot(
    *,
    province: str,
    before: ProvinceStatusSnapshot,
    after: ProvinceStatusSnapshot,
    delta_gap: DeltaGapSnapshot,
) -> Path:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "province": province,
        "before": asdict(before),
        "after": asdict(after),
        "coverage_category_summary": delta_gap.coverage_category_summary,
        "uncovered_gminy": delta_gap.uncovered_gminy,
        "covered_no_leads": delta_gap.covered_no_leads,
        "intersections_no_leads": delta_gap.intersections_no_leads,
        "backlog_hints": delta_gap.backlog_hints,
    }
    path = _runtime_path(f"coverage_backlog_{province}.json")
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


def _write_future_status_snapshot(*, province: str, payload: dict[str, object]) -> Path:
    path = _runtime_path(f"future_buildability_status_{province}.json")
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return path


async def _province_dzialka_ids(province: str) -> list[UUID]:
    province_key = normalize_province(province)
    async with AsyncSessionLocal() as db:
        return list(
            (
                await db.execute(
                    select(Dzialka.id).where(sql_teryt_prefix_filter(Dzialka.teryt_gmina, province_key))
                )
            ).scalars().all()
        )


async def _pending_listing_ids(province: str) -> list[UUID]:
    province_key = normalize_province(province)
    async with AsyncSessionLocal() as db:
        return list(
            (
                await db.execute(
                    select(RawListing.id)
                    .where(sql_listing_province_filter(RawListing.raw_wojewodztwo, province_key))
                    .where(RawListing.is_processed == False)  # noqa: E712
                )
            ).scalars().all()
        )


def _looks_transient(error_label: str) -> bool:
    lowered = error_label.lower()
    return "timeout" in lowered or "retry" in lowered or "request failed" in lowered


def _sync_autofix_messages(report: WFSSyncReport) -> list[str]:
    messages: list[str] = []
    for row in report.per_gmina:
        if row["status"] == "ok" and int(row.get("upserted", 0)) == 0:
            messages.append(
                f"sync_zero_features:{row['teryt']}:{row['label']} -> needs_manual_probe"
            )
    return messages


async def _run_scoped_resolution(
    province: str,
    *,
    autofix: bool,
) -> tuple[ResetReport, int, ResolutionReport, list[str]]:
    autofix_actions: list[str] = []
    stale_removed = 0
    if autofix:
        stale_removed = await sweep_stale_dlq_rows(province=province)
        if stale_removed:
            autofix_actions.append(f"stale_dlq_removed:{stale_removed}")

    reset_report = await reset_queues(
        province=province,
        destructive_gold_reset=False,
    )
    scoped_listing_ids = list(reset_report.target_listing_ids or [])
    if scoped_listing_ids:
        batch_size = max(100, len(scoped_listing_ids), reset_report.bronze_rows_requeued)
        geo_report = await run_geo_resolver(
            batch_size=batch_size,
            listing_ids=scoped_listing_ids,
        )
    else:
        geo_report = ResolutionReport()

    if autofix:
        pending_after_geo = await _pending_listing_ids(province)
        if pending_after_geo:
            autofix_actions.append(f"extra_geo_pass:{len(pending_after_geo)}")
            extra_geo = await run_geo_resolver(
                batch_size=max(100, len(pending_after_geo)),
                listing_ids=pending_after_geo,
            )
            geo_report.total_processed += extra_geo.total_processed
            geo_report.resolved += extra_geo.resolved
            geo_report.sent_to_dlq += extra_geo.sent_to_dlq
            geo_report.already_resolved += extra_geo.already_resolved
            geo_report.errors.extend(extra_geo.errors)
            geo_report.duration_s = round(geo_report.duration_s + extra_geo.duration_s, 2)

    return reset_report, stale_removed, geo_report, autofix_actions


async def _run_scoped_replay(
    province: str,
    *,
    autofix: bool,
) -> tuple[ResetReport, int, ResolutionReport, DeltaReport, list[str]]:
    reset_report, stale_removed, geo_report, autofix_actions = await _run_scoped_resolution(
        province,
        autofix=autofix,
    )
    dzialka_ids = await _province_dzialka_ids(province)
    if dzialka_ids:
        deleted_leads, deleted_deltas = await clear_gold_for_dzialki(dzialka_ids)
        reset_report.leads_deleted += deleted_leads
        reset_report.delta_results_deleted += deleted_deltas

    delta_report = await run_delta_engine(
        batch_size=max(100, len(dzialka_ids)),
        dzialka_ids=dzialka_ids or None,
    )

    return reset_report, stale_removed, geo_report, delta_report, autofix_actions


def _print_report(result: ProvinceCampaignResult) -> None:
    title = f"{result.before.display_name} — {result.stage}"
    print(f"\n{'=' * 72}")
    print(title)
    print(f"{'=' * 72}")
    print("Before:")
    print(
        f"  bronze={result.before.bronze_listings} pending={result.before.bronze_pending} "
        f"silver={result.before.silver_dzialki} dlq={result.before.silver_dlq} "
        f"zones={result.before.gold_planning_zones} leads={result.before.gold_investment_leads}"
    )
    print("After:")
    print(
        f"  bronze={result.after.bronze_listings} pending={result.after.bronze_pending} "
        f"silver={result.after.silver_dzialki} dlq={result.after.silver_dlq} "
        f"zones={result.after.gold_planning_zones} leads={result.after.gold_investment_leads}"
    )

    if result.sync_report:
        print(
            f"Sync: completed={result.sync_report['completed_gminy']} "
            f"failed={result.sync_report['failed_gminy']} "
            f"upserted={result.sync_report['total_features_upserted']}"
        )
    if result.geo_report:
        print(
            f"Geo: processed={result.geo_report['total_processed']} "
            f"resolved={result.geo_report['resolved']} "
            f"dlq={result.geo_report['sent_to_dlq']}"
        )
    if result.delta_report:
        print(
            f"Delta: analyzed={result.delta_report['dzialki_analyzed']} "
            f"deltas={result.delta_report['delta_results_created']} "
            f"leads_new={result.delta_report['leads_created']}"
        )
    if result.future_report:
        print(
            f"Future: analyzed={result.future_report['dzialki_analyzed']} "
            f"assessments_new={result.future_report['assessments_created']} "
            f"assessments_updated={result.future_report['assessments_updated']} "
            f"leads_new={result.future_report['leads_created']} "
            f"leads_updated={result.future_report['leads_updated']}"
        )
    if result.autofix_actions:
        print("Autofix:")
        for action in result.autofix_actions:
            print(f"  - {action}")
    if result.uncovered:
        print("Top uncovered:")
        for row in result.uncovered[:8]:
            print(
                f"  - {row['teryt']}: dzialki={row['dzialki_count']} "
                f"status={row.get('backlog_status', 'uncovered')} "
                f"category={row.get('coverage_category', '-')}"
                f" "
                f"localities={row.get('localities', '')} "
                f"covered_via={row.get('covered_via', row['teryt'])}"
            )
            if row.get("next_action"):
                print(f"    next_action={row['next_action']}")
    if result.after.dlq_by_error:
        print("DLQ by error:")
        for row in result.after.dlq_by_error[:6]:
            marker = " transient" if _looks_transient(str(row["error"])) else ""
            print(f"  - {row['error']}: {row['count']}{marker}")
    if result.after.dlq_by_category:
        print("DLQ by category:")
        for row in result.after.dlq_by_category[:6]:
            print(f"  - {row['category']}: {row['count']}")
    if result.after.manual_backlog_count:
        print(f"Manual backlog: {result.after.manual_backlog_count}")
        for row in result.after.manual_backlog_preview[:4]:
            print(
                f"  - {row['raw_teryt_input']}: "
                f"{row.get('title') or '-'}"
            )
    if result.after.lead_quality_summary:
        print("Lead quality:")
        for row in result.after.lead_quality_summary:
            print(f"  - {row['quality_signal']}: {row['count']}")
    if result.after.top_opportunities:
        print("Top opportunities:")
        for row in result.after.top_opportunities:
            price = row.get("price_per_m2_zl")
            coverage = row.get("max_coverage_pct")
            print(
                f"  - {row['identyfikator']}: "
                f"{price if price is not None else '-'} zł/m² "
                f"score={row['investment_score']} "
                f"signal={row['price_signal']} "
                f"quality={row['quality_signal']} "
                f"coverage={coverage if coverage is not None else '-'} "
                f"przezn={row.get('dominant_przeznaczenie') or '-'}"
            )
    if result.delta_gap and result.delta_gap.backlog_hints:
        if result.delta_gap.coverage_category_summary:
            print("Coverage blockers:")
            for row in result.delta_gap.coverage_category_summary:
                print(f"  - {row['coverage_category']}: {row['dzialki']} dzialki")
        print("Backlog hints:")
        for row in result.delta_gap.backlog_hints[:8]:
            localities = row.get("localities") or "-"
            print(
                f"  - {row['teryt_gmina']}: status={row['backlog_status']} "
                f"category={row.get('coverage_category', '-')} "
                f"why={row.get('why_no_lead', '-')} "
                f"dzialki={row['dzialki']} localities={localities} "
                f"covered_via={row.get('covered_via', row['teryt_gmina'])}"
            )
            if row.get("next_action"):
                print(f"    next_action={row['next_action']}")
    if result.delta_gap and result.delta_gap.covered_no_leads:
        print("Covered but no leads:")
        for row in result.delta_gap.covered_no_leads[:6]:
            print(
                f"  - {row['teryt_gmina']}: dzialki={row['dzialki']} "
                f"zones={row['planning_zones']} delta_rows={row.get('delta_rows', 0)} "
                f"why={row.get('why_no_lead', '-')} "
                f"localities={row.get('localities', '-')} "
                f"covered_via={row.get('covered_via', row['teryt_gmina'])}"
            )
    print(f"{'=' * 72}")


async def _run_campaign(
    *,
    province: str,
    stage: str,
    autofix: bool,
    parallel: bool,
) -> ProvinceCampaignResult:
    province_key = normalize_province(province)
    if province_key is None:
        raise ValueError(f"Unsupported province: {province!r}")

    before = await _status_snapshot(province_key, parallel=parallel)
    uncovered_before = [asdict(row) for row in await _fetch_uncovered_gminy(limit=12, province=province_key)]
    result = ProvinceCampaignResult(
        province=province_key,
        stage=stage,
        before=before,
        after=before,
        uncovered=uncovered_before,
        delta_gap=await _delta_gap_snapshot(province_key),
    )

    if stage in {"sync", "full"}:
        sync_report = await run_wfs_sync(province=province_key)
        result.sync_report = asdict(sync_report)
        if autofix:
            result.autofix_actions.extend(_sync_autofix_messages(sync_report))

    if stage in {"resolve", "replay", "full"}:
        if stage == "resolve":
            reset_report, stale_removed, geo_report, autofix_actions = await _run_scoped_resolution(
                province_key,
                autofix=autofix,
            )
            delta_report = None
        else:
            reset_report, stale_removed, geo_report, delta_report, autofix_actions = await _run_scoped_replay(
                province_key,
                autofix=autofix,
            )
        result.reset_report = asdict(reset_report)
        if stale_removed and f"stale_dlq_removed:{stale_removed}" not in autofix_actions:
            autofix_actions.insert(0, f"stale_dlq_removed:{stale_removed}")
        result.autofix_actions.extend(autofix_actions)
        result.geo_report = asdict(geo_report)
        if delta_report is not None:
            result.delta_report = asdict(delta_report)
            province_dzialka_ids = await _province_dzialka_ids(province_key)
            future_report: FutureBuildabilityReport = await run_future_buildability_engine(
                batch_size=max(100, len(province_dzialka_ids)),
                dzialka_ids=province_dzialka_ids or None,
            )
            result.future_report = asdict(future_report)

    result.after = await _status_snapshot(province_key, parallel=parallel)
    result.delta_gap = await _delta_gap_snapshot(province_key)
    result.uncovered = list(result.delta_gap.uncovered_gminy)
    _write_coverage_backlog_snapshot(
        province=province_key,
        before=result.before,
        after=result.after,
        delta_gap=result.delta_gap,
    )

    payload = await build_future_buildability_status_payload(province=province_key)
    _write_future_status_snapshot(province=province_key, payload=payload)
    result.future_status_reports[province_key] = payload
    return result


async def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    result = await _run_campaign(
        province=args.province,
        stage=args.stage,
        autofix=args.autofix,
        parallel=args.parallel,
    )

    if args.json:
        print(json.dumps(asdict(result), ensure_ascii=False, indent=2, default=str))
    else:
        _print_report(result)


if __name__ == "__main__":
    asyncio.run(main())
