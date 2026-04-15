from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.services.future_buildability_engine import FutureBuildabilityEngine
from app.services.operations_scope import province_display_name, province_teryt_prefix
from app.services.planning_signal_sync import probe_html_index_registry

_SIGNAL_COVERAGE_QUERY = text(
    """
    SELECT
        substr(ps.teryt_gmina, 1, 2) AS province_prefix,
        ps.signal_kind,
        ps.source_type,
        COUNT(*) AS row_count
    FROM gold.planning_signals ps
    WHERE (CAST(:province_prefix AS text) IS NULL OR substr(ps.teryt_gmina, 1, 2) = CAST(:province_prefix AS text))
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
    """
)

_PLANNING_SIGNAL_FRESHNESS_QUERY = text(
    """
    SELECT
        MAX(COALESCE(ps.updated_at, ps.created_at)) AS last_successful_sync_at,
        COUNT(*) AS row_count
    FROM gold.planning_signals ps
    WHERE (CAST(:province_prefix AS text) IS NULL OR substr(ps.teryt_gmina, 1, 2) = CAST(:province_prefix AS text))
      AND ps.source_type IN ('planning_zone_passthrough', 'html_index')
    """
)

_FUTURE_BUILDABILITY_FRESHNESS_QUERY = text(
    """
    SELECT
        MAX(COALESCE(fa.updated_at, fa.created_at)) AS last_successful_sync_at,
        COUNT(*) AS row_count
    FROM gold.future_buildability_assessments fa
    JOIN silver.dzialki d ON d.id = fa.dzialka_id
    WHERE (CAST(:province_prefix AS text) IS NULL OR substr(d.teryt_gmina, 1, 2) = CAST(:province_prefix AS text))
    """
)

_SOURCE_BACKLOG_QUERY = text(
    """
    SELECT
        ps.teryt_gmina,
        STRING_AGG(DISTINCT ps.source_type, ', ' ORDER BY ps.source_type) AS known_sources,
        MAX(COALESCE(ps.updated_at, ps.created_at)) AS last_source_sync_at
    FROM gold.planning_signals ps
    WHERE (CAST(:province_prefix AS text) IS NULL OR substr(ps.teryt_gmina, 1, 2) = CAST(:province_prefix AS text))
    GROUP BY ps.teryt_gmina
    """
)

_FUTURE_LEADS_QUERY = text(
    """
    SELECT
        substr(d.teryt_gmina, 1, 2) AS province_prefix,
        COALESCE(il.confidence_band, 'unclassified') AS confidence_band,
        COUNT(*) AS lead_count
    FROM gold.investment_leads il
    JOIN silver.dzialki d ON d.id = il.dzialka_id
    WHERE COALESCE(il.strategy_type, 'current_buildable') = 'future_buildable'
      AND (CAST(:province_prefix AS text) IS NULL OR substr(d.teryt_gmina, 1, 2) = CAST(:province_prefix AS text))
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
)

_NEAR_THRESHOLD_QUERY = text(
    """
    SELECT
        d.teryt_gmina,
        COUNT(DISTINCT fa.dzialka_id) AS parcel_count,
        MAX(fa.overall_score) AS max_overall_score,
        MAX(COALESCE(fa.updated_at, fa.created_at)) AS last_assessment_at
    FROM gold.future_buildability_assessments fa
    JOIN silver.dzialki d ON d.id = fa.dzialka_id
    WHERE fa.current_buildable_status = 'non_buildable'
      AND COALESCE(fa.overall_score, 0) >= 20
      AND COALESCE(fa.overall_score, 0) < 60
      AND (CAST(:province_prefix AS text) IS NULL OR substr(d.teryt_gmina, 1, 2) = CAST(:province_prefix AS text))
    GROUP BY d.teryt_gmina
    ORDER BY MAX(fa.overall_score) DESC, COUNT(DISTINCT fa.dzialka_id) DESC, d.teryt_gmina
    LIMIT 20
    """
)

_TOP_CANDIDATES_QUERY = text(
    """
    SELECT
        d.teryt_gmina,
        d.identyfikator,
        fa.overall_score,
        fa.future_signal_score,
        fa.cheapness_score,
        fa.distance_to_nearest_buildable_m,
        fa.adjacent_buildable_pct,
        fa.dominant_future_signal
    FROM gold.future_buildability_assessments fa
    JOIN silver.dzialki d ON d.id = fa.dzialka_id
    WHERE fa.current_buildable_status = 'non_buildable'
      AND fa.confidence_band IS NULL
      AND COALESCE(fa.overall_score, 0) >= 20
      AND (CAST(:province_prefix AS text) IS NULL OR substr(d.teryt_gmina, 1, 2) = CAST(:province_prefix AS text))
    ORDER BY fa.overall_score DESC, fa.future_signal_score DESC, d.teryt_gmina
    LIMIT 10
    """
)

_BENCHMARK_SCOPE_CANDIDATES_QUERY = text(
    """
    SELECT DISTINCT ON (d.teryt_gmina)
        d.teryt_gmina,
        d.identyfikator,
        fa.overall_score,
        fa.price_per_m2_zl
    FROM gold.future_buildability_assessments fa
    JOIN silver.dzialki d ON d.id = fa.dzialka_id
    WHERE fa.current_buildable_status = 'non_buildable'
      AND COALESCE(fa.overall_score, 0) >= 20
      AND (CAST(:province_prefix AS text) IS NULL OR substr(d.teryt_gmina, 1, 2) = CAST(:province_prefix AS text))
    ORDER BY d.teryt_gmina, fa.overall_score DESC, d.identyfikator
    LIMIT 20
    """
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print future-buildability rollout status")
    parser.add_argument(
        "--province",
        choices=("slaskie", "malopolskie"),
        help="Optional province scope for coverage and backlog sections",
    )
    parser.add_argument(
        "--skip-html-probe",
        action="store_true",
        help="Skip live html_index probing to avoid slow or hanging upstream sources",
    )
    return parser.parse_args(argv)


def _format_timestamp(value: datetime | None) -> str:
    if value is None:
        return "-"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _format_age(now: datetime, value: datetime | None) -> str:
    if value is None:
        return "-"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    delta = now - value.astimezone(timezone.utc)
    hours = delta.total_seconds() / 3600
    return f"{hours:.1f}h"


def _next_best_source_type(known_sources: set[str], html_status: str | None) -> str:
    if html_status == "upstream_broken":
        return "manual_research"
    if not known_sources:
        return "wykazplanow"
    if "planning_zone_passthrough" not in known_sources:
        return "studium_or_pog_geometry"
    if known_sources.intersection({"html_index"}):
        return "mpzp_project_or_resolution"
    return "heuristics_tuning"


def _operator_status(known_sources: set[str], html_status: str | None) -> str:
    if html_status == "upstream_broken":
        return "upstream_broken"
    if not known_sources:
        return "ready"
    if "planning_zone_passthrough" not in known_sources:
        return "needs_geometry_source"
    if html_status == "live":
        return "live"
    return "partial"


def _has_reliable_benchmark(sample_size: int | None, median_price_per_m2_zl: Any | None) -> bool:
    return bool(sample_size and sample_size >= 5 and median_price_per_m2_zl is not None)


async def _fetch_rows(query, *, province_prefix: str | None) -> list[dict[str, Any]]:
    async with AsyncSessionLocal() as db:
        result = await db.execute(query, {"province_prefix": province_prefix})
        return [dict(row) for row in result.mappings().all()]


async def _fetch_single_row(query, *, province_prefix: str | None) -> dict[str, Any] | None:
    rows = await _fetch_rows(query, province_prefix=province_prefix)
    return rows[0] if rows else None


def _build_backlog_entry(
    *,
    threshold_row: dict[str, Any],
    source_row: dict[str, Any] | None,
    html_probe: Any | None,
) -> dict[str, Any]:
    known_sources = {
        value.strip()
        for value in (
            (source_row.get("known_sources") if source_row else "") or ""
        ).split(",")
        if value.strip()
    }
    html_status = html_probe.status if html_probe else None
    return {
        "teryt_gmina": threshold_row["teryt_gmina"],
        "parcel_count": int(threshold_row["parcel_count"]),
        "max_overall_score": float(threshold_row["max_overall_score"]),
        "known_sources": sorted(known_sources),
        "next_best_source_type": _next_best_source_type(known_sources, html_status),
        "operator_status": _operator_status(known_sources, html_status),
        "html_index_status": html_status,
        "html_index_error": html_probe.error if html_probe else None,
        "last_assessment_at": _format_timestamp(threshold_row.get("last_assessment_at")),
        "last_source_sync_at": _format_timestamp(
            source_row.get("last_source_sync_at") if source_row else None
        ),
    }


async def _load_backlog_rows(
    *,
    province_prefix: str | None,
    html_sources: list[Any] | None = None,
) -> list[dict[str, Any]]:
    if html_sources is None:
        html_sources = await probe_html_index_registry(teryt_gmina=None)

    threshold_rows, source_rows = await asyncio.gather(
        _fetch_rows(_NEAR_THRESHOLD_QUERY, province_prefix=province_prefix),
        _fetch_rows(_SOURCE_BACKLOG_QUERY, province_prefix=province_prefix),
    )

    source_by_teryt = {row["teryt_gmina"]: row for row in source_rows}
    html_by_teryt = {
        probe.teryt_gmina: probe
        for probe in html_sources
        if province_prefix is None or probe.teryt_gmina.startswith(province_prefix)
    }

    rows = [
        _build_backlog_entry(
            threshold_row=row,
            source_row=source_by_teryt.get(row["teryt_gmina"]),
            html_probe=html_by_teryt.get(row["teryt_gmina"]),
        )
        for row in threshold_rows
    ]
    return rows


async def _load_freshness_snapshot(*, province_prefix: str | None) -> dict[str, Any]:
    planning_row, future_row = await asyncio.gather(
        _fetch_single_row(_PLANNING_SIGNAL_FRESHNESS_QUERY, province_prefix=province_prefix),
        _fetch_single_row(_FUTURE_BUILDABILITY_FRESHNESS_QUERY, province_prefix=province_prefix),
    )
    return {
        "planning_signals": planning_row or {},
        "future_buildability": future_row or {},
    }


async def _load_benchmark_gap_rows(*, province_prefix: str | None) -> list[dict[str, Any]]:
    candidate_rows = await _fetch_rows(_BENCHMARK_SCOPE_CANDIDATES_QUERY, province_prefix=province_prefix)
    if not candidate_rows:
        return []

    rows: list[dict[str, Any]] = []
    async with AsyncSessionLocal() as db:
        engine = FutureBuildabilityEngine(db)
        for candidate in candidate_rows:
            benchmark = await engine.market_benchmark(candidate["teryt_gmina"])
            if candidate["price_per_m2_zl"] is not None and _has_reliable_benchmark(
                benchmark.sample_size,
                benchmark.median_price_per_m2_zl,
            ):
                continue
            rows.append(
                {
                    "teryt_gmina": candidate["teryt_gmina"],
                    "identyfikator": candidate["identyfikator"],
                    "overall_score": float(candidate["overall_score"]),
                    "price_per_m2_zl": (
                        float(candidate["price_per_m2_zl"])
                        if candidate["price_per_m2_zl"] is not None
                        else None
                    ),
                    "benchmark_scope": benchmark.scope,
                    "benchmark_sample_size": benchmark.sample_size,
                    "benchmark_has_reliable_support": _has_reliable_benchmark(
                        benchmark.sample_size,
                        benchmark.median_price_per_m2_zl,
                    ),
                }
            )
    return rows


async def build_future_buildability_status_payload(
    *,
    province: str | None = None,
    skip_html_probe: bool = False,
) -> dict[str, Any]:
    province_prefix = province_teryt_prefix(province) if province else None
    province_label = province_display_name(province) if province else "Śląskie + Małopolskie"
    now = datetime.now(timezone.utc)

    if skip_html_probe:
        html_sources = []
    else:
        try:
            html_sources = await asyncio.wait_for(probe_html_index_registry(), timeout=20.0)
        except asyncio.TimeoutError:
            html_sources = []
    # Cloud SQL has a fairly tight connection ceiling, and each helper opens
    # its own session. Running all six concurrently is fast locally but can
    # exhaust shared connections during heavy province campaigns. Keep this
    # status report deterministic and cheap by resolving the queries in
    # sequence.
    coverage_rows = await _fetch_rows(_SIGNAL_COVERAGE_QUERY, province_prefix=province_prefix)
    lead_rows = await _fetch_rows(_FUTURE_LEADS_QUERY, province_prefix=province_prefix)
    backlog_rows = await _load_backlog_rows(province_prefix=province_prefix, html_sources=html_sources)
    top_rows = await _fetch_rows(_TOP_CANDIDATES_QUERY, province_prefix=province_prefix)
    freshness = await _load_freshness_snapshot(province_prefix=province_prefix)
    benchmark_gap_rows = await _load_benchmark_gap_rows(province_prefix=province_prefix)

    html_index_status_by_teryt = {
        row.teryt_gmina: row
        for row in html_sources
        if province_prefix is None or row.teryt_gmina.startswith(province_prefix)
    }
    source_health: dict[str, int] = defaultdict(int)
    for probe in html_index_status_by_teryt.values():
        source_health[probe.status] += 1

    return {
        "generated_at": now.astimezone(timezone.utc).isoformat(timespec="seconds"),
        "province": province,
        "province_label": province_label,
        "freshness": {
            "planning_signals_last_successful_sync_at": _format_timestamp(
                freshness["planning_signals"].get("last_successful_sync_at")
            ),
            "planning_signals_age": _format_age(now, freshness["planning_signals"].get("last_successful_sync_at")),
            "planning_signals_row_count": int(freshness["planning_signals"].get("row_count", 0) or 0),
            "future_buildability_last_successful_sync_at": _format_timestamp(
                freshness["future_buildability"].get("last_successful_sync_at")
            ),
            "future_buildability_age": _format_age(now, freshness["future_buildability"].get("last_successful_sync_at")),
            "future_buildability_row_count": int(freshness["future_buildability"].get("row_count", 0) or 0),
        },
        "planning_signal_coverage": coverage_rows,
        "source_health_summary": dict(source_health),
        "html_index_sources": [
            {
                "teryt_gmina": probe.teryt_gmina,
                "label": probe.label,
                "source_url": probe.source_url,
                "status": probe.status,
                "signals_detected": probe.signals_detected,
                "error": probe.error,
            }
            for probe in sorted(html_index_status_by_teryt.values(), key=lambda item: item.teryt_gmina)
        ],
        "broken_upstream_sources": [
            {
                "teryt_gmina": probe.teryt_gmina,
                "label": probe.label,
                "error": probe.error,
            }
            for probe in html_index_status_by_teryt.values()
            if probe.status == "upstream_broken"
        ],
        "near_threshold_backlog": backlog_rows,
        "future_leads_by_province": lead_rows,
        "top_candidates_without_enough_heuristics": top_rows,
        "candidates_missing_benchmark_support": benchmark_gap_rows,
    }


def render_future_buildability_status_text(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(f"Future Buildability Status — {payload['province_label']}")
    lines.append("")

    freshness = payload["freshness"]
    lines.append("freshness")
    lines.append(
        "  - planning_signals_last_successful_sync_at="
        f"{freshness['planning_signals_last_successful_sync_at']} "
        f"(age={freshness['planning_signals_age']}, rows={freshness['planning_signals_row_count']})"
    )
    lines.append(
        "  - future_buildability_last_successful_sync_at="
        f"{freshness['future_buildability_last_successful_sync_at']} "
        f"(age={freshness['future_buildability_age']}, rows={freshness['future_buildability_row_count']})"
    )
    lines.append("")

    lines.append("planning signal coverage")
    coverage_rows = payload["planning_signal_coverage"]
    if not coverage_rows:
        lines.append("  - no planning signals loaded")
    else:
        for row in coverage_rows:
            prefix = row["province_prefix"] or "--"
            lines.append(f"  - {prefix} | {row['signal_kind']} | {row['source_type']} | {row['row_count']}")
    lines.append("")

    lines.append("source health summary")
    source_health = payload["source_health_summary"]
    if not source_health:
        lines.append("  - no html_index sources in scope")
    else:
        for status_name in ("live", "partial", "upstream_broken"):
            lines.append(f"  - {status_name}={source_health.get(status_name, 0)}")
    lines.append("")

    lines.append("html_index live sources")
    html_sources = payload["html_index_sources"]
    if not html_sources:
        lines.append("  - no html_index sources in scope")
    else:
        for probe in html_sources:
            suffix = f" | signals={probe['signals_detected']}"
            if probe.get("error"):
                suffix += f" | error={probe['error']}"
            lines.append(f"  - {probe['teryt_gmina']} | {probe['status']}{suffix}")
    lines.append("")

    lines.append("broken upstream sources")
    broken = payload["broken_upstream_sources"]
    if not broken:
        lines.append("  - none")
    else:
        for probe in broken:
            lines.append(f"  - {probe['teryt_gmina']} | {probe['label']} | {probe['error']}")
    lines.append("")

    lines.append("gminy near-threshold")
    backlog_rows = payload["near_threshold_backlog"]
    if not backlog_rows:
        lines.append("  - no near-threshold non-buildable backlog")
    else:
        for row in backlog_rows:
            known_sources = ",".join(row["known_sources"]) or "-"
            lines.append(
                "  - "
                f"{row['teryt_gmina']} | parcels={row['parcel_count']} | "
                f"max_score={float(row['max_overall_score']):.0f} | "
                f"known_sources={known_sources} | "
                f"next_best_source_type={row['next_best_source_type']} | "
                f"operator_status={row['operator_status']} | "
                f"last_assessment_at={row['last_assessment_at']} | "
                f"last_source_sync_at={row['last_source_sync_at']}"
            )
    lines.append("")

    lines.append("future leads by province")
    lead_rows = payload["future_leads_by_province"]
    if not lead_rows:
        lines.append("  - none")
    else:
        grouped: dict[str, list[str]] = defaultdict(list)
        for row in lead_rows:
            grouped[row["province_prefix"] or "--"].append(f"{row['confidence_band']}={row['lead_count']}")
        for prefix, entries in sorted(grouped.items()):
            lines.append(f"  - {prefix} | {' | '.join(entries)}")
    lines.append("")

    lines.append("top candidates without enough heuristics")
    top_rows = payload["top_candidates_without_enough_heuristics"]
    if not top_rows:
        lines.append("  - none")
    else:
        for row in top_rows:
            distance = (
                f"{float(row['distance_to_nearest_buildable_m']):.1f}m"
                if row["distance_to_nearest_buildable_m"] is not None
                else "-"
            )
            adjacency = (
                f"{float(row['adjacent_buildable_pct']):.1f}%"
                if row["adjacent_buildable_pct"] is not None
                else "-"
            )
            lines.append(
                "  - "
                f"{row['teryt_gmina']} | {row['identyfikator']} | "
                f"overall={float(row['overall_score']):.0f} | "
                f"signal={float(row['future_signal_score']):.0f} | "
                f"cheapness={float(row['cheapness_score'] or 0):.0f} | "
                f"dist={distance} | adjacent={adjacency} | "
                f"dominant={row['dominant_future_signal'] or '-'}"
            )
    lines.append("")

    lines.append("candidates missing benchmark support")
    benchmark_gap_rows = payload["candidates_missing_benchmark_support"]
    if not benchmark_gap_rows:
        lines.append("  - none")
    else:
        for row in benchmark_gap_rows:
            price_label = (
                f"{row['price_per_m2_zl']:.2f} zł/m²"
                if row["price_per_m2_zl"] is not None
                else "price_per_m2 missing"
            )
            benchmark_label = (
                f"{row['benchmark_scope']} sample={row['benchmark_sample_size']}"
                if row["benchmark_has_reliable_support"]
                else f"{row['benchmark_scope']} sample={row['benchmark_sample_size']} unreliable"
            )
            lines.append(
                "  - "
                f"{row['teryt_gmina']} | {row['identyfikator']} | "
                f"overall={row['overall_score']:.0f} | {price_label} | {benchmark_label}"
            )
    return "\n".join(lines)


async def _main() -> None:
    if not settings.future_buildability_enabled:
        print("future_buildability_status feature=disabled")
        return

    args = parse_args()
    payload = await build_future_buildability_status_payload(
        province=args.province,
        skip_html_probe=args.skip_html_probe,
    )
    print(render_future_buildability_status_text(payload))


if __name__ == "__main__":
    asyncio.run(_main())
