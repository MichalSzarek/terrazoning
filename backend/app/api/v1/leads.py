"""Investment Leads endpoint — the primary consumer-facing API.

GET /api/v1/leads → GeoJSON FeatureCollection of investment leads.

Latency budget (Backend Lead Commandment 3):
  GeoJSON viewport query: p50 50ms | p99 150ms | Hard ceiling 500ms

Query architecture:
  - Raw SQL via text() — ST_AsGeoJSON + ST_Transform must be expressed in SQL
  - ST_Transform(d.geom, 4326) runs on the RESULT SET, not the full table
  - GiST index on silver.dzialki.geom is not used here (no bbox filter yet)
  - idx_leads_score (confidence_score DESC, priority) drives the filter + sort
  - JOIN on silver.dzialki via dzialka_id FK (indexed via idx_leads_dzialka)
  - LIMIT is mandatory — unbounded queries are bugs (Commandment 6)

GeoJSON contract:
  - Geometry: WGS84 (EPSG:4326), ready for Mapbox / MapLibre
  - Properties: see LeadProperties in app.schemas.leads
  - geometry field is produced by ST_AsGeoJSON(ST_Transform(geom, 4326))::json
    — PostGIS generates RFC 7946-compliant JSON; Python zero-touches coordinates

Future extension points (logged as backlog):
  - bbox filter: WHERE d.geom && ST_Transform(ST_MakeEnvelope(…,4326), 2180)
  - cursor pagination: WHERE il.confidence_score < :cursor ORDER BY …
  - status filter: WHERE il.status = :status
  - Redis cache TTL: 30s (invalidated by pipeline 'assessment-ready' event)
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional
from uuid import UUID

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_db
from app.models.gold import InvestmentLead
from app.services.ekw_links import build_ekw_search_url
from app.services.future_buildability_engine import FutureBuildabilityEngine
from app.services.future_buildability_engine import (
    _derive_next_best_action,
    _derive_signal_quality_tier,
)
from app.services.operations_scope import (
    classify_lead_quality,
    classify_price_signal,
    compute_investment_score,
)
from app.schemas.geojson import GeoJSONGeometry
from app.schemas.leads import (
    LeadFeature,
    LeadProperties,
    LeadStatusUpdatePayload,
    LeadStatusUpdateResponse,
    LeadsFeatureCollection,
    MarketBenchmarkResponse,
    PlanningSignalFeature,
    PlanningSignalProperties,
    PlanningSignalsFeatureCollection,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["leads"])

_MUTABLE_LEAD_STATUSES = {"reviewed", "shortlisted", "rejected", "invested"}

# ---------------------------------------------------------------------------
# SQL — one query, all data needed to build FeatureCollection
# ---------------------------------------------------------------------------

# Architecture notes:
#   - ST_AsGeoJSON(ST_Transform(d.geom, 4326))::json  → PostGIS outputs RFC 7946 JSON
#     asyncpg decodes ::json column to Python dict automatically
#   - We cast ::json not ::text so asyncpg gives us dict, not string
#   - NULLIF handles the rare case of a lead without a resolved dzialka geometry
#     (shouldn't happen — dzialka_id is FK NOT NULL, but defensive programming)
#   - ORDER BY is on the GiST-friendly (confidence_score DESC, created_at DESC)
#     using idx_leads_score index
#   - No COUNT(*) by default — it's O(n) on PostGIS tables; use ?include_count=true

_LEADS_QUERY = text(
    """
    SELECT
        il.id                                               AS lead_id,
        il.confidence_score,
        il.priority,
        COALESCE(il.strategy_type, 'current_buildable')     AS strategy_type,
        il.confidence_band,
        il.status,
        il.reviewed_at,
        il.notes,
        il.max_coverage_pct,
        (
            SELECT MAX(dr.intersection_area_m2)
            FROM gold.delta_results dr
            WHERE dr.dzialka_id = il.dzialka_id
              AND dr.is_upgrade = TRUE
        )                                                   AS max_buildable_area_m2,
        il.dominant_przeznaczenie,
        rl.price_zl,
        il.price_per_m2_zl,
        il.future_signal_score,
        il.cheapness_score,
        il.overall_score,
        il.dominant_future_signal,
        il.future_signal_count,
        il.distance_to_nearest_buildable_m,
        il.adjacent_buildable_pct,
        il.listing_id,
        rl.source_url,
        rl.raw_kw,
        il.evidence_chain,
        il.signal_breakdown,
        il.created_at,
        d.identyfikator,
        d.teryt_gmina,
        d.area_m2,
        CASE
            WHEN d.geom IS NULL THEN NULL
            ELSE ST_AsGeoJSON(
                ST_Transform(ST_PointOnSurface(d.geom), 4326)
            )::json
        END                                              AS display_point,
        -- PostGIS: EPSG:2180 → EPSG:4326, serialised to RFC 7946 JSON
        -- asyncpg decodes ::json to Python dict (no json.loads needed)
        ST_AsGeoJSON(ST_Transform(d.geom, 4326))::json      AS geometry
    FROM gold.investment_leads il
    JOIN silver.dzialki d ON d.id = il.dzialka_id
    LEFT JOIN bronze.raw_listings rl ON rl.id = il.listing_id
    WHERE il.confidence_score >= :min_score
      AND (CAST(:status_filter AS text) IS NULL OR il.status = CAST(:status_filter AS text))
      AND (CAST(:status_filter AS text) = 'rejected' OR il.status != 'rejected')
      AND (
          CAST(:strategy_filter AS text) IS NULL
          OR COALESCE(il.strategy_type, 'current_buildable') = CAST(:strategy_filter AS text)
      )
      AND (
          CAST(:confidence_band_filter AS text) IS NULL
          OR il.confidence_band = CAST(:confidence_band_filter AS text)
      )
      AND (
          CAST(:cheap_only AS boolean) = FALSE
          OR COALESCE(il.cheapness_score, 0) > 0
          OR (
              COALESCE(il.strategy_type, 'current_buildable') = 'current_buildable'
              AND il.price_per_m2_zl IS NOT NULL
          )
      )
      AND (CAST(:min_price_zl AS double precision) IS NULL OR rl.price_zl >= CAST(:min_price_zl AS double precision))
      AND (CAST(:max_price_zl AS double precision) IS NULL OR rl.price_zl <= CAST(:max_price_zl AS double precision))
      AND (
          CAST(:min_price_per_m2_zl AS double precision) IS NULL
          OR il.price_per_m2_zl >= CAST(:min_price_per_m2_zl AS double precision)
      )
      AND (
          CAST(:max_price_per_m2_zl AS double precision) IS NULL
          OR il.price_per_m2_zl <= CAST(:max_price_per_m2_zl AS double precision)
      )
      AND (CAST(:min_area_m2 AS double precision) IS NULL OR d.area_m2 >= CAST(:min_area_m2 AS double precision))
      AND (CAST(:max_area_m2 AS double precision) IS NULL OR d.area_m2 <= CAST(:max_area_m2 AS double precision))
      AND (
          CAST(:min_coverage_pct AS double precision) IS NULL
          OR il.max_coverage_pct >= CAST(:min_coverage_pct AS double precision)
      )
      AND (
          CAST(:min_buildable_area_m2 AS double precision) IS NULL
          OR (d.area_m2 * COALESCE(il.max_coverage_pct, 0) / 100.0) >= CAST(:min_buildable_area_m2 AS double precision)
      )
      AND (CAST(:teryt_prefix AS text) IS NULL OR d.teryt_gmina LIKE CAST(:teryt_prefix AS text) || '%')
      AND (CAST(:teryt_gmina AS text) IS NULL OR d.teryt_gmina = CAST(:teryt_gmina AS text))
      AND (
          CAST(:designation AS text) IS NULL
          OR COALESCE(il.dominant_przeznaczenie, '') ILIKE '%' || CAST(:designation AS text) || '%'
          OR COALESCE(il.dominant_future_signal, '') ILIKE '%' || CAST(:designation AS text) || '%'
      )
      AND (
          CAST(:search AS text) IS NULL
          OR d.identyfikator ILIKE '%' || CAST(:search AS text) || '%'
          OR COALESCE(rl.source_url, '') ILIKE '%' || CAST(:search AS text) || '%'
          OR COALESCE(il.dominant_future_signal, '') ILIKE '%' || CAST(:search AS text) || '%'
          OR COALESCE(il.notes, '') ILIKE '%' || CAST(:search AS text) || '%'
      )
    ORDER BY il.confidence_score DESC, il.created_at DESC
    LIMIT :limit
    """
)

_LEADS_COUNT_QUERY = text(
    """
    SELECT COUNT(*)
    FROM gold.investment_leads il
    JOIN silver.dzialki d ON d.id = il.dzialka_id
    LEFT JOIN bronze.raw_listings rl ON rl.id = il.listing_id
    WHERE il.confidence_score >= :min_score
      AND (CAST(:status_filter AS text) IS NULL OR il.status = CAST(:status_filter AS text))
      AND (CAST(:status_filter AS text) = 'rejected' OR il.status != 'rejected')
      AND (
          CAST(:strategy_filter AS text) IS NULL
          OR COALESCE(il.strategy_type, 'current_buildable') = CAST(:strategy_filter AS text)
      )
      AND (
          CAST(:confidence_band_filter AS text) IS NULL
          OR il.confidence_band = CAST(:confidence_band_filter AS text)
      )
      AND (
          CAST(:cheap_only AS boolean) = FALSE
          OR COALESCE(il.cheapness_score, 0) > 0
          OR (
              COALESCE(il.strategy_type, 'current_buildable') = 'current_buildable'
              AND il.price_per_m2_zl IS NOT NULL
          )
      )
      AND (CAST(:min_price_zl AS double precision) IS NULL OR rl.price_zl >= CAST(:min_price_zl AS double precision))
      AND (CAST(:max_price_zl AS double precision) IS NULL OR rl.price_zl <= CAST(:max_price_zl AS double precision))
      AND (
          CAST(:min_price_per_m2_zl AS double precision) IS NULL
          OR il.price_per_m2_zl >= CAST(:min_price_per_m2_zl AS double precision)
      )
      AND (
          CAST(:max_price_per_m2_zl AS double precision) IS NULL
          OR il.price_per_m2_zl <= CAST(:max_price_per_m2_zl AS double precision)
      )
      AND (CAST(:min_area_m2 AS double precision) IS NULL OR d.area_m2 >= CAST(:min_area_m2 AS double precision))
      AND (CAST(:max_area_m2 AS double precision) IS NULL OR d.area_m2 <= CAST(:max_area_m2 AS double precision))
      AND (
          CAST(:min_coverage_pct AS double precision) IS NULL
          OR il.max_coverage_pct >= CAST(:min_coverage_pct AS double precision)
      )
      AND (
          CAST(:min_buildable_area_m2 AS double precision) IS NULL
          OR (d.area_m2 * COALESCE(il.max_coverage_pct, 0) / 100.0) >= CAST(:min_buildable_area_m2 AS double precision)
      )
      AND (CAST(:teryt_prefix AS text) IS NULL OR d.teryt_gmina LIKE CAST(:teryt_prefix AS text) || '%')
      AND (CAST(:teryt_gmina AS text) IS NULL OR d.teryt_gmina = CAST(:teryt_gmina AS text))
      AND (
          CAST(:designation AS text) IS NULL
          OR COALESCE(il.dominant_przeznaczenie, '') ILIKE '%' || CAST(:designation AS text) || '%'
          OR COALESCE(il.dominant_future_signal, '') ILIKE '%' || CAST(:designation AS text) || '%'
      )
      AND (
          CAST(:search AS text) IS NULL
          OR d.identyfikator ILIKE '%' || CAST(:search AS text) || '%'
          OR COALESCE(rl.source_url, '') ILIKE '%' || CAST(:search AS text) || '%'
          OR COALESCE(il.dominant_future_signal, '') ILIKE '%' || CAST(:search AS text) || '%'
          OR COALESCE(il.notes, '') ILIKE '%' || CAST(:search AS text) || '%'
      )
    """
)

_LEAD_DETAIL_QUERY = text(
    _LEADS_QUERY.text.replace("ORDER BY il.confidence_score DESC, il.created_at DESC\n    LIMIT :limit", "AND il.id = :lead_id\n    LIMIT 1")
)

_PLANNING_SIGNALS_QUERY = text(
    """
    SELECT
        ps.id AS signal_id,
        ps.dzialka_id,
        ps.teryt_gmina,
        ps.signal_kind,
        ps.signal_status,
        ps.designation_raw,
        ps.designation_normalized,
        ps.description,
        ps.plan_name,
        ps.uchwala_nr,
        ps.effective_date,
        ps.source_url,
        ps.source_type,
        ps.source_confidence,
        ps.legal_weight,
        ps.evidence_chain,
        ps.created_at,
        CASE
            WHEN ps.geom IS NULL THEN NULL
            ELSE ST_AsGeoJSON(ST_Transform(ps.geom, 4326))::json
        END AS geometry
    FROM gold.planning_signals ps
    WHERE (CAST(:teryt_gmina AS text) IS NULL OR ps.teryt_gmina = CAST(:teryt_gmina AS text))
      AND (CAST(:signal_kind AS text) IS NULL OR ps.signal_kind = CAST(:signal_kind AS text))
      AND (CAST(:source_type AS text) IS NULL OR ps.source_type = CAST(:source_type AS text))
    ORDER BY ps.legal_weight DESC, ps.created_at DESC
    LIMIT :limit
    """
)


def _is_future_filter_requested(
    *,
    strategy_filter: str | None,
    confidence_band_filter: str | None,
) -> bool:
    return strategy_filter == "future_buildable" or confidence_band_filter is not None


def _enforce_future_buildability_flag(
    *,
    strategy_filter: str | None,
    confidence_band_filter: str | None,
) -> str | None:
    if settings.future_buildability_enabled:
        return strategy_filter
    if _is_future_filter_requested(
        strategy_filter=strategy_filter,
        confidence_band_filter=confidence_band_filter,
    ):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="future_buildability feature is disabled",
        )
    return "current_buildable" if strategy_filter is None else strategy_filter


def _future_buildability_explainability(
    *,
    strategy_type: str | None,
    confidence_band: str | None,
    signal_breakdown: list[dict[str, Any]] | None,
    dominant_future_signal: str | None,
) -> tuple[str | None, str | None]:
    return (
        _derive_signal_quality_tier(
            strategy_type=strategy_type,
            confidence_band=confidence_band,
            signal_breakdown=signal_breakdown,
        ),
        _derive_next_best_action(
            strategy_type=strategy_type,
            confidence_band=confidence_band,
            signal_breakdown=signal_breakdown,
            dominant_future_signal=dominant_future_signal,
        ),
    )


def _decode_geojson_geometry(raw_geometry: Any) -> Optional[GeoJSONGeometry]:
    """Normalize asyncpg/SQLAlchemy JSON output into a typed GeoJSON geometry."""

    if isinstance(raw_geometry, str):
        raw_geometry = json.loads(raw_geometry)
    if not raw_geometry:
        return None
    return GeoJSONGeometry(
        type=raw_geometry["type"],
        coordinates=raw_geometry.get("coordinates"),
    )


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get(
    "/leads",
    response_model=LeadsFeatureCollection,
    summary="List investment leads as GeoJSON FeatureCollection",
    description=(
        "Returns investment leads with parcel geometry projected to WGS84 (EPSG:4326), "
        "ready for direct use as a Mapbox/MapLibre GeoJSON source. "
        "Leads are sorted by `confidence_score DESC`. "
        "Default filter: `min_score=0.7`, `limit=100`."
    ),
    responses={
        200: {
            "description": "GeoJSON FeatureCollection — drop directly into Mapbox addSource()",
            "content": {
                "application/json": {
                    "example": {
                        "type": "FeatureCollection",
                        "count": 1,
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {
                                    "type": "MultiPolygon",
                                    "coordinates": [[[[21.01, 52.22], [21.02, 52.22],
                                                       [21.02, 52.23], [21.01, 52.22]]]],
                                },
                                "properties": {
                                    "lead_id": "550e8400-e29b-41d4-a716-446655440000",
                                    "confidence_score": 0.92,
                                    "priority": "high",
                                    "status": "new",
                                    "area_m2": 3456.78,
                                    "max_coverage_pct": 72.5,
                                    "max_buildable_area_m2": 2505.2,
                                    "dominant_przeznaczenie": "MN",
                                    "price_zl": 129000.0,
                                    "price_per_m2_zl": 178.45,
                                    "identyfikator": "141201_1.0001.123/4",
                                    "teryt_gmina": "1412011",
                                    "listing_id": None,
                                    "source_url": None,
                                    "kw_number": None,
                                    "ekw_search_url": None,
                                    "evidence_chain": [],
                                    "created_at": "2026-04-04T10:00:00Z",
                                },
                            }
                        ],
                    }
                }
            },
        }
    },
)
async def list_leads(
    min_score: float = Query(
        default=0.7,
        ge=0.0,
        le=1.0,
        description="Minimum confidence_score threshold (0.0–1.0)",
        examples={"default": {"value": 0.7}, "high_only": {"value": 0.9}},
    ),
    limit: int = Query(
        default=100,
        ge=1,
        le=500,
        description="Maximum number of leads to return (1–500). Hard ceiling: 500.",
    ),
    include_count: bool = Query(
        default=False,
        description=(
            "Set true to include total matching count in response metadata. "
            "Adds a COUNT(*) query — use sparingly on large datasets."
        ),
    ),
    status_filter: Optional[str] = Query(
        default=None,
        description="Optional workflow status filter: new | reviewed | shortlisted | rejected | invested",
    ),
    strategy_filter: Optional[str] = Query(
        default=None,
        description="Optional lead strategy filter: current_buildable | future_buildable",
    ),
    confidence_band_filter: Optional[str] = Query(
        default=None,
        description="Optional future-buildability band: formal | supported | speculative",
    ),
    strategy_type: Optional[str] = Query(
        default=None,
        description="Alias for strategy_filter: current_buildable | future_buildable",
    ),
    confidence_band: Optional[str] = Query(
        default=None,
        description="Alias for confidence_band_filter: formal | supported | speculative",
    ),
    cheap_only: bool = Query(
        default=False,
        description="When true, return only leads with a positive cheapness signal or a defined price/m².",
    ),
    min_price_zl: Optional[float] = Query(default=None, ge=0.0, description="Minimum total entry price in PLN."),
    max_price_zl: Optional[float] = Query(default=None, ge=0.0, description="Maximum total entry price in PLN."),
    min_price_per_m2_zl: Optional[float] = Query(default=None, ge=0.0, description="Minimum price per m² in PLN."),
    max_price_per_m2_zl: Optional[float] = Query(default=None, ge=0.0, description="Maximum price per m² in PLN."),
    min_area_m2: Optional[float] = Query(default=None, ge=0.0, description="Minimum parcel area in m²."),
    max_area_m2: Optional[float] = Query(default=None, ge=0.0, description="Maximum parcel area in m²."),
    min_coverage_pct: Optional[float] = Query(default=None, ge=0.0, le=100.0, description="Minimum planning coverage percentage."),
    min_buildable_area_m2: Optional[float] = Query(default=None, ge=0.0, description="Minimum buildable area estimate in m²."),
    teryt_prefix: Optional[str] = Query(default=None, description="Prefix of TERYT gmina, e.g. 12 for Małopolskie."),
    teryt_gmina: Optional[str] = Query(default=None, description="Exact 7-digit TERYT gmina code."),
    designation: Optional[str] = Query(default=None, description="Filter by dominant designation or dominant future signal."),
    search: Optional[str] = Query(default=None, description="Text search over parcel identifier, notes, source URL, and dominant future signal."),
    db: AsyncSession = Depends(get_db),
) -> LeadsFeatureCollection:
    """Return investment leads as a Mapbox-ready GeoJSON FeatureCollection.

    Geometry is stored in EPSG:2180 (PUWG 1992) internally. PostGIS projects
    it to WGS84 (EPSG:4326) in the query via ST_Transform — the API layer
    never touches coordinates.
    """
    logger.info(
        "[leads] GET /leads min_score=%.2f limit=%d include_count=%s status_filter=%s strategy_filter=%s confidence_band_filter=%s cheap_only=%s price=%s..%s price_m2=%s..%s area=%s..%s coverage>=%s buildable>=%s teryt_prefix=%s teryt_gmina=%s designation=%s search=%s",
        min_score, limit, include_count, status_filter, strategy_filter or strategy_type, confidence_band_filter or confidence_band, cheap_only,
        min_price_zl, max_price_zl, min_price_per_m2_zl, max_price_per_m2_zl, min_area_m2, max_area_m2, min_coverage_pct, min_buildable_area_m2,
        teryt_prefix, teryt_gmina, designation, search,
    )

    normalized_status_filter = status_filter.strip().lower() if status_filter else None
    raw_strategy_filter = strategy_type or strategy_filter
    raw_confidence_band_filter = confidence_band or confidence_band_filter
    normalized_strategy_filter = raw_strategy_filter.strip().lower() if raw_strategy_filter else None
    normalized_confidence_band_filter = (
        raw_confidence_band_filter.strip().lower() if raw_confidence_band_filter else None
    )
    normalized_teryt_prefix = teryt_prefix.strip() if teryt_prefix else None
    normalized_teryt_gmina = teryt_gmina.strip() if teryt_gmina else None
    normalized_designation = designation.strip() if designation else None
    normalized_search = search.strip() if search else None
    normalized_strategy_filter = _enforce_future_buildability_flag(
        strategy_filter=normalized_strategy_filter,
        confidence_band_filter=normalized_confidence_band_filter,
    )

    # Execute main spatial query
    result = await db.execute(
        _LEADS_QUERY,
        {
            "min_score": min_score,
            "limit": limit,
            "status_filter": normalized_status_filter,
            "strategy_filter": normalized_strategy_filter,
            "confidence_band_filter": normalized_confidence_band_filter,
            "cheap_only": cheap_only,
            "min_price_zl": min_price_zl,
            "max_price_zl": max_price_zl,
            "min_price_per_m2_zl": min_price_per_m2_zl,
            "max_price_per_m2_zl": max_price_per_m2_zl,
            "min_area_m2": min_area_m2,
            "max_area_m2": max_area_m2,
            "min_coverage_pct": min_coverage_pct,
            "min_buildable_area_m2": min_buildable_area_m2,
            "teryt_prefix": normalized_teryt_prefix,
            "teryt_gmina": normalized_teryt_gmina,
            "designation": normalized_designation,
            "search": normalized_search,
        },
    )
    rows = result.mappings().all()

    # Build GeoJSON features
    features: list[LeadFeature] = []
    for row in rows:
        geometry = _decode_geojson_geometry(row["geometry"])
        display_point = _decode_geojson_geometry(row["display_point"])

        price_signal = classify_price_signal(
            price_zl=row["price_zl"],
            price_per_m2_zl=row["price_per_m2_zl"],
        )
        quality_signal, missing_metrics = classify_lead_quality(
            price_zl=row["price_zl"],
            price_per_m2_zl=row["price_per_m2_zl"],
            area_m2=row["area_m2"],
            max_buildable_area_m2=row["max_buildable_area_m2"],
            max_coverage_pct=row["max_coverage_pct"],
            dominant_przeznaczenie=row["dominant_przeznaczenie"],
        )
        investment_score = compute_investment_score(
            confidence_score=row["confidence_score"],
            price_zl=row["price_zl"],
            price_per_m2_zl=row["price_per_m2_zl"],
            max_buildable_area_m2=row["max_buildable_area_m2"],
            max_coverage_pct=row["max_coverage_pct"],
            price_signal=price_signal,
            quality_signal=quality_signal,
        )
        if row["strategy_type"] == "future_buildable" and row["overall_score"] is not None:
            investment_score = float(row["overall_score"])
        signal_quality_tier, next_best_action = _future_buildability_explainability(
            strategy_type=row["strategy_type"],
            confidence_band=row["confidence_band"],
            signal_breakdown=row["signal_breakdown"] or [],
            dominant_future_signal=row["dominant_future_signal"],
        )

        props = LeadProperties(
            price_signal=price_signal,
            quality_signal=quality_signal,
            investment_score=investment_score,
            missing_metrics=list(missing_metrics),
            lead_id=row["lead_id"],
            confidence_score=float(row["confidence_score"]),
            priority=row["priority"],
            strategy_type=row["strategy_type"],
            confidence_band=row["confidence_band"],
            status=row["status"],
            reviewed_at=row["reviewed_at"],
            notes=row["notes"],
            display_point=display_point,
            area_m2=float(row["area_m2"]) if row["area_m2"] is not None else None,
            max_coverage_pct=(
                float(row["max_coverage_pct"])
                if row["max_coverage_pct"] is not None else None
            ),
            max_buildable_area_m2=(
                float(row["max_buildable_area_m2"])
                if row["max_buildable_area_m2"] is not None else None
            ),
            dominant_przeznaczenie=row["dominant_przeznaczenie"],
            price_zl=(
                float(row["price_zl"])
                if row["price_zl"] is not None else None
            ),
            price_per_m2_zl=(
                float(row["price_per_m2_zl"])
                if row["price_per_m2_zl"] is not None else None
            ),
            future_signal_score=(
                float(row["future_signal_score"])
                if row["future_signal_score"] is not None else None
            ),
            cheapness_score=(
                float(row["cheapness_score"])
                if row["cheapness_score"] is not None else None
            ),
            overall_score=(
                float(row["overall_score"])
                if row["overall_score"] is not None else None
            ),
            signal_quality_tier=signal_quality_tier,
            next_best_action=next_best_action,
            dominant_future_signal=row["dominant_future_signal"],
            future_signal_count=row["future_signal_count"],
            distance_to_nearest_buildable_m=(
                float(row["distance_to_nearest_buildable_m"])
                if row["distance_to_nearest_buildable_m"] is not None else None
            ),
            adjacent_buildable_pct=(
                float(row["adjacent_buildable_pct"])
                if row["adjacent_buildable_pct"] is not None else None
            ),
            identyfikator=row["identyfikator"],
            teryt_gmina=row["teryt_gmina"],
            listing_id=row["listing_id"],
            source_url=row["source_url"],
            kw_number=row["raw_kw"],
            ekw_search_url=build_ekw_search_url(row["raw_kw"]),
            evidence_chain=row["evidence_chain"] or [],
            signal_breakdown=row["signal_breakdown"] or [],
            created_at=row["created_at"],
        )

        features.append(LeadFeature(geometry=geometry, properties=props))

    count = len(features)

    # Optional: total count (adds a separate COUNT(*) query)
    if include_count:
        count_result = await db.execute(
            _LEADS_COUNT_QUERY,
            {
                "min_score": min_score,
                "status_filter": normalized_status_filter,
            "strategy_filter": normalized_strategy_filter,
            "confidence_band_filter": normalized_confidence_band_filter,
            "cheap_only": cheap_only,
            "min_price_zl": min_price_zl,
            "max_price_zl": max_price_zl,
            "min_price_per_m2_zl": min_price_per_m2_zl,
            "max_price_per_m2_zl": max_price_per_m2_zl,
            "min_area_m2": min_area_m2,
            "max_area_m2": max_area_m2,
            "min_coverage_pct": min_coverage_pct,
            "min_buildable_area_m2": min_buildable_area_m2,
            "teryt_prefix": normalized_teryt_prefix,
            "teryt_gmina": normalized_teryt_gmina,
            "designation": normalized_designation,
            "search": normalized_search,
        },
    )
        count = count_result.scalar_one()

    logger.info(
        "[leads] Returning %d features (min_score=%.2f limit=%d)",
        len(features), min_score, limit,
    )

    return LeadsFeatureCollection(features=features, count=count)


@router.get(
    "/leads/{lead_id}",
    response_model=LeadFeature,
    summary="Get a single investment lead as GeoJSON Feature",
)
async def get_lead(
    lead_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> LeadFeature:
    result = await db.execute(
        _LEAD_DETAIL_QUERY,
        {
            "min_score": 0.0,
            "limit": 1,
            "status_filter": None,
            "strategy_filter": None,
            "confidence_band_filter": None,
            "cheap_only": False,
            "min_price_zl": None,
            "max_price_zl": None,
            "min_price_per_m2_zl": None,
            "max_price_per_m2_zl": None,
            "min_area_m2": None,
            "max_area_m2": None,
            "min_coverage_pct": None,
            "min_buildable_area_m2": None,
            "teryt_prefix": None,
            "teryt_gmina": None,
            "designation": None,
            "search": None,
            "lead_id": lead_id,
        },
    )
    row = result.mappings().first()
    if row is not None:
        if (
            row["strategy_type"] == "future_buildable"
            and not settings.future_buildability_enabled
        ):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="future_buildability feature is disabled",
            )
        geometry = _decode_geojson_geometry(row["geometry"])
        display_point = _decode_geojson_geometry(row["display_point"])

        price_signal = classify_price_signal(
            price_zl=row["price_zl"],
            price_per_m2_zl=row["price_per_m2_zl"],
        )
        quality_signal, missing_metrics = classify_lead_quality(
            price_zl=row["price_zl"],
            price_per_m2_zl=row["price_per_m2_zl"],
            area_m2=row["area_m2"],
            max_buildable_area_m2=row["max_buildable_area_m2"],
            max_coverage_pct=row["max_coverage_pct"],
            dominant_przeznaczenie=row["dominant_przeznaczenie"],
        )
        investment_score = compute_investment_score(
            confidence_score=row["confidence_score"],
            price_zl=row["price_zl"],
            price_per_m2_zl=row["price_per_m2_zl"],
            max_buildable_area_m2=row["max_buildable_area_m2"],
            max_coverage_pct=row["max_coverage_pct"],
            price_signal=price_signal,
            quality_signal=quality_signal,
        )
        if row["strategy_type"] == "future_buildable" and row["overall_score"] is not None:
            investment_score = float(row["overall_score"])
        signal_quality_tier, next_best_action = _future_buildability_explainability(
            strategy_type=row["strategy_type"],
            confidence_band=row["confidence_band"],
            signal_breakdown=row["signal_breakdown"] or [],
            dominant_future_signal=row["dominant_future_signal"],
        )
        props = LeadProperties(
            price_signal=price_signal,
            quality_signal=quality_signal,
            investment_score=investment_score,
            missing_metrics=list(missing_metrics),
            lead_id=row["lead_id"],
            confidence_score=float(row["confidence_score"]),
            priority=row["priority"],
            strategy_type=row["strategy_type"],
            confidence_band=row["confidence_band"],
            status=row["status"],
            reviewed_at=row["reviewed_at"],
            notes=row["notes"],
            display_point=display_point,
            area_m2=float(row["area_m2"]) if row["area_m2"] is not None else None,
            max_coverage_pct=float(row["max_coverage_pct"]) if row["max_coverage_pct"] is not None else None,
            max_buildable_area_m2=float(row["max_buildable_area_m2"]) if row["max_buildable_area_m2"] is not None else None,
            dominant_przeznaczenie=row["dominant_przeznaczenie"],
            price_zl=float(row["price_zl"]) if row["price_zl"] is not None else None,
            price_per_m2_zl=float(row["price_per_m2_zl"]) if row["price_per_m2_zl"] is not None else None,
            future_signal_score=float(row["future_signal_score"]) if row["future_signal_score"] is not None else None,
            cheapness_score=float(row["cheapness_score"]) if row["cheapness_score"] is not None else None,
            overall_score=float(row["overall_score"]) if row["overall_score"] is not None else None,
            signal_quality_tier=signal_quality_tier,
            next_best_action=next_best_action,
            dominant_future_signal=row["dominant_future_signal"],
            future_signal_count=row["future_signal_count"],
            distance_to_nearest_buildable_m=float(row["distance_to_nearest_buildable_m"]) if row["distance_to_nearest_buildable_m"] is not None else None,
            adjacent_buildable_pct=float(row["adjacent_buildable_pct"]) if row["adjacent_buildable_pct"] is not None else None,
            identyfikator=row["identyfikator"],
            teryt_gmina=row["teryt_gmina"],
            listing_id=row["listing_id"],
            source_url=row["source_url"],
            kw_number=row["raw_kw"],
            ekw_search_url=build_ekw_search_url(row["raw_kw"]),
            evidence_chain=row["evidence_chain"] or [],
            signal_breakdown=row["signal_breakdown"] or [],
            created_at=row["created_at"],
        )
        return LeadFeature(geometry=geometry, properties=props)
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lead not found")


@router.get(
    "/future_buildability_signals",
    response_model=PlanningSignalsFeatureCollection,
    summary="List normalized planning signals as GeoJSON",
)
async def list_future_buildability_signals(
    teryt_gmina: Optional[str] = Query(default=None),
    signal_kind: Optional[str] = Query(default=None),
    source_type: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
) -> PlanningSignalsFeatureCollection:
    if not settings.future_buildability_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="future_buildability feature is disabled",
        )
    result = await db.execute(
        _PLANNING_SIGNALS_QUERY,
        {
            "teryt_gmina": teryt_gmina,
            "signal_kind": signal_kind,
            "source_type": source_type,
            "limit": limit,
        },
    )
    features: list[PlanningSignalFeature] = []
    for row in result.mappings().all():
        geom_data = row["geometry"]
        if isinstance(geom_data, str):
            geom_data = json.loads(geom_data)
        geometry: Optional[GeoJSONGeometry] = None
        if geom_data:
            geometry = GeoJSONGeometry(
                type=geom_data["type"],
                coordinates=geom_data.get("coordinates"),
            )
        props = PlanningSignalProperties(
            signal_id=row["signal_id"],
            dzialka_id=row["dzialka_id"],
            teryt_gmina=row["teryt_gmina"],
            signal_kind=row["signal_kind"],
            signal_status=row["signal_status"],
            designation_raw=row["designation_raw"],
            designation_normalized=row["designation_normalized"],
            description=row["description"],
            plan_name=row["plan_name"],
            uchwala_nr=row["uchwala_nr"],
            effective_date=row["effective_date"].isoformat() if row["effective_date"] else None,
            source_url=row["source_url"],
            source_type=row["source_type"],
            source_confidence=float(row["source_confidence"]),
            legal_weight=float(row["legal_weight"]),
            evidence_chain=row["evidence_chain"] or [],
            created_at=row["created_at"],
        )
        features.append(PlanningSignalFeature(geometry=geometry, properties=props))
    return PlanningSignalsFeatureCollection(features=features, count=len(features))


@router.get(
    "/market_benchmarks",
    response_model=MarketBenchmarkResponse,
    summary="Get local market price-per-m² benchmarks for a gmina",
)
async def get_market_benchmarks(
    teryt_gmina: str = Query(..., min_length=7, max_length=7),
    db: AsyncSession = Depends(get_db),
) -> MarketBenchmarkResponse:
    if not settings.future_buildability_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="future_buildability feature is disabled",
        )
    service = FutureBuildabilityEngine(db)
    benchmark = await service.market_benchmark(teryt_gmina)
    return MarketBenchmarkResponse(
        teryt_gmina=teryt_gmina,
        scope=benchmark.scope,
        sample_size=benchmark.sample_size,
        p25_price_per_m2_zl=float(benchmark.p25_price_per_m2_zl) if benchmark.p25_price_per_m2_zl is not None else None,
        p40_price_per_m2_zl=float(benchmark.p40_price_per_m2_zl) if benchmark.p40_price_per_m2_zl is not None else None,
        median_price_per_m2_zl=float(benchmark.median_price_per_m2_zl) if benchmark.median_price_per_m2_zl is not None else None,
    )


@router.patch(
    "/leads/{lead_id}/status",
    response_model=LeadStatusUpdateResponse,
    summary="Update analyst workflow status for a lead",
)
async def update_lead_status(
    lead_id: UUID,
    payload: LeadStatusUpdatePayload,
    db: AsyncSession = Depends(get_db),
) -> LeadStatusUpdateResponse:
    normalized_status = payload.status.strip().lower()
    if normalized_status not in _MUTABLE_LEAD_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unsupported status: {payload.status}",
        )

    lead = await db.get(InvestmentLead, lead_id)
    if lead is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Lead not found",
        )

    lead.status = normalized_status
    lead.notes = payload.notes.strip() if payload.notes else None
    lead.reviewed_at = datetime.now(timezone.utc)
    lead.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(lead)

    logger.info(
        "[leads] Updated lead %s status=%s",
        lead_id,
        normalized_status,
    )

    return LeadStatusUpdateResponse(
        lead_id=lead.id,
        status=lead.status,
        reviewed_at=lead.reviewed_at,
        notes=lead.notes,
    )
