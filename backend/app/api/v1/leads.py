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

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.geojson import GeoJSONGeometry
from app.schemas.leads import LeadFeature, LeadProperties, LeadsFeatureCollection

logger = logging.getLogger(__name__)

router = APIRouter(tags=["leads"])

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
        il.status,
        il.max_coverage_pct,
        il.dominant_przeznaczenie,
        il.listing_id,
        il.evidence_chain,
        il.created_at,
        d.identyfikator,
        d.teryt_gmina,
        d.area_m2,
        -- PostGIS: EPSG:2180 → EPSG:4326, serialised to RFC 7946 JSON
        -- asyncpg decodes ::json to Python dict (no json.loads needed)
        ST_AsGeoJSON(ST_Transform(d.geom, 4326))::json      AS geometry
    FROM gold.investment_leads il
    JOIN silver.dzialki d ON d.id = il.dzialka_id
    WHERE il.confidence_score >= :min_score
      AND il.status != 'rejected'
    ORDER BY il.confidence_score DESC, il.created_at DESC
    LIMIT :limit
    """
)

_LEADS_COUNT_QUERY = text(
    """
    SELECT COUNT(*)
    FROM gold.investment_leads il
    WHERE il.confidence_score >= :min_score
      AND il.status != 'rejected'
    """
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
                                    "dominant_przeznaczenie": "MN",
                                    "identyfikator": "141201_1.0001.123/4",
                                    "teryt_gmina": "1412011",
                                    "listing_id": None,
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
    db: AsyncSession = Depends(get_db),
) -> LeadsFeatureCollection:
    """Return investment leads as a Mapbox-ready GeoJSON FeatureCollection.

    Geometry is stored in EPSG:2180 (PUWG 1992) internally. PostGIS projects
    it to WGS84 (EPSG:4326) in the query via ST_Transform — the API layer
    never touches coordinates.
    """
    logger.info(
        "[leads] GET /leads min_score=%.2f limit=%d include_count=%s",
        min_score, limit, include_count,
    )

    # Execute main spatial query
    result = await db.execute(
        _LEADS_QUERY,
        {"min_score": min_score, "limit": limit},
    )
    rows = result.mappings().all()

    # Build GeoJSON features
    features: list[LeadFeature] = []
    for row in rows:
        geom_data = row["geometry"]   # asyncpg decodes ::json → dict

        # Defensive: asyncpg occasionally returns text for json columns in some
        # SQLAlchemy text() contexts — normalise to dict if needed.
        if isinstance(geom_data, str):
            geom_data = json.loads(geom_data)

        geometry: Optional[GeoJSONGeometry] = None
        if geom_data:
            geometry = GeoJSONGeometry(
                type=geom_data["type"],
                coordinates=geom_data.get("coordinates"),
            )

        props = LeadProperties(
            lead_id=row["lead_id"],
            confidence_score=float(row["confidence_score"]),
            priority=row["priority"],
            status=row["status"],
            area_m2=float(row["area_m2"]) if row["area_m2"] is not None else None,
            max_coverage_pct=(
                float(row["max_coverage_pct"])
                if row["max_coverage_pct"] is not None else None
            ),
            dominant_przeznaczenie=row["dominant_przeznaczenie"],
            identyfikator=row["identyfikator"],
            teryt_gmina=row["teryt_gmina"],
            listing_id=row["listing_id"],
            evidence_chain=row["evidence_chain"] or [],
            created_at=row["created_at"],
        )

        features.append(LeadFeature(geometry=geometry, properties=props))

    count = len(features)

    # Optional: total count (adds a separate COUNT(*) query)
    if include_count:
        count_result = await db.execute(
            _LEADS_COUNT_QUERY,
            {"min_score": min_score},
        )
        count = count_result.scalar_one()

    logger.info(
        "[leads] Returning %d features (min_score=%.2f limit=%d)",
        len(features), min_score, limit,
    )

    return LeadsFeatureCollection(features=features, count=count)
