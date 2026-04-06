"""Pydantic schemas for the /api/v1/leads endpoint.

Response contract for the Frontend Lead / Mapbox consumption:
  - GeoJSON FeatureCollection, Mapbox-ready
  - Geometry: WGS84 (EPSG:4326), MultiPolygon
  - Properties: investment lead metadata + full evidence chain
  - Paginated via `limit` (not cursor — map viewport queries are limit-bounded)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from app.schemas.geojson import Feature, FeatureCollection, GeoJSONGeometry


# ---------------------------------------------------------------------------
# Properties model (the 'properties' block of each GeoJSON Feature)
# ---------------------------------------------------------------------------

class LeadProperties(BaseModel):
    """Investment lead metadata embedded in the GeoJSON Feature properties."""

    lead_id: UUID = Field(description="gold.investment_leads primary key")
    confidence_score: float = Field(
        ge=0.0, le=1.0,
        description="Aggregated score: Silver match_confidence × delta_score",
        examples=[0.92],
    )
    priority: str = Field(
        description="'high' (≥0.90) | 'medium' (≥0.75) | 'low'",
        examples=["high"],
    )
    status: str = Field(
        description="'new' | 'reviewed' | 'shortlisted' | 'rejected' | 'invested'",
        examples=["new"],
    )

    # Spatial summary
    area_m2: Optional[float] = Field(
        default=None,
        description="Parcel area in m² (GENERATED ALWAYS AS ST_Area(geom) in PostGIS)",
        examples=[3456.78],
    )
    max_coverage_pct: Optional[float] = Field(
        default=None,
        ge=0.0, le=100.0,
        description="Max % of parcel area covered by a buildable planning zone",
        examples=[72.5],
    )
    dominant_przeznaczenie: Optional[str] = Field(
        default=None,
        description="Primary land-use designation of the highest-coverage zone",
        examples=["MN"],
    )

    # Parcel identifiers
    identyfikator: str = Field(
        description="Canonical TERYT parcel key: {obreb}.{numer_dzialki}",
        examples=["141201_1.0001.123/4"],
    )
    teryt_gmina: str = Field(
        description="7-char TERYT gmina code",
        examples=["1412011"],
    )

    # Source linkage
    listing_id: Optional[UUID] = Field(
        default=None,
        description="Originating bronze.raw_listings row (NULL for plan-only leads)",
    )

    # Full evidence chain — every hop from source to delta
    evidence_chain: list[Any] = Field(
        default_factory=list,
        description=(
            "Ordered list of evidence steps: "
            "[{step:'source', ref:uuid, url:str}, "
            "{step:'parcel', ref:uuid, teryt:str}, "
            "{step:'delta', ref:uuid, coverage:float, przeznaczenie:str, plan:str}]"
        ),
    )

    created_at: datetime = Field(description="Lead creation timestamp (UTC)")


# ---------------------------------------------------------------------------
# Concrete GeoJSON types (FastAPI / OpenAPI uses these names in Swagger)
# ---------------------------------------------------------------------------

class LeadFeature(Feature[LeadProperties]):
    """A single investment lead as a GeoJSON Feature with MultiPolygon geometry."""
    # Pydantic v2 resolves the generic to concrete LeadProperties for OpenAPI
    pass


class LeadsFeatureCollection(FeatureCollection[LeadProperties]):
    """GeoJSON FeatureCollection of investment leads — Mapbox-ready.

    Drop this directly into `map.addSource('leads', { type: 'geojson', data: response })`.
    """
    pass
