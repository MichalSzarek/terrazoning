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


class SignalBreakdownItem(BaseModel):
    """Explainable score component for future-buildability leads."""

    kind: str
    status: str
    designation_raw: Optional[str] = None
    designation_normalized: Optional[str] = None
    weight: float
    source_url: Optional[str] = None
    evidence_label: Optional[str] = None


class MarketBenchmarkResponse(BaseModel):
    """Price-per-m² benchmark bundle for investor comparisons."""

    teryt_gmina: str
    scope: str
    sample_size: int
    p25_price_per_m2_zl: Optional[float] = None
    p40_price_per_m2_zl: Optional[float] = None
    median_price_per_m2_zl: Optional[float] = None


class PlanningSignalProperties(BaseModel):
    """Planning signal metadata for debug/operator review."""

    signal_id: UUID
    dzialka_id: Optional[UUID] = None
    teryt_gmina: str
    signal_kind: str
    signal_status: str
    designation_raw: Optional[str] = None
    designation_normalized: Optional[str] = None
    description: Optional[str] = None
    plan_name: Optional[str] = None
    uchwala_nr: Optional[str] = None
    effective_date: Optional[str] = None
    source_url: Optional[str] = None
    source_type: str
    source_confidence: float
    legal_weight: float
    evidence_chain: list[Any] = Field(default_factory=list)
    created_at: datetime


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
    strategy_type: str = Field(
        description="'current_buildable' | 'future_buildable'",
        examples=["future_buildable"],
    )
    confidence_band: Optional[str] = Field(
        default=None,
        description="'formal' | 'supported' | 'speculative' for future_buildable leads",
        examples=["formal"],
    )
    signal_quality_tier: Optional[str] = Field(
        default=None,
        description="'formal' | 'supported' | 'below_threshold' | 'blocked' investor-facing future-buildability tier",
        examples=["supported"],
    )
    next_best_action: Optional[str] = Field(
        default=None,
        description="Short operator action describing the next best step for this future-buildability candidate",
        examples=["Confirm the planning source and validate the parcel boundary."],
    )
    status: str = Field(
        description="'new' | 'reviewed' | 'shortlisted' | 'rejected' | 'invested'",
        examples=["new"],
    )
    reviewed_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp of the latest analyst workflow update",
    )
    notes: Optional[str] = Field(
        default=None,
        description="Optional analyst note attached to the lead",
    )

    # Spatial summary
    display_point: Optional[GeoJSONGeometry] = Field(
        default=None,
        description=(
            "Map marker anchor point in WGS84 (EPSG:4326), derived with "
            "ST_PointOnSurface so it stays inside the parcel polygon."
        ),
    )
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
    max_buildable_area_m2: Optional[float] = Field(
        default=None,
        description="Largest absolute intersection area in m² among qualifying buildable zones",
        examples=[1820.33],
    )
    dominant_przeznaczenie: Optional[str] = Field(
        default=None,
        description="Primary land-use designation of the highest-coverage zone",
        examples=["MN"],
    )
    price_zl: Optional[float] = Field(
        default=None,
        description="Auction starting / asking price parsed from the source listing",
        examples=[129000.00],
    )
    price_per_m2_zl: Optional[float] = Field(
        default=None,
        description="Auction asking price divided by parcel area in m²",
        examples=[178.45],
    )
    price_signal: str = Field(
        description="'reliable' | 'suspicious' | 'missing' — backend quality classification for ranking",
        examples=["reliable"],
    )
    quality_signal: str = Field(
        description="'complete' | 'partial' | 'missing_financials' | 'review_required'",
        examples=["complete"],
    )
    investment_score: float = Field(
        description="Composite investor ranking score that balances price, coverage, buildable area, and data quality",
        examples=[78.4],
    )
    future_signal_score: Optional[float] = Field(
        default=None,
        description="Explainable score from formal planning signals and spatial heuristics",
        examples=[65.0],
    )
    cheapness_score: Optional[float] = Field(
        default=None,
        description="Score contribution based on local price-per-m² percentile benchmarks",
        examples=[20.0],
    )
    overall_score: Optional[float] = Field(
        default=None,
        description="Stored overall future-buildability score (0-100)",
        examples=[82.0],
    )
    dominant_future_signal: Optional[str] = Field(
        default=None,
        description="Main forward-looking planning signal, e.g. 'pog_zone: residential'",
        examples=["pog_zone: residential"],
    )
    future_signal_count: Optional[int] = Field(
        default=None,
        description="How many planning signals contributed to the assessment",
        examples=[3],
    )
    distance_to_nearest_buildable_m: Optional[float] = Field(
        default=None,
        description="Distance in meters to the nearest current buildable planning zone",
        examples=[18.4],
    )
    adjacent_buildable_pct: Optional[float] = Field(
        default=None,
        description="Shared boundary ratio with current buildable zones (%)",
        examples=[27.8],
    )
    missing_metrics: list[str] = Field(
        default_factory=list,
        description="Explicit list of lead metrics still missing from the dataset",
        examples=[["price_per_m2_zl"]],
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
    source_url: Optional[str] = Field(
        default=None,
        description="Canonical source auction URL from bronze.raw_listings.source_url",
        examples=[
            "https://licytacje.komornik.pl/wyszukiwarka/obwieszczenia-o-licytacji/32027/licytacja-nieruchomosci-prawo-wlasnosci-nieruchomosci-gruntowej",
        ],
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
    signal_breakdown: list[SignalBreakdownItem] = Field(
        default_factory=list,
        description="Explainable breakdown of future-buildability score components",
    )

    created_at: datetime = Field(description="Lead creation timestamp (UTC)")


# ---------------------------------------------------------------------------
# Concrete GeoJSON types (FastAPI / OpenAPI uses these names in Swagger)
# ---------------------------------------------------------------------------

class LeadFeature(Feature[LeadProperties]):
    """A single investment lead as a GeoJSON Feature with MultiPolygon geometry."""
    # Pydantic v2 resolves the generic to concrete LeadProperties for OpenAPI
    pass


class PlanningSignalFeature(Feature[PlanningSignalProperties]):
    pass


class LeadsFeatureCollection(FeatureCollection[LeadProperties]):
    """GeoJSON FeatureCollection of investment leads — Mapbox-ready.

    Drop this directly into `map.addSource('leads', { type: 'geojson', data: response })`.
    """
    pass


class PlanningSignalsFeatureCollection(FeatureCollection[PlanningSignalProperties]):
    """GeoJSON FeatureCollection of normalized planning signals."""

    pass


class LeadStatusUpdatePayload(BaseModel):
    """Body for analyst workflow updates on a single lead."""

    status: str = Field(
        description="'reviewed' | 'shortlisted' | 'rejected' | 'invested'",
        examples=["shortlisted"],
    )
    notes: Optional[str] = Field(
        default=None,
        description="Optional analyst note saved on the lead",
        examples=["Dobra cena wejścia, sprawdzić dostęp do drogi."],
    )


class LeadStatusUpdateResponse(BaseModel):
    """Compact response after a lead workflow update."""

    lead_id: UUID
    status: str
    reviewed_at: Optional[datetime] = None
    notes: Optional[str] = None
