"""Gold layer ORM models — spatial analysis results and investment leads.

Principle: Local-First GIS.
- Planning zones ingested and stored locally — NEVER queried via WMS in production.
- ST_Intersects executed in PostGIS, never Python-side.
- All geometry in EPSG:2180; intersection results validated for slivers (< 0.5 m²).

This is the layer the investor dashboard reads. confidence_score here is the
aggregated signal: it combines Silver match_confidence + delta_score.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

from geoalchemy2 import Geometry
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Computed,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base
from app.models.bronze import RawListing
from app.models.silver import Dzialka

# ---------------------------------------------------------------------------
# gold.planning_zones
# ---------------------------------------------------------------------------

PLAN_TYPES = ("mpzp", "pog", "studium")
PLANNING_SIGNAL_KINDS = (
    "pog_zone",
    "pog_ouz",
    "studium_zone",
    "mpzp_project",
    "planning_resolution",
    "coverage_only",
)
PLANNING_SIGNAL_STATUSES = (
    "formal_binding",
    "formal_directional",
    "formal_preparatory",
    "heuristic",
)
PLANNING_SIGNAL_SOURCE_TYPES = (
    "wfs",
    "wms_grid",
    "gison_popup",
    "pdf",
    "html_index",
    "planning_zone_passthrough",
    "manual_registry",
)
STRATEGY_TYPES = ("current_buildable", "future_buildable")
FUTURE_CONFIDENCE_BANDS = ("formal", "supported", "speculative")
FUTURE_CURRENT_BUILDABLE_STATUSES = ("non_buildable", "mixed", "already_buildable")


class PlanningZone(Base):
    """Local copy of a spatial planning zone polygon (MPZP / POG / Studium).

    This IS the local-first GIS cache described in the Architecture Commandments.
    Ingested from WFS/GML; validated with ST_MakeValid() before storage.
    spatial_index=False: GiST index managed in migrations.
    """

    __tablename__ = "planning_zones"
    __table_args__ = (
        CheckConstraint(
            f"plan_type IN {PLAN_TYPES}",
            name="ck_planning_zones_plan_type",
        ),
        UniqueConstraint(
            "source_wfs_url", "teryt_gmina", "przeznaczenie", "geom_hash",
            name="uq_planning_zones_spatial_key",
        ),
        Index("idx_planning_zones_gmina", "teryt_gmina"),
        Index("idx_planning_zones_type", "plan_type"),
        Index("idx_planning_zones_przeznaczenie", "przeznaczenie"),
        {"schema": "gold"},
    )

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )

    # Plan identification
    plan_type: Mapped[str] = mapped_column(Text, nullable=False)  # 'mpzp' | 'pog' | 'studium'
    plan_name: Mapped[str] = mapped_column(Text, nullable=False)
    uchwala_nr: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    teryt_gmina: Mapped[str] = mapped_column(Text, nullable=False)  # 7-char TERYT

    # Land use designation
    # Key values: MN, MW, U, R, ZL, KD — see DB_SCHEMA.md for full symbol table
    przeznaczenie: Mapped[str] = mapped_column(Text, nullable=False)
    przeznaczenie_opis: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Geometry — EPSG:2180 only
    geom: Mapped[Any] = mapped_column(
        Geometry("MULTIPOLYGON", srid=2180, spatial_index=False),
        nullable=False,
    )
    area_m2: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(14, 2),
        Computed("ST_Area(geom)", persisted=True),
        nullable=True,
    )

    # Spatial dedup key: "{round(centroid_x)}_{round(centroid_y)}" in EPSG:2180
    # Computed in Python before insert; used in upsert conflict target.
    geom_hash: Mapped[str] = mapped_column(Text, nullable=False)

    # Source metadata
    source_wfs_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    plan_effective_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # Relationships
    delta_results: Mapped[list["DeltaResult"]] = relationship(
        "DeltaResult", back_populates="planning_zone", lazy="select"
    )

    def __repr__(self) -> str:
        return (
            f"<PlanningZone id={self.id} type={self.plan_type!r} "
            f"przeznaczenie={self.przeznaczenie!r} gmina={self.teryt_gmina!r}>"
        )


# ---------------------------------------------------------------------------
# gold.delta_results
# ---------------------------------------------------------------------------


class DeltaResult(Base):
    """Result of ST_Intersects(dzialka.geom, planning_zone.geom).

    One row per (działka × plan_zone) pair. Multiple rows per działka are normal —
    a parcel may overlap multiple zones (e.g. 60% MN + 40% ZP).

    coverage_pct = ST_Area(intersection) / dzialka.area_m2 * 100

    GIS Specialist: intersection_geom must have slivers removed (< 0.5 m²)
    before storing. Use ST_MakeValid() on intersection output.
    """

    __tablename__ = "delta_results"
    __table_args__ = (
        CheckConstraint(
            "coverage_pct >= 0.00 AND coverage_pct <= 100.00",
            name="ck_delta_coverage_pct",
        ),
        CheckConstraint(
            "delta_score >= 0.00 AND delta_score <= 1.00",
            name="ck_delta_score",
        ),
        Index("idx_delta_dzialka", "dzialka_id"),
        Index("idx_delta_zone", "planning_zone_id"),
        Index(
            "idx_delta_upgrade",
            "is_upgrade",
            "delta_score",
            postgresql_where=text("is_upgrade"),
        ),
        # GiST on intersection_geom managed in migrations (nullable column)
        {"schema": "gold"},
    )

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    dzialka_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("silver.dzialki.id", ondelete="CASCADE"),
        nullable=False,
    )
    planning_zone_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("gold.planning_zones.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Spatial result — ST_Intersection output; NULL allowed for non-upgrade rows
    intersection_geom: Mapped[Optional[Any]] = mapped_column(
        Geometry("MULTIPOLYGON", srid=2180, spatial_index=False),
        nullable=True,
    )
    intersection_area_m2: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    # Percentage of działka area covered by this planning zone
    coverage_pct: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False)

    # Change in land designation (the "Delta")
    przeznaczenie_before: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    przeznaczenie_after: Mapped[str] = mapped_column(Text, nullable=False)
    is_upgrade: Mapped[bool] = mapped_column(
        Boolean, server_default=text("false"), nullable=False
    )

    # Score contribution to investment_leads.confidence_score
    delta_score: Mapped[Decimal] = mapped_column(
        Numeric(3, 2),
        server_default=text("0.00"),
        nullable=False,
    )

    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    dzialka: Mapped["Dzialka"] = relationship("Dzialka", lazy="select")
    planning_zone: Mapped["PlanningZone"] = relationship(
        "PlanningZone", back_populates="delta_results", lazy="select"
    )

    def __repr__(self) -> str:
        return (
            f"<DeltaResult id={self.id} dzialka={self.dzialka_id} "
            f"coverage={self.coverage_pct}% upgrade={self.is_upgrade}>"
        )


# ---------------------------------------------------------------------------
# gold.planning_signals
# ---------------------------------------------------------------------------


class PlanningSignal(Base):
    """Normalized planning signal store used by FutureBuildabilityEngine.

    Signals may come from geometry-backed sources (POG/Studium polygons),
    parcel-specific popup sources, or gmina-level PDF / HTML indexes without
    geometry. Geometry is therefore nullable by design.
    """

    __tablename__ = "planning_signals"
    __table_args__ = (
        CheckConstraint(
            f"signal_kind IN {PLANNING_SIGNAL_KINDS}",
            name="ck_planning_signals_kind",
        ),
        CheckConstraint(
            f"signal_status IN {PLANNING_SIGNAL_STATUSES}",
            name="ck_planning_signals_status",
        ),
        CheckConstraint(
            f"source_type IN {PLANNING_SIGNAL_SOURCE_TYPES}",
            name="ck_planning_signals_source_type",
        ),
        CheckConstraint(
            "source_confidence >= 0.00 AND source_confidence <= 1.00",
            name="ck_planning_signals_source_confidence",
        ),
        Index("idx_planning_signals_gmina", "teryt_gmina"),
        Index("idx_planning_signals_kind", "signal_kind"),
        Index("idx_planning_signals_status", "signal_status"),
        Index(
            "idx_planning_signals_dzialka",
            "dzialka_id",
            postgresql_where=text("dzialka_id IS NOT NULL"),
        ),
        {"schema": "gold"},
    )

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    dzialka_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("silver.dzialki.id", ondelete="CASCADE"),
        nullable=True,
    )
    teryt_gmina: Mapped[str] = mapped_column(Text, nullable=False)
    signal_kind: Mapped[str] = mapped_column(Text, nullable=False)
    signal_status: Mapped[str] = mapped_column(Text, nullable=False)
    designation_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    designation_normalized: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    plan_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    uchwala_nr: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    effective_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_type: Mapped[str] = mapped_column(
        Text,
        server_default=text("'manual_registry'"),
        nullable=False,
    )
    source_confidence: Mapped[Decimal] = mapped_column(
        Numeric(3, 2),
        server_default=text("1.00"),
        nullable=False,
    )
    legal_weight: Mapped[Decimal] = mapped_column(
        Numeric(6, 2),
        server_default=text("0.00"),
        nullable=False,
    )
    geom: Mapped[Optional[Any]] = mapped_column(
        Geometry("MULTIPOLYGON", srid=2180, spatial_index=False),
        nullable=True,
    )
    evidence_chain: Mapped[list] = mapped_column(
        JSONB,
        server_default=text("'[]'::jsonb"),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    dzialka: Mapped[Optional["Dzialka"]] = relationship("Dzialka", lazy="select")

    def __repr__(self) -> str:
        return (
            f"<PlanningSignal id={self.id} kind={self.signal_kind!r} "
            f"gmina={self.teryt_gmina!r} designation={self.designation_raw!r}>"
        )


# ---------------------------------------------------------------------------
# gold.future_buildability_assessments
# ---------------------------------------------------------------------------


class FutureBuildabilityAssessment(Base):
    """Rule-based assessment for parcels that may become buildable in the future."""

    __tablename__ = "future_buildability_assessments"
    __table_args__ = (
        CheckConstraint(
            "strategy_type = 'future_buildable'",
            name="ck_future_assessments_strategy_type",
        ),
        CheckConstraint(
            f"current_buildable_status IN {FUTURE_CURRENT_BUILDABLE_STATUSES}",
            name="ck_future_assessments_current_buildable_status",
        ),
        CheckConstraint(
            "future_signal_score >= 0.00 AND future_signal_score <= 100.00",
            name="ck_future_assessments_future_signal_score",
        ),
        CheckConstraint(
            "cheapness_score >= 0.00 AND cheapness_score <= 100.00",
            name="ck_future_assessments_cheapness_score",
        ),
        CheckConstraint(
            "overall_score >= 0.00 AND overall_score <= 100.00",
            name="ck_future_assessments_overall_score",
        ),
        CheckConstraint(
            f"confidence_band IN {FUTURE_CONFIDENCE_BANDS} OR confidence_band IS NULL",
            name="ck_future_assessments_confidence_band",
        ),
        UniqueConstraint(
            "dzialka_id",
            name="uq_future_assessments_dzialka",
        ),
        Index("idx_future_assessments_listing", "listing_id"),
        Index("idx_future_assessments_score", "overall_score"),
        {"schema": "gold"},
    )

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    dzialka_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("silver.dzialki.id", ondelete="CASCADE"),
        nullable=False,
    )
    listing_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("bronze.raw_listings.id", ondelete="SET NULL"),
        nullable=True,
    )
    strategy_type: Mapped[str] = mapped_column(
        Text,
        server_default=text("'future_buildable'"),
        nullable=False,
    )
    current_use: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    current_buildable_status: Mapped[str] = mapped_column(Text, nullable=False)
    future_signal_score: Mapped[Decimal] = mapped_column(Numeric(6, 2), nullable=False)
    cheapness_score: Mapped[Decimal] = mapped_column(
        Numeric(6, 2),
        server_default=text("0.00"),
        nullable=False,
    )
    overall_score: Mapped[Decimal] = mapped_column(Numeric(6, 2), nullable=False)
    confidence_band: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    dominant_future_signal: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    future_signal_count: Mapped[int] = mapped_column(
        Integer,
        server_default=text("0"),
        nullable=False,
    )
    distance_to_nearest_buildable_m: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(12, 2),
        nullable=True,
    )
    adjacent_buildable_pct: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        nullable=True,
    )
    price_per_m2_zl: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), nullable=True)
    status: Mapped[str] = mapped_column(
        Text,
        server_default=text("'assessed'"),
        nullable=False,
    )
    evidence_chain: Mapped[list] = mapped_column(
        JSONB,
        server_default=text("'[]'::jsonb"),
        nullable=False,
    )
    signal_breakdown: Mapped[list] = mapped_column(
        JSONB,
        server_default=text("'[]'::jsonb"),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    dzialka: Mapped["Dzialka"] = relationship("Dzialka", lazy="select")
    listing: Mapped[Optional["RawListing"]] = relationship("RawListing", lazy="select")

    def __repr__(self) -> str:
        return (
            f"<FutureBuildabilityAssessment dzialka={self.dzialka_id} "
            f"score={self.overall_score} band={self.confidence_band!r}>"
        )


# ---------------------------------------------------------------------------
# gold.investment_leads
# ---------------------------------------------------------------------------

LEAD_PRIORITIES = ("critical", "high", "medium", "low")
LEAD_STATUSES = ("new", "reviewed", "shortlisted", "rejected", "invested")


class InvestmentLead(Base):
    """Final investment lead — the row the investor dashboard reads.

    Aggregates: Silver match_confidence + Gold delta_score → confidence_score.
    evidence_chain JSON array links every step from source URL to delta result.

    Evidence chain format:
    [
      {"step": "source",   "ref": "<uuid>", "url": "https://..."},
      {"step": "document", "ref": "<uuid>", "uri": "gs://terrazoning-evidence/..."},
      {"step": "parcel",   "ref": "<uuid>", "teryt": "141201_1.0001.123/4"},
      {"step": "delta",    "ref": "<uuid>", "coverage": 72.5}
    ]
    """

    __tablename__ = "investment_leads"
    __table_args__ = (
        CheckConstraint(
            "confidence_score >= 0.00 AND confidence_score <= 1.00",
            name="ck_leads_confidence_score",
        ),
        CheckConstraint(
            f"priority IN {LEAD_PRIORITIES}",
            name="ck_leads_priority",
        ),
        CheckConstraint(
            f"status IN {LEAD_STATUSES}",
            name="ck_leads_status",
        ),
        CheckConstraint(
            f"strategy_type IN {STRATEGY_TYPES}",
            name="ck_leads_strategy_type",
        ),
        CheckConstraint(
            f"confidence_band IN {FUTURE_CONFIDENCE_BANDS} OR confidence_band IS NULL",
            name="ck_leads_confidence_band",
        ),
        Index(
            "idx_leads_score",
            "confidence_score",
            "priority",
            postgresql_using="btree",
        ),
        Index(
            "idx_leads_status",
            "status",
            postgresql_where=text("status NOT IN ('rejected')"),
        ),
        Index("idx_leads_dzialka", "dzialka_id"),
        Index(
            "idx_leads_listing",
            "listing_id",
            postgresql_where=text("listing_id IS NOT NULL"),
        ),
        Index("idx_leads_strategy", "strategy_type"),
        {"schema": "gold"},
    )

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    dzialka_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("silver.dzialki.id", ondelete="CASCADE"),
        nullable=False,
    )
    # NULL when lead generated from planning analysis alone (no auction listing)
    listing_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("bronze.raw_listings.id", ondelete="SET NULL"),
        nullable=True,
    )

    # Aggregated confidence
    confidence_score: Mapped[Decimal] = mapped_column(
        Numeric(3, 2),
        server_default=text("0.00"),
        nullable=False,
    )
    priority: Mapped[str] = mapped_column(
        Text,
        server_default=text("'medium'"),
        nullable=False,
    )
    strategy_type: Mapped[str] = mapped_column(
        Text,
        server_default=text("'current_buildable'"),
        nullable=False,
    )
    confidence_band: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Delta summary
    max_coverage_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2), nullable=True)
    dominant_przeznaczenie: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    price_per_m2_zl: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), nullable=True)
    estimated_value_uplift_pct: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(7, 2), nullable=True
    )
    future_signal_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    cheapness_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    overall_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    dominant_future_signal: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    future_signal_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    distance_to_nearest_buildable_m: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(12, 2), nullable=True
    )
    adjacent_buildable_pct: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2), nullable=True
    )
    signal_breakdown: Mapped[list] = mapped_column(
        JSONB,
        server_default=text("'[]'::jsonb"),
        nullable=False,
    )

    # Full evidence chain — never empty on a complete lead
    evidence_chain: Mapped[list] = mapped_column(
        JSONB,
        server_default=text("'[]'::jsonb"),
        nullable=False,
    )

    # Analyst workflow
    status: Mapped[str] = mapped_column(
        Text,
        server_default=text("'new'"),
        nullable=False,
    )
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # Relationships
    dzialka: Mapped["Dzialka"] = relationship("Dzialka", lazy="select")
    listing: Mapped[Optional["RawListing"]] = relationship("RawListing", lazy="select")

    def __repr__(self) -> str:
        return (
            f"<InvestmentLead id={self.id} score={self.confidence_score} "
            f"priority={self.priority!r} status={self.status!r}>"
        )
