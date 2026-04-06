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
    ForeignKey,
    Index,
    Numeric,
    Text,
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

    # Source metadata
    source_wfs_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ingested_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), nullable=False
    )
    plan_effective_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
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
        server_default=func.now(), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), nullable=False
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

    # Delta summary
    max_coverage_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2), nullable=True)
    dominant_przeznaczenie: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    price_per_m2_zl: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), nullable=True)
    estimated_value_uplift_pct: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(7, 2), nullable=True
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
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
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
