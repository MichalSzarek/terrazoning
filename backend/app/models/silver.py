"""Silver layer ORM models — normalised, spatially resolved records.

Principle: EPSG:2180 is King.
- ALL geometry stored in PUWG 1992 (EPSG:2180).
- TERYT identyfikator is the canonical spatial foreign key.
- Every record carries match_confidence (0.00–1.00).
- ST_Transform to EPSG:4326 happens ONLY at the API response boundary.

GIS Specialist contract:
- Apply ST_MakeValid() BEFORE inserting into Dzialka.geom.
- Validate coordinate ranges: X ≈ 140k–900k, Y ≈ 100k–800k.
- Area discrepancy > 5% vs. ULDK → flag; > 15% → reject.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

from geoalchemy2 import Geometry
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Computed,
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

# ---------------------------------------------------------------------------
# silver.dzialki
# ---------------------------------------------------------------------------

RESOLUTION_STATUSES = ("pending", "resolved", "failed", "retry", "geometry_missing")
MATCH_METHODS = ("teryt_exact", "kw_lookup", "address_fuzzy", "uldk_partial", "manual")


class Dzialka(Base):
    """Normalised parcel with ULDK-resolved geometry in EPSG:2180.

    CRITICAL: Geometry column `geom` is MultiPolygon, SRID 2180.
    GeoAlchemy2 enforces the SRID at the ORM level.
    spatial_index=False — GiST index created in init SQL / Alembic migration.
    """

    __tablename__ = "dzialki"
    __table_args__ = (
        CheckConstraint(
            "match_confidence >= 0.00 AND match_confidence <= 1.00",
            name="ck_dzialki_match_confidence",
        ),
        CheckConstraint(
            f"resolution_status IN {RESOLUTION_STATUSES}",
            name="ck_dzialki_resolution_status",
        ),
        UniqueConstraint("identyfikator", name="uq_dzialki_identyfikator"),
        Index("idx_dzialki_gmina", "teryt_gmina"),
        Index("idx_dzialki_obreb", "teryt_obreb"),
        Index(
            "idx_dzialki_status",
            "resolution_status",
            postgresql_where=text("resolution_status != 'resolved'"),
        ),
        # GiST index on geom is defined in init SQL and future Alembic migrations.
        # spatial_index=False on the column prevents GeoAlchemy2 from auto-creating
        # a second index that would conflict.
        {"schema": "silver"},
    )

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )

    # TERYT hierarchy (Polish administrative structure)
    teryt_wojewodztwo: Mapped[str] = mapped_column(Text, nullable=False)  # 2-char
    teryt_powiat: Mapped[str] = mapped_column(Text, nullable=False)        # 4-char
    teryt_gmina: Mapped[str] = mapped_column(Text, nullable=False)         # 7-char
    teryt_obreb: Mapped[str] = mapped_column(Text, nullable=False)         # 9-char
    numer_dzialki: Mapped[str] = mapped_column(Text, nullable=False)       # e.g. '123/4'
    # Canonical spatial key: {teryt_obreb}.{numer_dzialki}
    identyfikator: Mapped[str] = mapped_column(Text, nullable=False)

    # EGiB land use code — required for genuine delta detection.
    # Source: Ewidencja Gruntów i Budynków (EGIB) / GUGiK.
    # Common codes: R (grunty orne), Ł (łąki), Ps (pastwiska), Ls (las),
    #   Lz (zadrzewione), S (sady), B (tereny mieszkaniowe - ALREADY BUILT),
    #   Ba (przemysłowe), Bi (inne zabudowane), Bp (zurbanizowane niezabudowane).
    # NULL when not yet fetched from EGiB. Delta Engine SKIPS leads without this field.
    current_use: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Geometry — EPSG:2180 enforced by GeoAlchemy2
    # spatial_index=False: index is managed in migrations, not auto-created here.
    geom: Mapped[Any] = mapped_column(
        Geometry("MULTIPOLYGON", srid=2180, spatial_index=False),
        nullable=False,
    )
    # Computed from ST_Area(geom) in m² — read-only, set by DB
    area_m2: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(12, 2),
        Computed("ST_Area(geom)", persisted=True),
        nullable=True,
    )

    # ULDK API metadata
    uldk_response_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    uldk_raw_response: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    # Confidence & resolution pipeline state
    match_confidence: Mapped[Decimal] = mapped_column(
        Numeric(3, 2),
        server_default=text("0.00"),
        nullable=False,
    )
    resolution_status: Mapped[str] = mapped_column(
        Text,
        server_default=text("'pending'"),
        nullable=False,
    )
    failure_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

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
    ksiegi_wieczyste: Mapped[list["KsiegaWieczysta"]] = relationship(
        "KsiegaWieczysta", back_populates="dzialka", lazy="select"
    )
    listing_parcels: Mapped[list["ListingParcel"]] = relationship(
        "ListingParcel", back_populates="dzialka", lazy="select"
    )
    def __repr__(self) -> str:
        return (
            f"<Dzialka id={self.id} identyfikator={self.identyfikator!r} "
            f"confidence={self.match_confidence} status={self.resolution_status!r}>"
        )


# ---------------------------------------------------------------------------
# silver.ksiegi_wieczyste
# ---------------------------------------------------------------------------


class KsiegaWieczysta(Base):
    """Land registry (KW) to parcel mapping.
    One KW can cover multiple działki; one działka can have multiple historical KWs.
    """

    __tablename__ = "ksiegi_wieczyste"
    __table_args__ = (
        Index("idx_kw_numer", "numer_kw"),
        Index("idx_kw_dzialka", "dzialka_id"),
        {"schema": "silver"},
    )

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    # Format: {kod_sadu}/{numer_ks}/{cyfra_kontrolna} e.g. 'WA1M/00012345/6'
    numer_kw: Mapped[str] = mapped_column(Text, nullable=False)
    dzialka_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("silver.dzialki.id", ondelete="CASCADE"),
        nullable=False,
    )
    sad_rejonowy: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_verified: Mapped[bool] = mapped_column(
        Boolean, server_default=text("false"), nullable=False
    )
    verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
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
    dzialka: Mapped["Dzialka"] = relationship(
        "Dzialka", back_populates="ksiegi_wieczyste", lazy="select"
    )

    def __repr__(self) -> str:
        return f"<KsiegaWieczysta numer_kw={self.numer_kw!r} verified={self.is_verified}>"


# ---------------------------------------------------------------------------
# silver.listing_parcels — junction: RawListing ↔ Dzialka
# ---------------------------------------------------------------------------


class ListingParcel(Base):
    """Junction table: one raw listing may correspond to one or many działki.
    match_method records HOW the linkage was established — critical for audit.
    """

    __tablename__ = "listing_parcels"
    __table_args__ = (
        CheckConstraint(
            "match_confidence >= 0.00 AND match_confidence <= 1.00",
            name="ck_listing_parcels_confidence",
        ),
        CheckConstraint(
            f"match_method IN {MATCH_METHODS}",
            name="ck_listing_parcels_method",
        ),
        UniqueConstraint("listing_id", "dzialka_id", name="uq_listing_parcel_pair"),
        Index("idx_listing_parcels_listing", "listing_id"),
        Index("idx_listing_parcels_dzialka", "dzialka_id"),
        {"schema": "silver"},
    )

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    listing_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("bronze.raw_listings.id", ondelete="CASCADE"),
        nullable=False,
    )
    dzialka_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("silver.dzialki.id", ondelete="CASCADE"),
        nullable=False,
    )
    match_confidence: Mapped[Decimal] = mapped_column(
        Numeric(3, 2),
        server_default=text("0.00"),
        nullable=False,
    )
    match_method: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    listing: Mapped["RawListing"] = relationship("RawListing", lazy="select")
    dzialka: Mapped["Dzialka"] = relationship(
        "Dzialka", back_populates="listing_parcels", lazy="select"
    )

    def __repr__(self) -> str:
        return (
            f"<ListingParcel listing={self.listing_id} dzialka={self.dzialka_id} "
            f"method={self.match_method!r} confidence={self.match_confidence}>"
        )


# ---------------------------------------------------------------------------
# silver.dlq_parcels — Dead Letter Queue
# ---------------------------------------------------------------------------


class DlqParcel(Base):
    """Dead Letter Queue for ULDK resolution failures.
    Retry schedule: 1h → 4h → 24h → 72h → manual review.
    Records with attempt_count == 5 require human intervention.
    """

    __tablename__ = "dlq_parcels"
    __table_args__ = (
        CheckConstraint(
            "attempt_count >= 1 AND attempt_count <= 5",
            name="ck_dlq_attempt_count",
        ),
        Index(
            "idx_dlq_retry",
            "next_retry_at",
            postgresql_where=text("attempt_count < 5"),
        ),
        Index("idx_dlq_listing", "listing_id"),
        {"schema": "silver"},
    )

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    listing_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("bronze.raw_listings.id", ondelete="CASCADE"),
        nullable=False,
    )
    raw_teryt_input: Mapped[str] = mapped_column(Text, nullable=False)
    attempt_count: Mapped[int] = mapped_column(
        Integer, server_default=text("1"), nullable=False
    )
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    next_retry_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=text("now() + INTERVAL '1 hour'"),
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

    # Relationships
    listing: Mapped["RawListing"] = relationship("RawListing", lazy="select")

    def __repr__(self) -> str:
        return (
            f"<DlqParcel id={self.id} attempts={self.attempt_count} "
            f"next_retry={self.next_retry_at}>"
        )
