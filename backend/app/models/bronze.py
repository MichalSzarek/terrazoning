"""Bronze layer ORM models — raw, untransformed ingestion data.

Principle: Trust No One. Data lands here exactly as scraped.
No geometry transformations, no normalisation — append-only for audit integrity.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
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

# ---------------------------------------------------------------------------
# bronze.scrape_runs
# ---------------------------------------------------------------------------

SCRAPE_RUN_STATUSES = ("running", "completed", "failed", "partial")


class ScrapeRun(Base):
    """Meta-data for every scraper invocation. Never delete — needed for Evidence Chain."""

    __tablename__ = "scrape_runs"
    __table_args__ = (
        Index("idx_scrape_runs_source", "source_name", "started_at"),
        {"schema": "bronze"},
    )

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(
        Text,
        CheckConstraint(
            f"status IN {SCRAPE_RUN_STATUSES}",
            name="ck_scrape_runs_status",
        ),
        server_default=text("'running'"),
        nullable=False,
    )
    records_found: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    records_saved: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    proxy_used: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # NOTE: Python attribute 'job_metadata' maps to DB column 'metadata'
    # 'metadata' is a reserved class-level attribute on DeclarativeBase.
    job_metadata: Mapped[dict] = mapped_column(
        "metadata",
        JSONB,
        server_default=text("'{}'::jsonb"),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    listings: Mapped[list["RawListing"]] = relationship(
        "RawListing", back_populates="scrape_run", lazy="select"
    )

    def __repr__(self) -> str:
        return f"<ScrapeRun id={self.id} source={self.source_name!r} status={self.status!r}>"


# ---------------------------------------------------------------------------
# bronze.raw_listings
# ---------------------------------------------------------------------------

LISTING_SOURCE_TYPES = ("licytacja_komornicza", "ogloszenie", "przetarg", "inne")


class RawListing(Base):
    """Raw auction / listing record. Append-only after insert.
    Only `is_processed` and `updated_at` should be mutated post-insert.
    """

    __tablename__ = "raw_listings"
    __table_args__ = (
        UniqueConstraint("dedup_hash", name="uq_raw_listings_dedup"),
        # Composite index for "druga licytacja" detection: same case + same KW.
        # Partial (WHERE both columns NOT NULL) because komornik cases and KWs
        # may not always be extractable. A PostgreSQL partial unique index is
        # created in init SQL — this B-tree index covers lookup performance.
        Index("idx_raw_listings_sygn_kw", "sygnatura_akt", "raw_kw"),
        Index("idx_raw_listings_source_type", "source_type", "created_at"),
        Index(
            "idx_raw_listings_unprocessed",
            "is_processed",
            postgresql_where=text("NOT is_processed"),
        ),
        Index("idx_raw_listings_scrape_run", "scrape_run_id"),
        # GIN trigram indexes are created in init SQL (not replicated here to avoid
        # Alembic/GeoAlchemy2 index-type conflicts on first migrations).
        {"schema": "bronze"},
    )

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    scrape_run_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("bronze.scrape_runs.id", ondelete="RESTRICT"),
        nullable=False,
    )

    # Source
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    source_type: Mapped[str] = mapped_column(
        Text,
        CheckConstraint(
            f"source_type IN {LISTING_SOURCE_TYPES}",
            name="ck_raw_listings_source_type",
        ),
        nullable=False,
    )
    title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    price_zl: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)
    area_m2: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    auction_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    # Raw fields extracted by regex/NLP — may be NULL or wrong, never trust directly
    raw_numer_dzialki: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_obreb: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_gmina: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_powiat: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_wojewodztwo: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_kw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Komornik case number — canonical form: "Km 123/25".
    # Used to detect "druga licytacja" (same property, second auction at lower price).
    # Extracted by regex from obwieszczenie text. NULL when not found.
    sygnatura_akt: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Evidence Chain pointers
    raw_html_ref: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_pdf_ref: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Deduplication — caller must compute SHA-256(source_url + raw_text) before insert
    dedup_hash: Mapped[str] = mapped_column(Text, nullable=False)
    is_processed: Mapped[bool] = mapped_column(
        Boolean, server_default=text("false"), nullable=False
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
    scrape_run: Mapped["ScrapeRun"] = relationship(
        "ScrapeRun", back_populates="listings", lazy="select"
    )
    documents: Mapped[list["RawDocument"]] = relationship(
        "RawDocument", back_populates="listing", lazy="select"
    )

    def __repr__(self) -> str:
        return f"<RawListing id={self.id} source_type={self.source_type!r} processed={self.is_processed}>"


# ---------------------------------------------------------------------------
# bronze.raw_documents
# ---------------------------------------------------------------------------

DOCUMENT_TYPES = ("html", "pdf", "screenshot", "json")


class RawDocument(Base):
    """Immutable archive of original HTML/PDF/screenshot files.
    Each row is one link in the Evidence Chain. Never update, never delete.
    """

    __tablename__ = "raw_documents"
    __table_args__ = (
        UniqueConstraint("content_hash", name="uq_raw_documents_hash"),
        Index("idx_raw_documents_listing", "listing_id"),
        {"schema": "bronze"},
    )

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    )
    listing_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("bronze.raw_listings.id", ondelete="SET NULL"),
        nullable=True,
    )
    document_type: Mapped[str] = mapped_column(
        Text,
        CheckConstraint(
            f"document_type IN {DOCUMENT_TYPES}",
            name="ck_raw_documents_type",
        ),
        nullable=False,
    )
    # GCS path: gs://terrazoning-evidence/{source_type}/{YYYY-MM-DD}/{listing_id}.{ext}
    storage_uri: Mapped[str] = mapped_column(Text, nullable=False)
    file_size_bytes: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    listing: Mapped[Optional["RawListing"]] = relationship(
        "RawListing", back_populates="documents", lazy="select"
    )

    def __repr__(self) -> str:
        return f"<RawDocument id={self.id} type={self.document_type!r} uri={self.storage_uri!r}>"
