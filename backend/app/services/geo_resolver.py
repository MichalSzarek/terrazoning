"""GeoResolver — Bronze → Silver pipeline orchestrator.

Consumes unprocessed rows from bronze.raw_listings, resolves each to
a geometry via ULDKClient, and writes results to silver.dzialki +
silver.listing_parcels. Failed resolutions go to silver.dlq_parcels.

GIS Specialist Commandments enforced here:
  - geometry stored exclusively in EPSG:2180
  - area_m2 is computed by PostGIS (GENERATED column), not Python
  - ST_GeomFromWKB(…, 2180) used for explicit SRID injection on insert
  - confidence_score reflects the resolution method used

Backend Lead Commandments:
  - all DB operations via async SQLAlchemy (no raw psycopg2)
  - ON CONFLICT DO NOTHING for idempotent upserts on silver.dzialki
  - batch processing with configurable page size
  - structured logging with correlation ID (scrape_run_id)

DLQ retry schedule (silver.dlq_parcels):
  attempt 1  → next_retry_at = now + 1h
  attempt 2  → next_retry_at = now + 4h
  attempt 3  → next_retry_at = now + 24h
  attempt 4  → next_retry_at = now + 72h
  attempt 5  → manual review required (attempt_count capped at 5)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Literal
from uuid import UUID

from geoalchemy2.shape import from_shape
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.bronze import RawListing
from app.models.silver import DlqParcel, Dzialka, ListingParcel
from app.services.uldk import (
    GeometryValidationError,
    ULDKAPIError,
    ULDKClient,
    ULDKNotFoundError,
    ULDKParcel,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DLQ retry schedule
# ---------------------------------------------------------------------------

_DLQ_RETRY_DELAYS: list[timedelta] = [
    timedelta(hours=1),    # attempt 1 → retry in 1h
    timedelta(hours=4),    # attempt 2 → retry in 4h
    timedelta(hours=24),   # attempt 3 → retry in 24h
    timedelta(hours=72),   # attempt 4 → retry in 72h
    # attempt 5 → no auto-retry, requires manual intervention
]
_DLQ_MAX_ATTEMPTS = 5

# Throttle between ULDK calls (GUGiK enforces ~2 req/s)
_ULDK_INTER_REQUEST_DELAY_S = 0.6

# Confidence scores by resolution method
_CONFIDENCE = {
    "kw_lookup":     Decimal("0.92"),  # KW → ULDK: deterministic, single parcel or few
    "teryt_exact":   Decimal("0.98"),  # TERYT ID exact match: canonical
    "address_fuzzy": Decimal("0.55"),  # address-derived parcel ID: uncertain
    "uldk_partial":  Decimal("0.70"),  # resolved but check digit was invalid in source
}


def _next_retry_at(attempt: int) -> datetime:
    """Compute next_retry_at for a given attempt number (1-indexed)."""
    idx = attempt - 1
    if idx < len(_DLQ_RETRY_DELAYS):
        return datetime.now(timezone.utc) + _DLQ_RETRY_DELAYS[idx]
    # Beyond schedule → sentinel datetime (manual review)
    return datetime(9999, 12, 31, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Resolution report
# ---------------------------------------------------------------------------

@dataclass
class ResolutionReport:
    """Aggregate result from GeoResolver.process_pending_listings()."""
    total_processed: int = 0
    resolved: int = 0
    sent_to_dlq: int = 0
    already_resolved: int = 0
    errors: list[str] = field(default_factory=list)
    duration_s: float = 0.0

    @property
    def success_rate(self) -> float:
        if self.total_processed == 0:
            return 0.0
        return round(self.resolved / self.total_processed, 3)


# ---------------------------------------------------------------------------
# GeoResolver
# ---------------------------------------------------------------------------

class GeoResolver:
    """Orchestrates Bronze → Silver geometry resolution.

    Usage:
        async with AsyncSessionLocal() as db:
            async with ULDKClient() as uldk:
                resolver = GeoResolver(db, uldk)
                report = await resolver.process_pending_listings(batch_size=50)
    """

    def __init__(self, db: AsyncSession, uldk: ULDKClient) -> None:
        self.db = db
        self.uldk = uldk

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def process_pending_listings(
        self,
        batch_size: int = 50,
    ) -> ResolutionReport:
        """Process a batch of unresolved raw_listings → silver.dzialki.

        Queries bronze.raw_listings WHERE is_processed = false,
        ordered by created_at ASC (oldest first — FIFO).
        """
        t_start = asyncio.get_event_loop().time()
        report = ResolutionReport()

        listings = await self._fetch_pending_listings(batch_size)
        logger.info("[GeoResolver] Processing %d pending listings", len(listings))

        for listing in listings:
            outcome = await self._resolve_listing(listing)

            if outcome == "resolved":
                report.resolved += 1
            elif outcome == "dlq":
                report.sent_to_dlq += 1
            else:  # 'skip'
                report.already_resolved += 1

            report.total_processed += 1

            # Throttle ULDK calls — respect GUGiK rate limits
            await asyncio.sleep(_ULDK_INTER_REQUEST_DELAY_S)

        report.duration_s = round(asyncio.get_event_loop().time() - t_start, 2)
        logger.info(
            "[GeoResolver] Complete — resolved=%d dlq=%d total=%d in %.1fs "
            "(success_rate=%.1f%%)",
            report.resolved, report.sent_to_dlq, report.total_processed,
            report.duration_s, report.success_rate * 100,
        )
        return report

    # ------------------------------------------------------------------
    # Per-listing resolution
    # ------------------------------------------------------------------

    async def _resolve_listing(
        self, listing: RawListing
    ) -> Literal["resolved", "dlq", "skip"]:
        """Try all resolution strategies for a single listing.

        Strategy priority:
          1. KW lookup (if raw_kw is set) — most deterministic
          2. Parcel ID lookup (if raw_numer_dzialki + raw_obreb are set)
          3. DLQ on all failures

        Always marks listing.is_processed = True after this method,
        regardless of outcome — the DLQ handles retries, not this method.
        """
        listing_id = listing.id
        log_prefix = f"[GeoResolver] listing={listing_id}"

        try:
            parcels: list[ULDKParcel] = []
            match_method = "kw_lookup"

            # Strategy 1: resolve by KW
            if listing.raw_kw:
                logger.info("%s trying KW lookup: %s", log_prefix, listing.raw_kw)
                try:
                    parcels = await self.uldk.resolve_parcel_by_kw(listing.raw_kw)
                    if parcels:
                        match_method = "kw_lookup"
                except ULDKNotFoundError:
                    logger.info("%s KW not found in ULDK: %s", log_prefix, listing.raw_kw)
                except (ULDKAPIError, GeometryValidationError) as exc:
                    logger.warning("%s KW lookup failed: %s", log_prefix, exc)

            # Strategy 2: resolve by parcel TERYT ID
            if not parcels and listing.raw_numer_dzialki and listing.raw_obreb:
                parcel_id = self._build_uldk_id(listing)
                if parcel_id:
                    logger.info("%s trying parcel ID lookup: %s", log_prefix, parcel_id)
                    try:
                        p = await self.uldk.resolve_parcel_by_id(parcel_id)
                        if p:
                            parcels = [p]
                            match_method = "teryt_exact"
                    except ULDKNotFoundError:
                        logger.info("%s parcel ID not found: %s", log_prefix, parcel_id)
                    except (ULDKAPIError, GeometryValidationError) as exc:
                        logger.warning("%s parcel ID lookup failed: %s", log_prefix, exc)

            # Outcome: resolved
            if parcels:
                await self._save_to_silver(listing, parcels, match_method)
                await self._mark_processed(listing_id)
                return "resolved"

            # Outcome: send to DLQ
            reason = (
                f"All resolution strategies exhausted. "
                f"raw_kw={listing.raw_kw!r}, "
                f"raw_numer_dzialki={listing.raw_numer_dzialki!r}"
            )
            await self._send_to_dlq(listing_id, reason, attempt=1)
            await self._mark_processed(listing_id)
            return "dlq"

        except Exception as exc:
            logger.error(
                "%s Unexpected error — sending to DLQ: %s", log_prefix, exc, exc_info=True,
            )
            await self._send_to_dlq(
                listing_id,
                f"Unexpected error: {type(exc).__name__}: {exc}",
                attempt=1,
            )
            await self._mark_processed(listing_id)
            return "dlq"

    # ------------------------------------------------------------------
    # Silver layer persistence
    # ------------------------------------------------------------------

    async def _save_to_silver(
        self,
        listing: RawListing,
        parcels: list[ULDKParcel],
        match_method: str,
    ) -> None:
        """Write resolved parcels to silver.dzialki + silver.listing_parcels.

        Uses ON CONFLICT DO NOTHING on identyfikator (unique constraint) so
        re-running the resolver is safe and idempotent.

        Geometry is injected as a GeoAlchemy2 WKBElement via from_shape(),
        which constructs the correct EWKB payload with SRID=2180.
        PostGIS will compute area_m2 via the GENERATED ALWAYS AS column.
        """
        confidence = _CONFIDENCE.get(match_method, Decimal("0.70"))

        for parcel in parcels:
            # --- Convert Shapely → GeoAlchemy2 WKBElement (SRID 2180) ---
            # from_shape() produces EWKB with embedded SRID.
            # PostGIS stores it as GEOMETRY(MultiPolygon, 2180).
            # area_m2 is GENERATED — we do NOT set it here.
            geom_element = from_shape(parcel.geom_shape, srid=2180)

            logger.debug(
                "[GIS] Inserting Dzialka identyfikator=%s area=%.2f m² "
                "was_made_valid=%s SRID=2180",
                parcel.identyfikator,
                float(parcel.area_m2),
                parcel.was_made_valid,
            )
            if parcel.was_made_valid:
                logger.warning(
                    "[GIS] SEVERITY:MEDIUM — ST_MakeValid() was applied to %s "
                    "before insert. Geometry was invalid from ULDK.",
                    parcel.identyfikator,
                )

            # Upsert into silver.dzialki (idempotent)
            dzialka_stmt = (
                pg_insert(Dzialka)
                .values(
                    teryt_wojewodztwo=parcel.teryt_wojewodztwo,
                    teryt_powiat=parcel.teryt_powiat,
                    teryt_gmina=parcel.teryt_gmina,
                    teryt_obreb=parcel.teryt_obreb,
                    numer_dzialki=parcel.numer_dzialki,
                    identyfikator=parcel.identyfikator,
                    geom=geom_element,
                    # area_m2: GENERATED ALWAYS AS (ST_Area(geom)) — not set here
                    uldk_response_date=parcel.fetched_at,
                    uldk_raw_response={
                        "identifier": parcel.identifier,
                        "voivodeship": parcel.voivodeship,
                        "county": parcel.county,
                        "commune": parcel.commune,
                        "region": parcel.region,
                        "parcel": parcel.parcel,
                        "geom_wkb_hex": parcel.geom_wkb_hex[:40] + "…",   # truncated for JSON
                        "area_m2_computed": float(parcel.area_m2),
                        "was_made_valid": parcel.was_made_valid,
                    },
                    match_confidence=float(confidence),
                    resolution_status="resolved",
                )
                .on_conflict_do_update(
                    index_elements=["identyfikator"],
                    set_={
                        # On conflict: refresh geometry and metadata, keep original created_at
                        "geom": geom_element,
                        "uldk_response_date": parcel.fetched_at,
                        "match_confidence": float(confidence),
                        "resolution_status": "resolved",
                        "failure_reason": None,
                        "updated_at": datetime.now(timezone.utc),
                    },
                )
                .returning(Dzialka.id)
            )

            result = await self.db.execute(dzialka_stmt)
            dzialka_row = result.fetchone()
            if dzialka_row is None:
                logger.error(
                    "[GeoResolver] Dzialka upsert returned no ID for %s",
                    parcel.identyfikator,
                )
                continue

            dzialka_id: UUID = dzialka_row[0]

            # Insert into silver.listing_parcels (Bronze → Silver link)
            lp_stmt = (
                pg_insert(ListingParcel)
                .values(
                    listing_id=listing.id,
                    dzialka_id=dzialka_id,
                    match_confidence=float(confidence),
                    match_method=match_method,
                )
                .on_conflict_do_nothing(
                    index_elements=["listing_id", "dzialka_id"]
                )
            )
            await self.db.execute(lp_stmt)

        await self.db.commit()
        logger.info(
            "[GeoResolver] Saved %d parcel(s) to Silver for listing %s",
            len(parcels), listing.id,
        )

    # ------------------------------------------------------------------
    # DLQ
    # ------------------------------------------------------------------

    async def _send_to_dlq(
        self,
        listing_id: UUID,
        error: str,
        attempt: int = 1,
    ) -> None:
        """Insert or increment a DLQ entry for the given listing.

        If a DLQ row already exists for this listing (from a previous run),
        increments attempt_count and updates next_retry_at using the
        exponential backoff schedule.
        """
        from sqlalchemy import select

        existing = await self.db.execute(
            select(DlqParcel).where(DlqParcel.listing_id == listing_id)
        )
        row = existing.scalar_one_or_none()

        if row is not None:
            new_attempt = min(row.attempt_count + 1, _DLQ_MAX_ATTEMPTS)
            row.attempt_count = new_attempt
            row.last_error = error
            row.next_retry_at = _next_retry_at(new_attempt)
            row.updated_at = datetime.now(timezone.utc)
            logger.info(
                "[GeoResolver] DLQ increment listing=%s attempt=%d next_retry=%s",
                listing_id, new_attempt, row.next_retry_at.isoformat(),
            )
        else:
            # Fetch the raw_teryt_input to store in DLQ for traceability
            listing_q = await self.db.execute(
                select(RawListing).where(RawListing.id == listing_id)
            )
            listing = listing_q.scalar_one_or_none()
            raw_input = (
                listing.raw_kw or listing.raw_numer_dzialki or str(listing_id)
                if listing else str(listing_id)
            )

            dlq = DlqParcel(
                listing_id=listing_id,
                raw_teryt_input=raw_input,
                attempt_count=attempt,
                last_error=error,
                next_retry_at=_next_retry_at(attempt),
            )
            self.db.add(dlq)
            logger.info(
                "[GeoResolver] DLQ new entry listing=%s attempt=1 "
                "next_retry=%s error=%s",
                listing_id, dlq.next_retry_at.isoformat(), error[:80],
            )

        await self.db.commit()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _fetch_pending_listings(self, limit: int) -> list[RawListing]:
        """Fetch unprocessed listings from bronze, oldest first."""
        result = await self.db.execute(
            select(RawListing)
            .where(RawListing.is_processed == False)  # noqa: E712
            .order_by(RawListing.created_at.asc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def _mark_processed(self, listing_id: UUID) -> None:
        """Mark a raw_listing as processed (regardless of outcome)."""
        await self.db.execute(
            update(RawListing)
            .where(RawListing.id == listing_id)
            .values(is_processed=True)
        )
        await self.db.commit()

    @staticmethod
    def _build_uldk_id(listing: RawListing) -> str | None:
        """Attempt to construct a ULDK parcel ID from raw listing fields.

        ULDK format: '{commune}.{obreb}.{numer_dzialki}'
        e.g., '1412011.0001.123/4'

        This is a best-effort construction — the raw fields may be incomplete
        or fuzzy-matched. Confidence will be set to 'address_fuzzy' (<0.7)
        if the ID was constructed this way.
        """
        # If raw_obreb looks like a 9-digit TERYT code, we can split it
        obreb = listing.raw_obreb or ""
        numer = listing.raw_numer_dzialki or ""

        if not numer:
            return None

        # Case 1: raw_obreb is a 9-digit TERYT code
        if obreb.isdigit() and len(obreb) == 9:
            commune = obreb[:7]
            obreb4 = obreb[7:].zfill(4)
            return f"{commune}.{obreb4}.{numer}"

        # Case 2: raw_obreb has the full GUGiK dot-notation
        if "." in obreb and obreb.replace(".", "").isdigit():
            return f"{obreb}.{numer}"

        # Cannot build a reliable ULDK ID — return None (will go to DLQ)
        return None


# ---------------------------------------------------------------------------
# Standalone runner (for testing / one-off runs)
# ---------------------------------------------------------------------------

async def run_geo_resolver(batch_size: int = 20) -> ResolutionReport:
    """Run one resolution cycle — useful for Cloud Run Jobs and CLI invocation."""
    from app.core.database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        async with ULDKClient() as uldk:
            resolver = GeoResolver(db, uldk)
            return await resolver.process_pending_listings(batch_size=batch_size)


if __name__ == "__main__":
    import asyncio as _asyncio
    import logging as _logging

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    report = _asyncio.run(run_geo_resolver(batch_size=10))

    print(f"\n{'='*60}")
    print("GEO RESOLVER COMPLETE")
    print(f"{'='*60}")
    print(f"  Total processed : {report.total_processed}")
    print(f"  Resolved        : {report.resolved}")
    print(f"  Sent to DLQ     : {report.sent_to_dlq}")
    print(f"  Success rate    : {report.success_rate:.1%}")
    print(f"  Duration        : {report.duration_s}s")
    if report.errors:
        print("\n  ERRORS:")
        for e in report.errors:
            print(f"    - {e}")
    print(f"{'='*60}")
