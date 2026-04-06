"""DeltaEngine — the arbitrage core: Silver → Gold spatial analysis.

This is where TerraZoning's value is created:
  ST_Intersects(silver.dzialki.geom, gold.planning_zones.geom)
  → coverage_pct per zone per parcel
  → delta_score based on land use potential
  → investment_leads for buildable zones covering > 30% of parcel

Architecture Commandments enforced:
  - ALL spatial queries execute in PostGIS, not Python
  - Raw SQL via text() — SQLAlchemy ORM cannot express ST_Intersection efficiently
  - GIST indexes on geom columns make ST_Intersects O(log n), not O(n²)
  - area_m2 comes from the GENERATED column (never recomputed in Python)
  - Slivers (intersection_area < 0.5 m²) are filtered out in SQL
  - EPSG:2180 maintained throughout; no ST_Transform in delta queries

Lead generation logic:
  - A działka qualifies for an investment_leads row when:
      coverage_pct > 30% AND przeznaczenie ∈ _BUILDABLE_PRZEZNACZENIA
  - confidence_score = match_confidence × delta_score (clamped to 1.00)
  - priority:  ≥ 0.90 → 'high'  |  ≥ 0.75 → 'medium'  |  else → 'low'

Evidence chain format appended to InvestmentLead.evidence_chain:
    [
      {"step": "source",   "ref": "<listing_uuid>", "url": "https://..."},
      {"step": "parcel",   "ref": "<dzialka_uuid>", "teryt": "141201_1.0001.123/4"},
      {"step": "delta",    "ref": "<delta_uuid>",   "coverage": 72.5,
                           "przeznaczenie": "MN",   "plan": "MPZP Wola 2022"}
    ]
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.gold import DeltaResult, InvestmentLead
from app.models.silver import Dzialka, ListingParcel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Buildable land use categories (przeznaczenie → investment potential)
# ---------------------------------------------------------------------------

# These symbols indicate that a zone has building/development potential.
# Source: Polish planning law (ustawa o planowaniu i zagospodarowaniu przestrzennym)
_BUILDABLE_PRZEZNACZENIA: frozenset[str] = frozenset({
    # Mieszkaniowe (residential)
    "MN",       # zabudowa mieszkaniowa jednorodzinna
    "MW",       # zabudowa mieszkaniowa wielorodzinna
    "ML",       # zabudowa letniskowa / rekreacyjna
    "MR",       # zabudowa zagrodowa
    # Usługowe (commercial / service)
    "U",        # usługi
    "UC",       # usługi centrotwórcze (retail/office center)
    "UK",       # usługi kultu religijnego
    # Mixed
    "MU",       # mieszkalnictwo z usługami
    "UM",       # usługi i mieszkalnictwo
    "MN/U",     # mieszkaniowo-usługowe
    "U/MN",     # usługowo-mieszkaniowe
    "MNU",      # mieszkaniowo-usługowe (alternative notation)
})

# Minimum coverage threshold to qualify for investment_leads
_LEAD_COVERAGE_THRESHOLD_PCT = Decimal("30.00")

# Delta score by coverage percentage
_DELTA_SCORE_TIERS: list[tuple[Decimal, Decimal]] = [
    (Decimal("70.00"), Decimal("0.95")),   # ≥ 70% → 0.95
    (Decimal("50.00"), Decimal("0.90")),   # ≥ 50% → 0.90
    (Decimal("30.00"), Decimal("0.85")),   # ≥ 30% → 0.85
]
_DELTA_SCORE_DEFAULT = Decimal("0.50")     # below threshold (non-lead zones)

# Sliver threshold — intersections smaller than this are noise
_SLIVER_THRESHOLD_M2 = 0.5


# ---------------------------------------------------------------------------
# Raw SQL — ST_Intersects spatial join
# ---------------------------------------------------------------------------

# Parameterisable SQL template.
# When :dzialka_ids IS NULL, processes ALL resolved parcels without prior delta.
# When :dzialka_ids is a uuid[] cast, restricts to the supplied IDs.
#
# Performance notes:
#   - ST_Intersects uses GIST indexes on both geom columns → index scan
#   - ST_Intersection is computed only on the overlapping pairs (post-filter)
#   - ST_MakeValid on intersection output handles degenerate slivers
#   - ROUND(... / d.area_m2 * 100, 2) uses the GENERATED ALWAYS AS column
_SPATIAL_JOIN_SQL = text(
    """
    WITH
    -- Identify działki to analyse
    target_dzialki AS (
        SELECT d.id, d.identyfikator, d.match_confidence, d.area_m2, d.teryt_gmina
        FROM silver.dzialki d
        WHERE d.resolution_status = 'resolved'
          AND d.area_m2 > 0
          AND (
              CAST(:dzialka_ids AS uuid[]) IS NULL
              OR d.id = ANY(CAST(:dzialka_ids AS uuid[]))
          )
    ),
    -- Spatial join: działki × planning_zones
    intersections AS (
        SELECT
            td.id                                                   AS dzialka_id,
            td.identyfikator,
            td.match_confidence,
            td.area_m2                                             AS dzialka_area_m2,
            td.teryt_gmina,
            pz.id                                                   AS planning_zone_id,
            pz.przeznaczenie,
            pz.plan_type,
            pz.plan_name,
            ST_Area(ST_Intersection(td.geom, pz.geom))             AS intersection_area_m2,
            ST_Multi(
                ST_MakeValid(ST_Intersection(td.geom, pz.geom))
            )                                                       AS intersection_geom
        FROM target_dzialki td
        JOIN silver.dzialki d ON d.id = td.id
        JOIN gold.planning_zones pz ON ST_Intersects(d.geom, pz.geom)
    )
    SELECT
        dzialka_id,
        identyfikator,
        match_confidence,
        dzialka_area_m2,
        teryt_gmina,
        planning_zone_id,
        przeznaczenie,
        plan_type,
        plan_name,
        ROUND(
            intersection_area_m2 / dzialka_area_m2 * 100.0,
            2
        )                                                          AS coverage_pct,
        intersection_area_m2,
        intersection_geom
    FROM intersections
    WHERE intersection_area_m2 > :sliver_threshold
    ORDER BY dzialka_id, coverage_pct DESC
    """
)

# Query to find dzialki without any delta results yet (unanalyzed batch)
_UNANALYZED_DZIALKI_SQL = text(
    """
    SELECT d.id
    FROM silver.dzialki d
    WHERE d.resolution_status = 'resolved'
      AND d.area_m2 > 0
      AND NOT EXISTS (
          SELECT 1 FROM gold.delta_results dr WHERE dr.dzialka_id = d.id
      )
    ORDER BY d.created_at ASC
    LIMIT :batch_size
    """
)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class DeltaRow:
    """One spatial intersection result: działka × planning_zone."""
    dzialka_id: UUID
    identyfikator: str
    match_confidence: Decimal
    dzialka_area_m2: Decimal
    teryt_gmina: str
    planning_zone_id: UUID
    przeznaczenie: str
    plan_type: str
    plan_name: str
    coverage_pct: Decimal
    intersection_area_m2: Decimal
    intersection_geom: Any              # GeoAlchemy2 WKBElement (MULTIPOLYGON 2180)

    @property
    def is_buildable(self) -> bool:
        return self.przeznaczenie in _BUILDABLE_PRZEZNACZENIA

    @property
    def qualifies_for_lead(self) -> bool:
        return self.is_buildable and self.coverage_pct >= _LEAD_COVERAGE_THRESHOLD_PCT


@dataclass
class DeltaReport:
    """Summary of a DeltaEngine.calculate_deltas() run."""
    dzialki_analyzed: int = 0
    delta_results_created: int = 0
    leads_created: int = 0
    leads_updated: int = 0
    duration_s: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def lead_conversion_rate(self) -> float:
        if self.dzialki_analyzed == 0:
            return 0.0
        return round(self.leads_created / self.dzialki_analyzed, 3)


# ---------------------------------------------------------------------------
# DeltaEngine
# ---------------------------------------------------------------------------

class DeltaEngine:
    """Arbitrage engine: runs ST_Intersects and generates investment leads.

    Usage:
        async with AsyncSessionLocal() as db:
            engine = DeltaEngine(db)
            report = await engine.calculate_deltas(batch_size=100)
    """

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def calculate_deltas(
        self,
        dzialka_ids: Optional[list[UUID]] = None,
        batch_size: int = 100,
    ) -> DeltaReport:
        """Calculate spatial deltas for all new (or specified) działki.

        If dzialka_ids is None, fetches the next batch_size unanalyzed
        resolved parcels (FIFO by created_at).

        If dzialka_ids is provided, forces recalculation for those IDs.
        Existing delta_results for those IDs are deleted before recomputing
        to ensure idempotency.
        """
        t_start = asyncio.get_event_loop().time()
        report = DeltaReport()

        if dzialka_ids is None:
            target_ids = await self._fetch_unanalyzed_dzialki(batch_size)
        else:
            target_ids = list(dzialka_ids)
            # Force recalc: clear existing delta results for these IDs
            await self._delete_delta_results(target_ids)

        if not target_ids:
            logger.info("[DeltaEngine] No unanalyzed działki to process")
            return report

        logger.info("[DeltaEngine] Calculating deltas for %d działki", len(target_ids))

        # Execute ST_Intersects spatial join
        delta_rows = await self._run_spatial_join(target_ids)
        logger.info("[DeltaEngine] Spatial join produced %d intersection rows", len(delta_rows))

        # Save all delta_results
        saved_ids = await self._save_delta_results(delta_rows)
        report.delta_results_created = len(saved_ids)

        # Generate investment_leads for qualifying buildable zones
        report.leads_created, report.leads_updated = await self._generate_leads(
            delta_rows, dzialka_ids=set(target_ids)
        )

        report.dzialki_analyzed = len(target_ids)
        report.duration_s = round(asyncio.get_event_loop().time() - t_start, 2)

        logger.info(
            "[DeltaEngine] Done — analyzed=%d deltas=%d leads_new=%d leads_updated=%d "
            "conversion=%.1f%% in %.1fs",
            report.dzialki_analyzed,
            report.delta_results_created,
            report.leads_created,
            report.leads_updated,
            report.lead_conversion_rate * 100,
            report.duration_s,
        )
        return report

    # ------------------------------------------------------------------
    # Spatial join
    # ------------------------------------------------------------------

    async def _run_spatial_join(
        self,
        target_ids: list[UUID],
    ) -> list[DeltaRow]:
        """Execute the ST_Intersects raw SQL query, return DeltaRow list."""

        # asyncpg requires uuid[] as an array of UUID objects (not strings)
        # Passing None means "process all" in the SQL template; we always pass IDs here.
        ids_param = [str(id_) for id_ in target_ids]

        # Build parameter string for uuid[] cast
        # asyncpg handles Python list → PostgreSQL ARRAY correctly when cast explicitly
        stmt = _SPATIAL_JOIN_SQL.bindparams(
            dzialka_ids=ids_param,          # cast to uuid[] in SQL
            sliver_threshold=_SLIVER_THRESHOLD_M2,
        )

        try:
            result = await self.db.execute(stmt)
        except Exception as exc:
            logger.error("[DeltaEngine] Spatial join SQL failed: %s", exc, exc_info=True)
            raise

        rows: list[DeltaRow] = []
        for row in result.mappings():
            rows.append(DeltaRow(
                dzialka_id=row["dzialka_id"],
                identyfikator=row["identyfikator"],
                match_confidence=Decimal(str(row["match_confidence"])),
                dzialka_area_m2=Decimal(str(row["dzialka_area_m2"])),
                teryt_gmina=row["teryt_gmina"],
                planning_zone_id=row["planning_zone_id"],
                przeznaczenie=row["przeznaczenie"],
                plan_type=row["plan_type"],
                plan_name=row["plan_name"],
                coverage_pct=Decimal(str(row["coverage_pct"])),
                intersection_area_m2=Decimal(str(row["intersection_area_m2"])),
                intersection_geom=row["intersection_geom"],
            ))
        return rows

    # ------------------------------------------------------------------
    # Persist delta_results
    # ------------------------------------------------------------------

    async def _save_delta_results(
        self,
        rows: list[DeltaRow],
    ) -> list[UUID]:
        """Batch-insert delta_results rows. Returns list of inserted IDs.

        ON CONFLICT DO NOTHING — if the same (dzialka_id, planning_zone_id) pair
        already exists (from a prior run without force-recalc), we skip it.
        The delta_results table accumulates rows; use calculate_deltas(dzialka_ids=...)
        to force a clean recompute.
        """
        if not rows:
            return []

        inserted_ids: list[UUID] = []

        for row in rows:
            delta_score = _compute_delta_score(row.coverage_pct)
            is_upgrade = row.is_buildable

            stmt = (
                pg_insert(DeltaResult)
                .values(
                    dzialka_id=row.dzialka_id,
                    planning_zone_id=row.planning_zone_id,
                    intersection_geom=row.intersection_geom,
                    intersection_area_m2=float(row.intersection_area_m2),
                    coverage_pct=float(row.coverage_pct),
                    przeznaczenie_after=row.przeznaczenie,
                    is_upgrade=is_upgrade,
                    delta_score=float(delta_score),
                    computed_at=datetime.now(timezone.utc),
                )
                .on_conflict_do_nothing()
                .returning(DeltaResult.id)
            )
            result = await self.db.execute(stmt)
            row_id = result.scalar_one_or_none()
            if row_id is not None:
                inserted_ids.append(row_id)

        await self.db.commit()
        logger.info(
            "[DeltaEngine] Saved %d delta_results (%d already existed)",
            len(inserted_ids), len(rows) - len(inserted_ids),
        )
        return inserted_ids

    # ------------------------------------------------------------------
    # Generate investment_leads
    # ------------------------------------------------------------------

    async def _generate_leads(
        self,
        delta_rows: list[DeltaRow],
        dzialka_ids: set[UUID],
    ) -> tuple[int, int]:
        """Create or update investment_leads for qualifying działki.

        A działka qualifies when at least one of its delta rows has:
          - przeznaczenie ∈ _BUILDABLE_PRZEZNACZENIA
          - coverage_pct ≥ 30%

        For each qualifying działka, we:
          1. Select the highest-coverage buildable zone as the dominant przeznaczenie
          2. Aggregate a combined confidence_score
          3. Fetch the listing_id (from silver.listing_parcels) for the evidence chain
          4. Upsert into gold.investment_leads
        """
        created = 0
        updated = 0

        # Group delta rows by dzialka_id; keep only qualifying ones
        qualifying: dict[UUID, list[DeltaRow]] = {}
        for row in delta_rows:
            if row.qualifies_for_lead:
                qualifying.setdefault(row.dzialka_id, []).append(row)

        for dzialka_id, q_rows in qualifying.items():
            # Best row = highest coverage among buildable zones
            best = max(q_rows, key=lambda r: r.coverage_pct)
            delta_score = _compute_delta_score(best.coverage_pct)
            # confidence_score = Silver match_confidence × delta_score, capped at 1.00
            confidence = min(best.match_confidence * delta_score, Decimal("1.00"))
            priority = _priority_for_score(confidence)

            # Max coverage among all buildable zones for this dzialka
            max_coverage = max(r.coverage_pct for r in q_rows)

            # Fetch listing_id via silver.listing_parcels (most recent match)
            listing_id = await self._fetch_listing_id(dzialka_id)

            # Build evidence chain step
            evidence_entry: dict[str, Any] = {
                "step": "delta",
                "ref": str(best.planning_zone_id),
                "coverage": float(best.coverage_pct),
                "przeznaczenie": best.przeznaczenie,
                "plan": best.plan_name,
                "plan_type": best.plan_type,
                "computed_at": datetime.now(timezone.utc).isoformat(),
            }

            was_updated = await self._upsert_lead(
                dzialka_id=dzialka_id,
                listing_id=listing_id,
                confidence_score=confidence,
                priority=priority,
                max_coverage_pct=max_coverage,
                dominant_przeznaczenie=best.przeznaczenie,
                evidence_entry=evidence_entry,
            )
            if was_updated:
                updated += 1
            else:
                created += 1

        await self.db.commit()
        logger.info(
            "[DeltaEngine] Investment leads: %d created, %d updated "
            "(%d/%d dzialki qualified)",
            created, updated, len(qualifying), len(dzialka_ids),
        )
        return created, updated

    async def _upsert_lead(
        self,
        *,
        dzialka_id: UUID,
        listing_id: Optional[UUID],
        confidence_score: Decimal,
        priority: str,
        max_coverage_pct: Decimal,
        dominant_przeznaczenie: str,
        evidence_entry: dict[str, Any],
    ) -> bool:
        """Insert or update an investment_leads row. Returns True if update, False if insert."""
        import json

        # Check if a lead already exists
        existing_q = await self.db.execute(
            select(InvestmentLead).where(InvestmentLead.dzialka_id == dzialka_id)
        )
        existing = existing_q.scalar_one_or_none()

        if existing is not None:
            # Update only if new confidence is higher
            if confidence_score > existing.confidence_score:
                existing.confidence_score = confidence_score
                existing.priority = priority
                existing.max_coverage_pct = max_coverage_pct
                existing.dominant_przeznaczenie = dominant_przeznaczenie
                existing.updated_at = datetime.now(timezone.utc)
                # Append new delta evidence to chain
                chain = list(existing.evidence_chain or [])
                chain.append(evidence_entry)
                existing.evidence_chain = chain
                logger.debug(
                    "[DeltaEngine] Updated lead dzialka=%s score=%.2f priority=%s",
                    dzialka_id, confidence_score, priority,
                )
            else:
                logger.debug(
                    "[DeltaEngine] Skipped lead update dzialka=%s (existing score=%.2f ≥ new=%.2f)",
                    dzialka_id, existing.confidence_score, confidence_score,
                )
            return True

        # New lead
        evidence_chain = [evidence_entry]
        # Add parcel step to chain
        parcel_step: dict[str, Any] = {
            "step": "parcel",
            "ref": str(dzialka_id),
        }
        if listing_id:
            parcel_step["listing_ref"] = str(listing_id)

        lead = InvestmentLead(
            dzialka_id=dzialka_id,
            listing_id=listing_id,
            confidence_score=confidence_score,
            priority=priority,
            max_coverage_pct=max_coverage_pct,
            dominant_przeznaczenie=dominant_przeznaczenie,
            evidence_chain=[parcel_step, evidence_entry],
            status="new",
        )
        self.db.add(lead)
        logger.info(
            "[DeltaEngine] New lead: dzialka=%s score=%.2f priority=%s coverage=%.1f%% "
            "przeznaczenie=%s",
            dzialka_id, confidence_score, priority, max_coverage_pct, dominant_przeznaczenie,
        )
        return False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _fetch_unanalyzed_dzialki(self, batch_size: int) -> list[UUID]:
        """Fetch IDs of resolved parcels that have no delta_results yet."""
        result = await self.db.execute(
            _UNANALYZED_DZIALKI_SQL.bindparams(batch_size=batch_size)
        )
        return [row[0] for row in result.fetchall()]

    async def _delete_delta_results(self, dzialka_ids: list[UUID]) -> None:
        """Delete existing delta_results for forced recalculation."""
        from sqlalchemy import delete

        await self.db.execute(
            delete(DeltaResult).where(DeltaResult.dzialka_id.in_(dzialka_ids))
        )
        await self.db.commit()
        logger.debug(
            "[DeltaEngine] Deleted existing delta_results for %d dzialki (force recalc)",
            len(dzialka_ids),
        )

    async def _fetch_listing_id(self, dzialka_id: UUID) -> Optional[UUID]:
        """Find the most recent listing linked to this dzialka via silver.listing_parcels."""
        result = await self.db.execute(
            select(ListingParcel.listing_id)
            .where(ListingParcel.dzialka_id == dzialka_id)
            .order_by(ListingParcel.created_at.desc())
            .limit(1)
        )
        row = result.scalar_one_or_none()
        return row


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

def _compute_delta_score(coverage_pct: Decimal) -> Decimal:
    """Map coverage percentage to a delta_score (0.00–1.00)."""
    for threshold, score in _DELTA_SCORE_TIERS:
        if coverage_pct >= threshold:
            return score
    return _DELTA_SCORE_DEFAULT


def _priority_for_score(confidence: Decimal) -> str:
    """Map confidence_score to a priority string."""
    if confidence >= Decimal("0.90"):
        return "high"
    if confidence >= Decimal("0.75"):
        return "medium"
    return "low"


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------

async def run_delta_engine(
    batch_size: int = 100,
    dzialka_ids: Optional[list[UUID]] = None,
) -> DeltaReport:
    """Run one delta analysis cycle — usable from Cloud Run Jobs or CLI."""
    from app.core.database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        engine = DeltaEngine(db)
        return await engine.calculate_deltas(
            dzialka_ids=dzialka_ids,
            batch_size=batch_size,
        )


if __name__ == "__main__":
    import asyncio as _asyncio
    import logging as _logging

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    report = _asyncio.run(run_delta_engine(batch_size=50))

    print(f"\n{'='*60}")
    print("DELTA ENGINE COMPLETE")
    print(f"{'='*60}")
    print(f"  Działki analyzed   : {report.dzialki_analyzed}")
    print(f"  Delta results      : {report.delta_results_created}")
    print(f"  Leads created      : {report.leads_created}")
    print(f"  Leads updated      : {report.leads_updated}")
    print(f"  Conversion rate    : {report.lead_conversion_rate:.1%}")
    print(f"  Duration           : {report.duration_s}s")
    if report.errors:
        print("\n  ERRORS:")
        for e in report.errors:
            print(f"    - {e}")
    print(f"{'='*60}")
