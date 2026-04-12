"""Service helpers for parcel quarantine and manual analyst overrides."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import delete, exists, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.bronze import RawListing
from app.models.gold import DeltaResult, InvestmentLead, PlanningZone
from app.models.silver import Dzialka, ListingParcel
from app.schemas.quarantine_parcels import (
    QuarantineParcelFeature,
    QuarantineParcelFeatureCollection,
    QuarantineParcelProperties,
)
from app.services.delta_engine import (
    DeltaEngine,
    DeltaRow,
    _compute_delta_score,
    is_buildable_symbol,
    normalize_symbol,
)

_MANUAL_SOURCE_URL = "manual://quarantine_override"


@dataclass(slots=True)
class ManualOverrideResult:
    dzialka_id: UUID
    lead_id: UUID
    delta_result_id: UUID
    planning_zone_id: UUID
    manual_przeznaczenie: str
    source_url: str | None
    lead_created: bool
    lead_updated: bool


async def list_quarantine_parcels(
    db: AsyncSession,
    *,
    limit: int,
    only_uncovered_gmina: bool,
) -> QuarantineParcelFeatureCollection:
    """Return resolved parcels without a generated investment lead."""
    stmt = text(
        """
        SELECT
            d.id AS dzialka_id,
            d.identyfikator,
            d.teryt_gmina,
            d.area_m2,
            d.current_use,
            d.created_at,
            ST_AsGeoJSON(ST_Transform(d.geom, 4326)) AS geometry,
            (
                SELECT rl.source_url
                FROM silver.listing_parcels lp
                JOIN bronze.raw_listings rl ON rl.id = lp.listing_id
                WHERE lp.dzialka_id = d.id
                ORDER BY lp.created_at DESC
                LIMIT 1
            ) AS source_url,
            (
                SELECT dr.przeznaczenie_after
                FROM gold.delta_results dr
                WHERE dr.dzialka_id = d.id
                ORDER BY dr.coverage_pct DESC, dr.created_at DESC
                LIMIT 1
            ) AS dominant_przeznaczenie,
            EXISTS (
                SELECT 1
                FROM gold.planning_zones pz
                WHERE pz.teryt_gmina = d.teryt_gmina
            ) AS has_planning_zones
        FROM silver.dzialki d
        WHERE d.resolution_status = 'resolved'
          AND NOT EXISTS (
              SELECT 1
              FROM gold.investment_leads il
              WHERE il.dzialka_id = d.id
          )
          AND (
              NOT CAST(:only_uncovered_gmina AS boolean)
              OR NOT EXISTS (
                  SELECT 1
                  FROM gold.planning_zones pz2
                  WHERE pz2.teryt_gmina = d.teryt_gmina
              )
          )
        ORDER BY d.created_at DESC
        LIMIT :limit
        """
    )
    rows = (await db.execute(stmt, {
        "limit": limit,
        "only_uncovered_gmina": only_uncovered_gmina,
    })).mappings().all()

    count_stmt = text(
        """
        SELECT COUNT(*)
        FROM silver.dzialki d
        WHERE d.resolution_status = 'resolved'
          AND NOT EXISTS (
              SELECT 1
              FROM gold.investment_leads il
              WHERE il.dzialka_id = d.id
          )
          AND (
              NOT CAST(:only_uncovered_gmina AS boolean)
              OR NOT EXISTS (
                  SELECT 1
                  FROM gold.planning_zones pz
                  WHERE pz.teryt_gmina = d.teryt_gmina
              )
          )
        """
    )
    count = int((await db.execute(
        count_stmt, {"only_uncovered_gmina": only_uncovered_gmina}
    )).scalar_one())

    features: list[QuarantineParcelFeature] = []
    for row in rows:
        has_planning_zones = bool(row["has_planning_zones"])
        reason = (
            "Brak pokrycia MPZP dla gminy"
            if not has_planning_zones
            else "Dzialka bez wygenerowanego leada"
        )
        status = "uncovered_gmina" if not has_planning_zones else "review_required"
        features.append(
            QuarantineParcelFeature(
                geometry=json.loads(row["geometry"]),
                properties=QuarantineParcelProperties(
                    dzialka_id=row["dzialka_id"],
                    identyfikator=row["identyfikator"],
                    teryt_gmina=row["teryt_gmina"],
                    area_m2=float(row["area_m2"]) if row["area_m2"] is not None else None,
                    source_url=row["source_url"],
                    reason=reason,
                    status=status,
                    current_use=row["current_use"],
                    dominant_przeznaczenie=row["dominant_przeznaczenie"],
                    manual_przeznaczenie=None,
                    created_at=row["created_at"],
                ),
            )
        )

    return QuarantineParcelFeatureCollection(features=features, count=count)


async def apply_manual_override(
    db: AsyncSession,
    *,
    dzialka_id: UUID,
    manual_przeznaczenie: str,
) -> ManualOverrideResult:
    """Create a synthetic delta result for analyst-confirmed buildable zoning."""
    manual_przeznaczenie = manual_przeznaczenie.strip()
    normalized_symbol = normalize_symbol(manual_przeznaczenie)
    if not normalized_symbol or not is_buildable_symbol(manual_przeznaczenie):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "manual_przeznaczenie does not map to a buildable zoning symbol. "
                "Use values like MN, U, MW, MN/U, MNU, U/MW."
            ),
        )

    dzialka = await db.get(Dzialka, dzialka_id)
    if dzialka is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dzialka {dzialka_id} not found",
        )

    existing_lead = await db.execute(
        select(InvestmentLead.id).where(InvestmentLead.dzialka_id == dzialka_id)
    )
    if existing_lead.scalar_one_or_none() is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="This parcel already has an investment lead and is not in quarantine.",
        )

    latest_source_stmt = (
        select(ListingParcel.listing_id, RawListing.source_url)
        .join(RawListing, RawListing.id == ListingParcel.listing_id)
        .where(ListingParcel.dzialka_id == dzialka_id)
        .order_by(ListingParcel.created_at.desc())
        .limit(1)
    )
    latest_source_row = (await db.execute(latest_source_stmt)).first()
    source_url = latest_source_row[1] if latest_source_row else None

    # Remove prior manual synthetic rows for this parcel so the newest analyst
    # decision becomes the single source of truth.
    existing_manual_delta_ids = (
        await db.execute(
            select(DeltaResult.id)
            .join(PlanningZone, PlanningZone.id == DeltaResult.planning_zone_id)
            .where(
                DeltaResult.dzialka_id == dzialka_id,
                PlanningZone.source_wfs_url == _MANUAL_SOURCE_URL,
            )
        )
    ).scalars().all()
    if existing_manual_delta_ids:
        await db.execute(delete(DeltaResult).where(DeltaResult.id.in_(existing_manual_delta_ids)))

    zone_stmt = (
        pg_insert(PlanningZone)
        .values(
            plan_type="mpzp",
            plan_name="Manual override (kwarantanna)",
            uchwala_nr="MANUAL_OVERRIDE",
            teryt_gmina=dzialka.teryt_gmina,
            przeznaczenie=manual_przeznaczenie,
            przeznaczenie_opis="Ręczna decyzja analityka z UI kwarantanny",
            geom=dzialka.geom,
            geom_hash=f"manual:{dzialka_id}:{normalized_symbol}",
            source_wfs_url=_MANUAL_SOURCE_URL,
            plan_effective_date=None,
        )
        .on_conflict_do_update(
            index_elements=[
                "source_wfs_url",
                "teryt_gmina",
                "przeznaczenie",
                "geom_hash",
            ],
            set_={
                "plan_name": "Manual override (kwarantanna)",
                "przeznaczenie_opis": "Ręczna decyzja analityka z UI kwarantanny",
                "geom": dzialka.geom,
                "updated_at": func.now(),
            },
        )
        .returning(PlanningZone.id)
    )
    planning_zone_id = (await db.execute(zone_stmt)).scalar_one()

    existing_delta_stmt = select(DeltaResult).where(
        DeltaResult.dzialka_id == dzialka_id,
        DeltaResult.planning_zone_id == planning_zone_id,
    )
    existing_delta = (await db.execute(existing_delta_stmt)).scalar_one_or_none()

    coverage_pct = Decimal("100.00")
    intersection_area = Decimal(str(dzialka.area_m2 or 0))
    delta_score = _compute_delta_score(coverage_pct)

    if existing_delta is None:
        delta_result = DeltaResult(
            dzialka_id=dzialka_id,
            planning_zone_id=planning_zone_id,
            intersection_geom=dzialka.geom,
            intersection_area_m2=intersection_area,
            coverage_pct=coverage_pct,
            przeznaczenie_before=dzialka.current_use,
            przeznaczenie_after=manual_przeznaczenie,
            is_upgrade=True,
            delta_score=delta_score,
            computed_at=datetime.now(timezone.utc),
        )
        db.add(delta_result)
        await db.flush()
    else:
        existing_delta.intersection_geom = dzialka.geom
        existing_delta.intersection_area_m2 = intersection_area
        existing_delta.coverage_pct = coverage_pct
        existing_delta.przeznaczenie_before = dzialka.current_use
        existing_delta.przeznaczenie_after = manual_przeznaczenie
        existing_delta.is_upgrade = True
        existing_delta.delta_score = delta_score
        existing_delta.computed_at = datetime.now(timezone.utc)
        delta_result = existing_delta
        await db.flush()

    engine = DeltaEngine(db)
    before_lead_id = (
        await db.execute(
            select(InvestmentLead.id).where(InvestmentLead.dzialka_id == dzialka_id)
        )
    ).scalar_one_or_none()

    created, updated = await engine._generate_leads(
        [
            DeltaRow(
                dzialka_id=dzialka.id,
                identyfikator=dzialka.identyfikator,
                match_confidence=Decimal(str(dzialka.match_confidence)),
                dzialka_area_m2=Decimal(str(dzialka.area_m2 or 0)),
                teryt_gmina=dzialka.teryt_gmina,
                current_use=None,
                planning_zone_id=planning_zone_id,
                przeznaczenie=manual_przeznaczenie,
                plan_type="mpzp",
                plan_name="Manual override (kwarantanna)",
                coverage_pct=coverage_pct,
                intersection_area_m2=intersection_area,
                intersection_geom=dzialka.geom,
            )
        ],
        dzialka_ids={dzialka_id},
    )

    lead = (
        await db.execute(
            select(InvestmentLead).where(InvestmentLead.dzialka_id == dzialka_id)
        )
    ).scalar_one_or_none()
    if lead is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Manual override saved, but DeltaEngine did not create an investment lead.",
        )

    await db.commit()

    return ManualOverrideResult(
        dzialka_id=dzialka_id,
        lead_id=lead.id,
        delta_result_id=delta_result.id,
        planning_zone_id=planning_zone_id,
        manual_przeznaczenie=manual_przeznaczenie,
        source_url=source_url,
        lead_created=before_lead_id is None and created > 0,
        lead_updated=before_lead_id is not None or updated > 0,
    )
