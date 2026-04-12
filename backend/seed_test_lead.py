"""Seed one synthetic Gliwice lead so the dashboard can be verified end-to-end.

Usage:
    uv run python seed_test_lead.py
    uv run python seed_test_lead.py --verbose
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select, text

from app.core.database import AsyncSessionLocal
from app.models.bronze import RawListing
from app.models.gold import InvestmentLead

logger = logging.getLogger("seed_test_lead")

TEST_IDENTYFIKATOR = "246601101.9999/1"
TEST_TERYT_GMINA = "2466011"
TEST_TERYT_OBREB = "246601101"
TEST_NUMER_DZIALKI = "9999/1"
TEST_WKT_4326 = (
    "POLYGON((18.6520 50.2945, 18.6566 50.2945, "
    "18.6566 50.2978, 18.6520 50.2978, 18.6520 50.2945))"
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed a synthetic Gliwice investment lead")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    return parser.parse_args()


async def _find_gliwice_listing_id(db) -> UUID | None:
    stmt = (
        select(RawListing.id)
        .where(
            (RawListing.raw_obreb.ilike("%Gliwice%"))
            | (RawListing.raw_gmina.ilike("%Gliwice%"))
        )
        .order_by(RawListing.created_at.desc())
        .limit(1)
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def _upsert_test_dzialka(db) -> UUID:
    payload = {
        "seed": "seed_test_lead.py",
        "kind": "synthetic_gliwice_parcel",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    stmt = text(
        """
        INSERT INTO silver.dzialki (
            teryt_wojewodztwo,
            teryt_powiat,
            teryt_gmina,
            teryt_obreb,
            numer_dzialki,
            identyfikator,
            current_use,
            geom,
            uldk_response_date,
            uldk_raw_response,
            match_confidence,
            resolution_status,
            failure_reason
        )
        VALUES (
            '24',
            '2466',
            :teryt_gmina,
            :teryt_obreb,
            :numer_dzialki,
            :identyfikator,
            NULL,
            ST_Transform(
                ST_SetSRID(ST_Multi(ST_GeomFromText(:wkt_4326)), 4326),
                2180
            ),
            now(),
            CAST(:uldk_raw_response AS jsonb),
            0.99,
            'resolved',
            NULL
        )
        ON CONFLICT (identyfikator) DO UPDATE
        SET
            geom = EXCLUDED.geom,
            current_use = EXCLUDED.current_use,
            uldk_response_date = EXCLUDED.uldk_response_date,
            uldk_raw_response = EXCLUDED.uldk_raw_response,
            match_confidence = EXCLUDED.match_confidence,
            resolution_status = 'resolved',
            failure_reason = NULL,
            updated_at = now()
        RETURNING id
        """
    )
    result = await db.execute(
        stmt,
        {
            "teryt_gmina": TEST_TERYT_GMINA,
            "teryt_obreb": TEST_TERYT_OBREB,
            "numer_dzialki": TEST_NUMER_DZIALKI,
            "identyfikator": TEST_IDENTYFIKATOR,
            "wkt_4326": TEST_WKT_4326,
            "uldk_raw_response": json.dumps(payload),
        },
    )
    dzialka_id = result.scalar_one()
    await db.flush()
    return dzialka_id


async def _upsert_test_lead(db, dzialka_id: UUID, listing_id: UUID | None) -> InvestmentLead:
    evidence_chain = [
        {
            "step": "seed",
            "ref": "seed_test_lead.py",
            "label": "Synthetic Gliwice lead for dashboard verification",
        },
        {
            "step": "parcel",
            "ref": str(dzialka_id),
            "teryt": TEST_IDENTYFIKATOR,
            "gmina": TEST_TERYT_GMINA,
        },
        {
            "step": "delta",
            "ref": "synthetic-mn-u-zone",
            "coverage": 91.0,
            "przeznaczenie": "MN/U",
            "plan": "TEST GLIWICE MPZP",
            "note": "Synthetic seed for frontend smoke test",
        },
    ]
    if listing_id:
        evidence_chain.insert(
            1,
            {
                "step": "source",
                "ref": str(listing_id),
                "note": "Linked to an existing Gliwice bronze listing for auditability",
            },
        )

    existing = await db.execute(
        select(InvestmentLead)
        .where(InvestmentLead.dzialka_id == dzialka_id)
        .order_by(InvestmentLead.created_at.desc())
        .limit(1)
    )
    lead = existing.scalar_one_or_none()

    if lead is None:
        lead = InvestmentLead(
            dzialka_id=dzialka_id,
            listing_id=listing_id,
            confidence_score=Decimal("0.99"),
            priority="high",
            max_coverage_pct=Decimal("91.00"),
            dominant_przeznaczenie="MN/U",
            price_per_m2_zl=Decimal("140.00"),
            estimated_value_uplift_pct=Decimal("65.00"),
            evidence_chain=evidence_chain,
            status="new",
            notes="Synthetic Gliwice lead seeded for frontend smoke test.",
        )
        db.add(lead)
        await db.flush()
        logger.info("Created synthetic lead %s for dzialka %s", lead.id, dzialka_id)
        return lead

    lead.listing_id = listing_id
    lead.confidence_score = Decimal("0.99")
    lead.priority = "high"
    lead.max_coverage_pct = Decimal("91.00")
    lead.dominant_przeznaczenie = "MN/U"
    lead.price_per_m2_zl = Decimal("140.00")
    lead.estimated_value_uplift_pct = Decimal("65.00")
    lead.evidence_chain = evidence_chain
    lead.status = "new"
    lead.notes = "Synthetic Gliwice lead refreshed for frontend smoke test."
    lead.updated_at = datetime.now(timezone.utc)
    await db.flush()
    logger.info("Updated existing synthetic lead %s for dzialka %s", lead.id, dzialka_id)
    return lead


async def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    async with AsyncSessionLocal() as db:
        listing_id = await _find_gliwice_listing_id(db)
        dzialka_id = await _upsert_test_dzialka(db)
        lead = await _upsert_test_lead(db, dzialka_id=dzialka_id, listing_id=listing_id)
        await db.commit()

        geom_result = await db.execute(
            text(
                """
                SELECT
                    ST_AsText(ST_Centroid(ST_Transform(geom, 4326))) AS centroid_wgs84,
                    round(area_m2::numeric, 2) AS area_m2
                FROM silver.dzialki
                WHERE id = :dzialka_id
                """
            ),
            {"dzialka_id": dzialka_id},
        )
        geom_row = geom_result.mappings().one()

    print(f"\n{'=' * 60}")
    print("TEST LEAD SEEDED")
    print(f"{'=' * 60}")
    print(f"  Lead ID         : {lead.id}")
    print(f"  Dzialka ID      : {dzialka_id}")
    print(f"  Identyfikator   : {TEST_IDENTYFIKATOR}")
    print(f"  TERYT gmina     : {TEST_TERYT_GMINA} (Gliwice)")
    print(f"  Listing linked  : {listing_id or 'None'}")
    print(f"  Area            : {geom_row['area_m2']} m2")
    print(f"  Centroid WGS84  : {geom_row['centroid_wgs84']}")
    print("  API check       : http://localhost:8000/api/v1/leads?min_score=0.95&limit=5")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    asyncio.run(main())
