"""Seed synthetic MPZP planning zones around resolved parcels.

This script is for DEVELOPMENT/TESTING only.
It creates synthetic planning zones by buffering existing parcel geometries
from silver.dzialki, so the delta_engine has something to intersect with.

Background: The GUGiK national WFS for MPZP zones
(integracja.gugik.gov.pl/cgi-bin/KrajowaIntegracjaUzytkowaniaTerenu)
was discontinued. The replacement (mapy.geoportal.gov.pl) is WMS-only.
Real MPZP data must come from municipality-level WFS services configured
per-gmina in run_wfs_sync.py.

Usage:
    uv run python seed_test_zones.py
    uv run python seed_test_zones.py --buffer-m 500 --verbose
    uv run python seed_test_zones.py --clear     # remove synthetic zones first
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
from datetime import date, datetime, timezone

from geoalchemy2.shape import from_shape
from shapely.geometry import box
from shapely.ops import unary_union
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.database import AsyncSessionLocal
from app.models.gold import PlanningZone
from app.models.silver import Dzialka

logger = logging.getLogger("seed_test_zones")

# Realistic MPZP zone designations used in Polish planning
_ZONE_TYPES = [
    ("MN", "Tereny zabudowy mieszkaniowej jednorodzinnej"),
    ("MU", "Tereny zabudowy mieszkaniowo-usługowej"),
    ("U", "Tereny usług komercyjnych"),
    ("PU", "Tereny produkcyjno-usługowe"),
    ("ZP", "Tereny zieleni urządzonej"),
    ("ZL", "Tereny lasów"),
    ("R", "Tereny rolnicze"),
    ("KD", "Tereny dróg publicznych"),
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed synthetic MPZP zones for pipeline testing"
    )
    parser.add_argument(
        "--buffer-m",
        type=float,
        default=300.0,
        help="Buffer radius in metres around each parcel (default: 300)",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Delete existing synthetic zones before seeding",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG logging",
    )
    return parser.parse_args()


async def clear_synthetic_zones(db_session) -> int:
    """Remove all planning zones marked as synthetic (source_wfs_url IS NULL)."""
    result = await db_session.execute(
        text("DELETE FROM gold.planning_zones WHERE source_wfs_url IS NULL OR source_wfs_url LIKE '%seed_test%'")
    )
    await db_session.commit()
    return result.rowcount


async def seed_zones(buffer_m: float = 300.0) -> int:
    """Create synthetic planning zones around all resolved parcels.

    Returns number of zones inserted.
    """
    async with AsyncSessionLocal() as db:
        # Load all resolved parcels with geometry
        result = await db.execute(
            select(Dzialka).where(Dzialka.geom.is_not(None))
        )
        dzialki = result.scalars().all()

        if not dzialki:
            logger.warning("No parcels with geometry found in silver.dzialki")
            return 0

        logger.info("Found %d parcels to create zones for", len(dzialki))

        # Group parcels by teryt_gmina so zones are realistic
        gmina_parcels: dict[str, list] = {}
        for d in dzialki:
            key = d.teryt_gmina or "unknown"
            gmina_parcels.setdefault(key, []).append(d)

        inserted = 0
        for teryt_gmina, parcels in gmina_parcels.items():
            logger.debug("Processing gmina=%s (%d parcels)", teryt_gmina, len(parcels))

            for dzialka in parcels:
                # Get parcel geometry via raw SQL (avoids geoalchemy2 shapely import dance)
                geo_result = await db.execute(
                    text(
                        "SELECT ST_AsText(ST_Buffer(geom, :buf)) as zone_wkt, "
                        "       ST_AsText(ST_Envelope(geom)) as bbox_wkt "
                        "FROM silver.dzialki WHERE id = :id"
                    ),
                    {"buf": buffer_m, "id": dzialka.id},
                )
                row = geo_result.fetchone()
                if row is None:
                    continue

                # Pick a random zone type (seed for reproducibility based on identyfikator)
                rng = random.Random(hash(dzialka.identyfikator))
                zone_code, zone_desc = rng.choice(_ZONE_TYPES)

                # Parse the buffered polygon WKT into a Shapely geometry
                from shapely import wkt as shapely_wkt
                zone_geom = shapely_wkt.loads(row.zone_wkt)
                geom_wkb = from_shape(zone_geom, srid=2180)
                c = zone_geom.centroid
                geom_hash = f"{round(c.x)}_{round(c.y)}"

                stmt = (
                    pg_insert(PlanningZone)
                    .values(
                        plan_type="mpzp",
                        plan_name=f"MPZP gmina {teryt_gmina} [TEST]",
                        uchwala_nr=f"TEST/{teryt_gmina}/001",
                        teryt_gmina=teryt_gmina,
                        przeznaczenie=zone_code,
                        przeznaczenie_opis=zone_desc,
                        geom=geom_wkb,
                        geom_hash=geom_hash,
                        source_wfs_url="seed_test://synthetic",
                        ingested_at=datetime.now(timezone.utc),
                        plan_effective_date=date(2020, 1, 1),
                    )
                    .on_conflict_do_update(
                        constraint="uq_planning_zones_spatial_key",
                        set_={
                            "geom": geom_wkb,
                            "ingested_at": datetime.now(timezone.utc),
                            "updated_at": datetime.now(timezone.utc),
                        },
                    )
                )
                await db.execute(stmt)
                inserted += 1
                logger.debug(
                    "  Inserted zone %s for parcel %s",
                    zone_code,
                    dzialka.identyfikator,
                )

        await db.commit()
        return inserted


async def main() -> None:
    args = _parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    async with AsyncSessionLocal() as db:
        if args.clear:
            removed = await clear_synthetic_zones(db)
            logger.info("Cleared %d synthetic zones", removed)

    count = await seed_zones(buffer_m=args.buffer_m)

    print(f"\n{'=' * 50}")
    print("SEED COMPLETE")
    print(f"{'=' * 50}")
    print(f"  Zones inserted  : {count}")
    print(f"  Buffer radius   : {args.buffer_m} m")
    print(f"  Source          : synthetic (seed_test)")
    print(f"\n  Run delta engine next:")
    print(f"    uv run python -m app.services.delta_engine")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    asyncio.run(main())
