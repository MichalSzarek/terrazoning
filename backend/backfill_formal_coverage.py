"""Backfill conservative planning coverage for formal-source backlog gminy.

This helper creates one synthetic planning zone per TERYT by unioning the
already resolved parcel geometries in `silver.dzialki`. It is intentionally
conservative:

- it does not invent buildable zoning classes,
- it stores a neutral `MPZP` designation that normalizes to `unknown`,
- it exists to mark geometry-backed planning coverage for operator-confirmed
  formal-source backlog gminy when public parcel-safe zoning geometry is still
  missing.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime, timezone

from geoalchemy2.shape import from_shape
from shapely import wkt as shapely_wkt
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.database import AsyncSessionLocal
from app.models.gold import PlanningZone


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill conservative planning coverage for formal-source backlog gminy."
    )
    parser.add_argument("--teryt", action="append", required=True, help="7-digit TERYT gmina code")
    parser.add_argument(
        "--plan-name-prefix",
        default="Formal backlog coverage",
        help="Prefix for stored plan_name",
    )
    parser.add_argument(
        "--designation",
        default="MPZP",
        help="Neutral designation stored in planning_zones (default: MPZP)",
    )
    parser.add_argument(
        "--description",
        default="Formal-source synthetic parcel envelope coverage",
        help="Stored przeznaczenie_opis",
    )
    return parser.parse_args()


async def backfill_one(
    *,
    teryt_gmina: str,
    plan_name_prefix: str,
    designation: str,
    description: str,
) -> tuple[str, int]:
    source_url = f"synthetic://formal_backlog/{teryt_gmina}"

    async with AsyncSessionLocal() as db:
        union_row = (
            await db.execute(
                text(
                    """
                    SELECT
                      COUNT(*) AS parcel_count,
                      ST_AsText(ST_UnaryUnion(ST_Collect(geom))) AS geom_wkt
                    FROM silver.dzialki
                    WHERE teryt_gmina = :teryt
                      AND geom IS NOT NULL
                      AND resolution_status = 'resolved'
                    """
                ),
                {"teryt": teryt_gmina},
            )
        ).mappings().first()

        if not union_row or not union_row["geom_wkt"]:
            return teryt_gmina, 0

        geom = shapely_wkt.loads(union_row["geom_wkt"])
        geom_wkb = from_shape(geom, srid=2180)
        centroid = geom.centroid
        geom_hash = f"{round(centroid.x)}_{round(centroid.y)}"

        await db.execute(
            text("DELETE FROM gold.planning_zones WHERE source_wfs_url = :source_url"),
            {"source_url": source_url},
        )

        stmt = (
            pg_insert(PlanningZone)
            .values(
                plan_type="mpzp",
                plan_name=f"{plan_name_prefix} {teryt_gmina}",
                uchwala_nr=f"FORMAL/{teryt_gmina}",
                teryt_gmina=teryt_gmina,
                przeznaczenie=designation,
                przeznaczenie_opis=description,
                geom=geom_wkb,
                geom_hash=geom_hash,
                source_wfs_url=source_url,
                ingested_at=datetime.now(timezone.utc),
                plan_effective_date=date(2026, 1, 1),
            )
            .on_conflict_do_update(
                constraint="uq_planning_zones_spatial_key",
                set_={
                    "geom": geom_wkb,
                    "geom_hash": geom_hash,
                    "plan_name": f"{plan_name_prefix} {teryt_gmina}",
                    "uchwala_nr": f"FORMAL/{teryt_gmina}",
                    "przeznaczenie": designation,
                    "przeznaczenie_opis": description,
                    "updated_at": datetime.now(timezone.utc),
                    "ingested_at": datetime.now(timezone.utc),
                },
            )
        )
        await db.execute(stmt)
        await db.commit()
        return teryt_gmina, int(union_row["parcel_count"] or 0)


async def main() -> None:
    args = parse_args()
    results = []
    for teryt in args.teryt:
        results.append(
            await backfill_one(
                teryt_gmina=teryt,
                plan_name_prefix=args.plan_name_prefix,
                designation=args.designation,
                description=args.description,
            )
        )

    print("\n============================================================")
    print("FORMAL COVERAGE BACKFILL COMPLETE")
    print("============================================================")
    for teryt, parcel_count in results:
        print(f"  {teryt}  parcels_used={parcel_count}")
    print("============================================================")


if __name__ == "__main__":
    asyncio.run(main())
