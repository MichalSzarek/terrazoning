from __future__ import annotations

import argparse
import asyncio
import logging
from uuid import UUID

from sqlalchemy import or_, select

from app.core.database import AsyncSessionLocal
from app.models.silver import Dzialka, ListingParcel
from app.services.operations_scope import province_teryt_prefix
from app.services.future_buildability_engine import run_future_buildability_engine


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run future-buildability assessments")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--dzialka-id", action="append", dest="dzialka_ids")
    parser.add_argument("--teryt-gmina", action="append", dest="teryt_gminy")
    parser.add_argument(
        "--province",
        action="append",
        dest="provinces",
        help="Optional province scope: slaskie | malopolskie",
    )
    return parser.parse_args(argv)


async def _resolve_scoped_dzialka_ids(
    *,
    explicit_ids: list[UUID] | None,
    teryt_gminy: list[str] | None,
    provinces: list[str] | None,
) -> list[UUID] | None:
    scoped_ids: list[UUID] = list(explicit_ids or [])
    if not teryt_gminy and not provinces:
        return scoped_ids or None

    stmt = (
        select(Dzialka.id)
        .join(ListingParcel, ListingParcel.dzialka_id == Dzialka.id)
        .where(Dzialka.resolution_status == "resolved")
    )
    filters = []
    if teryt_gminy:
        filters.append(Dzialka.teryt_gmina.in_(tuple(dict.fromkeys(teryt_gminy))))
    if provinces:
        prefixes = [province_teryt_prefix(province) for province in provinces]
        valid_prefixes = [prefix for prefix in prefixes if prefix]
        if valid_prefixes:
            filters.append(or_(*[Dzialka.teryt_gmina.like(f"{prefix}%") for prefix in valid_prefixes]))
    if filters:
        stmt = stmt.where(or_(*filters))
    stmt = stmt.group_by(Dzialka.id).order_by(Dzialka.created_at.asc())

    async with AsyncSessionLocal() as db:
        result = await db.execute(stmt)
        scoped_ids.extend(row[0] for row in result.fetchall())

    deduped = list(dict.fromkeys(scoped_ids))
    return deduped or None


async def _main() -> None:
    args = parse_args()
    explicit_ids = [UUID(raw) for raw in args.dzialka_ids] if args.dzialka_ids else None
    dzialka_ids = await _resolve_scoped_dzialka_ids(
        explicit_ids=explicit_ids,
        teryt_gminy=args.teryt_gminy,
        provinces=args.provinces,
    )
    report = await run_future_buildability_engine(
        batch_size=args.batch_size,
        dzialka_ids=dzialka_ids,
    )
    print(
        "future_buildability "
        f"analyzed={report.dzialki_analyzed} "
        f"assessments_created={report.assessments_created} "
        f"assessments_updated={report.assessments_updated} "
        f"leads_created={report.leads_created} "
        f"leads_updated={report.leads_updated} "
        f"duration_s={report.duration_s}"
    )
    if report.errors:
        print("errors:")
        for error in report.errors:
            print(f"  - {error}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    asyncio.run(_main())
