from __future__ import annotations

import argparse

from sqlalchemy import text

from app.core.database import AsyncSessionLocal

_COUNT_QUERY = text("SELECT COUNT(*) AS total FROM gold.investment_leads")

_DEDUP_QUERY = text(
    """
    WITH ranked AS (
        SELECT
            id,
            dzialka_id,
            COALESCE(strategy_type, 'current_buildable') AS strategy_type_key,
            ROW_NUMBER() OVER (
                PARTITION BY dzialka_id, COALESCE(strategy_type, 'current_buildable')
                ORDER BY COALESCE(updated_at, created_at) DESC, id DESC
            ) AS rn
        FROM gold.investment_leads
    )
    DELETE FROM gold.investment_leads il
    USING ranked r
    WHERE il.id = r.id
      AND r.rn > 1
    RETURNING il.id
    """
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deduplicate gold.investment_leads by dzialka/strategy")
    parser.add_argument("--dry-run", action="store_true", help="Only report counts, do not delete")
    return parser.parse_args()


async def _main() -> None:
    args = _parse_args()
    async with AsyncSessionLocal() as db:
        before = (await db.execute(_COUNT_QUERY)).mappings().one()["total"]
        deleted = 0
        if not args.dry_run:
            deleted = len((await db.execute(_DEDUP_QUERY)).scalars().all())
            await db.commit()
        after = (await db.execute(_COUNT_QUERY)).mappings().one()["total"]

    print("dedupe_investment_leads")
    print(f"  dry_run: {args.dry_run}")
    print(f"  total_before: {before}")
    print(f"  deleted: {deleted}")
    print(f"  total_after: {after}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(_main())
