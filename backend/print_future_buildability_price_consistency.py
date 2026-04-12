from __future__ import annotations

import argparse
import logging
from decimal import Decimal

from sqlalchemy import text

from app.core.database import AsyncSessionLocal

_PRICE_CONSISTENCY_QUERY = text(
    """
    WITH assessment_rows AS (
        SELECT
            'assessment'::text AS row_kind,
            fba.dzialka_id,
            fba.listing_id,
            d.identyfikator,
            d.area_m2,
            rl.price_zl,
            fba.price_per_m2_zl AS stored_price_per_m2_zl,
            ROUND((rl.price_zl / NULLIF(d.area_m2, 0))::numeric, 2) AS expected_price_per_m2_zl
        FROM gold.future_buildability_assessments fba
        JOIN silver.dzialki d ON d.id = fba.dzialka_id
        LEFT JOIN bronze.raw_listings rl ON rl.id = fba.listing_id
    ),
    lead_rows AS (
        SELECT
            'lead'::text AS row_kind,
            il.dzialka_id,
            il.listing_id,
            d.identyfikator,
            d.area_m2,
            rl.price_zl,
            il.price_per_m2_zl AS stored_price_per_m2_zl,
            ROUND((rl.price_zl / NULLIF(d.area_m2, 0))::numeric, 2) AS expected_price_per_m2_zl
        FROM gold.investment_leads il
        JOIN silver.dzialki d ON d.id = il.dzialka_id
        LEFT JOIN bronze.raw_listings rl ON rl.id = il.listing_id
        WHERE il.strategy_type = 'future_buildable'
    )
    SELECT *
    FROM (
        SELECT * FROM assessment_rows
        UNION ALL
        SELECT * FROM lead_rows
    ) q
    ORDER BY row_kind, identyfikator
    """
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print future-buildable price_per_m2 consistency report")
    parser.add_argument(
        "--tolerance",
        type=Decimal,
        default=Decimal("0.01"),
        help="Allowed absolute difference between stored and expected price_per_m2_zl",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of mismatches to print",
    )
    return parser.parse_args()


async def _main() -> None:
    args = parse_args()
    async with AsyncSessionLocal() as db:
        rows = (await db.execute(_PRICE_CONSISTENCY_QUERY)).mappings().all()

    total = len(rows)
    comparable = 0
    mismatches: list[dict] = []
    missing_price_context = 0

    for row in rows:
        expected = row["expected_price_per_m2_zl"]
        stored = row["stored_price_per_m2_zl"]
        if expected is None and stored is None:
            missing_price_context += 1
            continue
        if expected is None or stored is None:
            mismatches.append(dict(row))
            continue
        comparable += 1
        if abs(Decimal(str(stored)) - Decimal(str(expected))) > args.tolerance:
            mismatches.append(dict(row))

    print("future_buildability_price_consistency")
    print(f"  total_rows: {total}")
    print(f"  comparable_rows: {comparable}")
    print(f"  missing_price_context: {missing_price_context}")
    print(f"  mismatches: {len(mismatches)}")
    print(f"  tolerance: {args.tolerance}")

    for row in mismatches[: args.limit]:
        print(
            "  - "
            f"{row['row_kind']} | {row['identyfikator']} | "
            f"stored={row['stored_price_per_m2_zl']} | expected={row['expected_price_per_m2_zl']} | "
            f"price_zl={row['price_zl']} | area_m2={row['area_m2']}"
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s | %(message)s")
    import asyncio

    asyncio.run(_main())
