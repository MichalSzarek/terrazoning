from __future__ import annotations

import argparse
import asyncio
from typing import Any

from sqlalchemy import text

from app.core.database import AsyncSessionLocal
from app.services.operations_scope import province_display_name, province_teryt_prefix, provinces

_SUMMARY_QUERY = text(
    """
    SELECT
        COUNT(*) AS total_parcels,
        COUNT(*) FILTER (WHERE NULLIF(BTRIM(current_use), '') IS NOT NULL) AS filled_current_use,
        COUNT(*) FILTER (WHERE NULLIF(BTRIM(current_use), '') IS NULL) AS missing_current_use
    FROM silver.dzialki
    WHERE (
        CAST(:province_prefix AS text) IS NULL
        OR substr(teryt_gmina, 1, 2) = CAST(:province_prefix AS text)
    )
    """
)

_BY_CODE_QUERY = text(
    """
    SELECT
        COALESCE(NULLIF(BTRIM(current_use), ''), '(missing)') AS current_use_code,
        COUNT(*) AS parcel_count
    FROM silver.dzialki
    WHERE (
        CAST(:province_prefix AS text) IS NULL
        OR substr(teryt_gmina, 1, 2) = CAST(:province_prefix AS text)
    )
    GROUP BY 1
    ORDER BY COUNT(*) DESC, 1
    LIMIT :limit
    """
)

_MISSING_BY_TERYT_QUERY = text(
    """
    SELECT
        teryt_gmina,
        COUNT(*) AS missing_count,
        MIN(identyfikator) AS sample_ident
    FROM silver.dzialki
    WHERE NULLIF(BTRIM(current_use), '') IS NULL
      AND (
        CAST(:province_prefix AS text) IS NULL
        OR substr(teryt_gmina, 1, 2) = CAST(:province_prefix AS text)
      )
    GROUP BY teryt_gmina
    ORDER BY COUNT(*) DESC, teryt_gmina
    LIMIT :limit
    """
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print silver.dzialki current_use coverage status")
    parser.add_argument(
        "--province",
        choices=provinces(),
        help="Optional province scope",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Max rows for code / TERYT breakdowns",
    )
    return parser.parse_args(argv)


async def _fetch_all(query, *, province_prefix: str | None, limit: int) -> list[dict[str, Any]]:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            query,
            {"province_prefix": province_prefix, "limit": limit},
        )
        return [dict(row) for row in result.mappings().all()]


async def _fetch_one(query, *, province_prefix: str | None) -> dict[str, Any]:
    async with AsyncSessionLocal() as db:
        result = await db.execute(query, {"province_prefix": province_prefix})
        row = result.mappings().one()
        return dict(row)


def _fmt_pct(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0.0%"
    return f"{(numerator / denominator) * 100:.1f}%"


async def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    province_prefix = province_teryt_prefix(args.province) if args.province else None
    summary = await _fetch_one(_SUMMARY_QUERY, province_prefix=province_prefix)
    by_code = await _fetch_all(_BY_CODE_QUERY, province_prefix=province_prefix, limit=args.limit)
    missing_by_teryt = await _fetch_all(
        _MISSING_BY_TERYT_QUERY,
        province_prefix=province_prefix,
        limit=args.limit,
    )

    total = int(summary["total_parcels"] or 0)
    filled = int(summary["filled_current_use"] or 0)
    missing = int(summary["missing_current_use"] or 0)
    suffix = f" — {province_display_name(args.province)}" if args.province else ""

    print(f"\n{'=' * 72}")
    print(f"CURRENT_USE STATUS{suffix}")
    print(f"{'=' * 72}")
    print(f"  Total parcels        : {total}")
    print(f"  Filled current_use   : {filled} ({_fmt_pct(filled, total)})")
    print(f"  Missing current_use  : {missing} ({_fmt_pct(missing, total)})")

    print(f"\nTop current_use codes (limit={args.limit}):")
    if not by_code:
        print("  - no parcels in scope")
    else:
        for row in by_code:
            code = row["current_use_code"]
            count = int(row["parcel_count"] or 0)
            print(f"  - {code}: {count}")

    print(f"\nTop TERYT gaps (limit={args.limit}):")
    if not missing_by_teryt:
        print("  - no missing current_use rows")
    else:
        for row in missing_by_teryt:
            print(
                f"  - {row['teryt_gmina']}: missing={int(row['missing_count'] or 0)} "
                f"sample={row['sample_ident']}"
            )
    print(f"{'=' * 72}\n")


if __name__ == "__main__":
    asyncio.run(main())
