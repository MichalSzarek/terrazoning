from __future__ import annotations

import argparse
import asyncio
import json
import math
from collections import Counter
from dataclasses import asdict, dataclass
from typing import Any

from sqlalchemy import text

from app.core.database import AsyncSessionLocal
from app.services.operations_scope import province_display_name, provinces
from run_wfs_sync import _fetch_uncovered_gminy


_COVERAGE_QUERY = text(
    """
    WITH dzialki AS (
        SELECT CASE
                 WHEN d.teryt_wojewodztwo = '12' THEN 'malopolskie'
                 WHEN d.teryt_wojewodztwo = '18' THEN 'podkarpackie'
                 WHEN d.teryt_wojewodztwo = '24' THEN 'slaskie'
               END AS province,
               d.teryt_gmina
        FROM silver.dzialki d
        WHERE d.teryt_wojewodztwo IN ('12', '18', '24')
    ),
    covered AS (
        SELECT CASE
                 WHEN d.teryt_wojewodztwo = '12' THEN 'malopolskie'
                 WHEN d.teryt_wojewodztwo = '18' THEN 'podkarpackie'
                 WHEN d.teryt_wojewodztwo = '24' THEN 'slaskie'
               END AS province,
               d.teryt_gmina
        FROM silver.dzialki d
        JOIN gold.planning_zones p ON p.teryt_gmina = d.teryt_gmina
        WHERE d.teryt_wojewodztwo IN ('12', '18', '24')
        GROUP BY 1, 2
    ),
    delta_gminy AS (
        SELECT CASE
                 WHEN d.teryt_wojewodztwo = '12' THEN 'malopolskie'
                 WHEN d.teryt_wojewodztwo = '18' THEN 'podkarpackie'
                 WHEN d.teryt_wojewodztwo = '24' THEN 'slaskie'
               END AS province,
               d.teryt_gmina
        FROM gold.delta_results dr
        JOIN silver.dzialki d ON d.id = dr.dzialka_id
        WHERE d.teryt_wojewodztwo IN ('12', '18', '24')
        GROUP BY 1, 2
    ),
    lead_gminy AS (
        SELECT CASE
                 WHEN d.teryt_wojewodztwo = '12' THEN 'malopolskie'
                 WHEN d.teryt_wojewodztwo = '18' THEN 'podkarpackie'
                 WHEN d.teryt_wojewodztwo = '24' THEN 'slaskie'
               END AS province,
               d.teryt_gmina
        FROM gold.investment_leads il
        JOIN silver.dzialki d ON d.id = il.dzialka_id
        WHERE d.teryt_wojewodztwo IN ('12', '18', '24')
        GROUP BY 1, 2
    )
    SELECT
        d.province,
        count(DISTINCT d.teryt_gmina) AS gminy_with_dzialki,
        count(DISTINCT c.teryt_gmina) AS gminy_covered,
        round(
            100.0 * count(DISTINCT c.teryt_gmina)::numeric
            / NULLIF(count(DISTINCT d.teryt_gmina), 0),
            1
        ) AS coverage_pct,
        count(DISTINCT dg.teryt_gmina) AS gminy_with_delta,
        count(DISTINCT lg.teryt_gmina) AS gminy_with_leads
    FROM dzialki d
    LEFT JOIN covered c
        ON c.province = d.province
       AND c.teryt_gmina = d.teryt_gmina
    LEFT JOIN delta_gminy dg
        ON dg.province = d.province
       AND dg.teryt_gmina = d.teryt_gmina
    LEFT JOIN lead_gminy lg
        ON lg.province = d.province
       AND lg.teryt_gmina = d.teryt_gmina
    WHERE d.province IS NOT NULL
    GROUP BY d.province
    ORDER BY d.province
    """
)


@dataclass
class CoverageTargetStatus:
    province: str
    province_label: str
    gminy_with_dzialki: int
    gminy_covered: int
    coverage_pct: float
    target_pct: int
    target_gminy: int
    gminy_needed_to_target: int
    gminy_with_delta: int
    gminy_with_leads: int
    uncovered_gminy: int
    uncovered_buckets: dict[str, int]
    top_uncovered: list[dict[str, Any]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print province coverage progress toward a target percentage.")
    parser.add_argument(
        "--province",
        action="append",
        choices=provinces(),
        help="Province to inspect. Repeat to pass multiple provinces; defaults to all provinces.",
    )
    parser.add_argument("--target-pct", type=int, default=70)
    parser.add_argument("--limit", type=int, default=10, help="How many uncovered gminy to show per province.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


async def _load_status(*, provinces_filter: set[str] | None, target_pct: int, limit: int) -> list[CoverageTargetStatus]:
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(_COVERAGE_QUERY)).mappings().all()

    statuses: list[CoverageTargetStatus] = []
    for row in rows:
        province = str(row["province"])
        if provinces_filter and province not in provinces_filter:
            continue

        uncovered = await _fetch_uncovered_gminy(limit=200, province=province)
        buckets = Counter(item.coverage_category for item in uncovered)
        total_gminy = int(row["gminy_with_dzialki"])
        target_gminy = math.ceil((target_pct / 100.0) * total_gminy)
        top_uncovered = [
            {
                "teryt": item.teryt,
                "dzialki_count": item.dzialki_count,
                "coverage_category": item.coverage_category,
                "localities": item.localities,
                "next_action": item.next_action,
            }
            for item in uncovered[:limit]
        ]
        statuses.append(
            CoverageTargetStatus(
                province=province,
                province_label=province_display_name(province) or province,
                gminy_with_dzialki=total_gminy,
                gminy_covered=int(row["gminy_covered"]),
                coverage_pct=float(row["coverage_pct"] or 0.0),
                target_pct=target_pct,
                target_gminy=target_gminy,
                gminy_needed_to_target=max(target_gminy - int(row["gminy_covered"]), 0),
                gminy_with_delta=int(row["gminy_with_delta"]),
                gminy_with_leads=int(row["gminy_with_leads"]),
                uncovered_gminy=len(uncovered),
                uncovered_buckets=dict(buckets),
                top_uncovered=top_uncovered,
            )
        )
    return statuses


def _print_text(statuses: list[CoverageTargetStatus]) -> None:
    for status in statuses:
        print(f"\n{'=' * 72}")
        print(f"{status.province_label.upper()} — COVERAGE TARGET {status.target_pct}%")
        print(f"{'=' * 72}")
        print(f"  Gminy z działkami   : {status.gminy_with_dzialki}")
        print(f"  Gminy covered       : {status.gminy_covered}")
        print(f"  Coverage            : {status.coverage_pct:.1f}%")
        print(f"  Target gminy        : {status.target_gminy}")
        print(f"  Brakuje do targetu  : {status.gminy_needed_to_target}")
        print(f"  Gminy z deltą       : {status.gminy_with_delta}")
        print(f"  Gminy z leadami     : {status.gminy_with_leads}")
        print(f"  Uncovered gminy     : {status.uncovered_gminy}")
        print(f"  Uncovered buckets   : {status.uncovered_buckets}")
        if status.top_uncovered:
            print("\n  Top uncovered:")
            for row in status.top_uncovered:
                print(
                    f"    {row['teryt']}  dzialki={row['dzialki_count']}  "
                    f"bucket={row['coverage_category']}  localities={row['localities'] or '-'}"
                )


async def main() -> None:
    args = parse_args()
    provinces_filter = set(args.province) if args.province else None
    statuses = await _load_status(
        provinces_filter=provinces_filter,
        target_pct=args.target_pct,
        limit=args.limit,
    )
    if args.json:
        print(json.dumps([asdict(status) for status in statuses], ensure_ascii=False, indent=2))
        return
    _print_text(statuses)


if __name__ == "__main__":
    asyncio.run(main())
