"""List and promote quarantine parcels with a deterministic operator heuristic.

This helper is intentionally conservative:
- it only considers already resolved parcels without an investment lead,
- it ranks them by source-url / area hints,
- and it promotes exactly one parcel through the existing manual override path.

Typical usage:

    uv run python promote_quarantine_parcel.py --province podkarpackie
    uv run python promote_quarantine_parcel.py --province podkarpackie --auto-pick --apply
    uv run python promote_quarantine_parcel.py --dzialka-id <uuid> --manual-przeznaczenie MN --apply
"""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict, dataclass, field
from typing import Any
from uuid import UUID

from sqlalchemy import text

from app.core.database import AsyncSessionLocal
from app.services.operations_scope import normalize_province, province_teryt_prefix
from app.services.quarantine_parcels import apply_manual_override


_POSITIVE_HINTS: tuple[tuple[str, float, str], ...] = (
    ("warunkami-zabudowy", 45.0, "listing_mentions_wz"),
    ("inwestycyjne", 35.0, "listing_mentions_investment"),
    ("budowlane", 30.0, "listing_mentions_buildable"),
    ("widokowe", 12.0, "listing_mentions_view"),
    ("mpzp", 15.0, "listing_mentions_mpzp"),
    ("niezabudowane", 8.0, "listing_mentions_unbuilt"),
)
_NEGATIVE_HINTS: tuple[tuple[str, float, str], ...] = (
    ("rolna", -35.0, "listing_mentions_agricultural"),
    ("las", -30.0, "listing_mentions_forest"),
    ("zalesien", -30.0, "listing_mentions_afforestation"),
    ("udzial", -20.0, "listing_mentions_fractional_share"),
)


@dataclass(slots=True)
class CandidateScore:
    score: float
    reasons: list[str] = field(default_factory=list)


@dataclass(slots=True)
class QuarantineCandidate:
    dzialka_id: str
    identyfikator: str
    teryt_gmina: str
    area_m2: float | None
    current_use: str | None
    source_url: str | None
    has_planning_zones: bool
    score: float
    score_reasons: list[str]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="List and promote quarantine parcels with a deterministic operator heuristic."
    )
    parser.add_argument("--province", help="Province scope, e.g. podkarpackie")
    parser.add_argument("--teryt", help="Single TERYT gmina scope, e.g. 1816145")
    parser.add_argument("--dzialka-id", help="Promote a specific silver.dzialki UUID")
    parser.add_argument(
        "--manual-przeznaczenie",
        default="MN",
        help="Manual planning symbol to inject during promotion (default: MN)",
    )
    parser.add_argument(
        "--source-url-contains",
        action="append",
        default=[],
        help="Optional substring filter for source_url; may be passed multiple times",
    )
    parser.add_argument("--limit", type=int, default=20, help="Number of candidates to print")
    parser.add_argument(
        "--auto-pick",
        action="store_true",
        help="Pick the top-ranked candidate when --dzialka-id is not provided",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist the manual override instead of only printing candidates",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output")
    return parser.parse_args(argv)


def _score_candidate(*, area_m2: float | None, current_use: str | None, source_url: str | None) -> CandidateScore:
    lowered = (source_url or "").lower()
    score = 0.0
    reasons: list[str] = []

    for token, weight, reason in _POSITIVE_HINTS:
        if token in lowered:
            score += weight
            reasons.append(reason)
    for token, weight, reason in _NEGATIVE_HINTS:
        if token in lowered:
            score += weight
            reasons.append(reason)

    if area_m2 is not None:
        if 300.0 <= area_m2 <= 5000.0:
            score += 20.0
            reasons.append("area_in_investor_band")
        elif 150.0 <= area_m2 < 300.0:
            score += 6.0
            reasons.append("area_small_but_usable")
        elif area_m2 > 5000.0:
            score += 8.0
            reasons.append("area_large")

    normalized_use = (current_use or "").upper()
    if normalized_use in {"R", "LS", "Ł"}:
        score += 8.0
        reasons.append("current_use_nonbuilt")
    elif normalized_use == "B":
        score += 3.0
        reasons.append("current_use_residential_ground")

    return CandidateScore(score=score, reasons=reasons)


async def _fetch_candidates(args: argparse.Namespace) -> list[QuarantineCandidate]:
    province_prefix = None
    if args.province:
        province = normalize_province(args.province)
        if province is None:
            raise SystemExit(f"Unsupported province: {args.province!r}")
        province_prefix = province_teryt_prefix(province)

    filters = [
        "d.resolution_status = 'resolved'",
        "NOT EXISTS (SELECT 1 FROM gold.investment_leads il WHERE il.dzialka_id = d.id)",
    ]
    params: dict[str, Any] = {"limit": max(args.limit * 5, 50)}

    if province_prefix:
        filters.append("substr(d.teryt_gmina, 1, 2) = :province_prefix")
        params["province_prefix"] = province_prefix
    if args.teryt:
        filters.append("d.teryt_gmina = :teryt")
        params["teryt"] = args.teryt
    if args.dzialka_id:
        filters.append("d.id = CAST(:dzialka_id AS uuid)")
        params["dzialka_id"] = args.dzialka_id

    stmt = text(
        f"""
        WITH latest_source AS (
          SELECT DISTINCT ON (lp.dzialka_id)
            lp.dzialka_id,
            rl.source_url
          FROM silver.listing_parcels lp
          JOIN bronze.raw_listings rl ON rl.id = lp.listing_id
          ORDER BY lp.dzialka_id, lp.created_at DESC
        )
        SELECT
          d.id::text AS dzialka_id,
          d.identyfikator,
          d.teryt_gmina,
          d.area_m2,
          d.current_use,
          ls.source_url,
          EXISTS (
            SELECT 1
            FROM gold.planning_zones pz
            WHERE pz.teryt_gmina = d.teryt_gmina
          ) AS has_planning_zones
        FROM silver.dzialki d
        LEFT JOIN latest_source ls ON ls.dzialka_id = d.id
        WHERE {" AND ".join(filters)}
        ORDER BY d.created_at DESC
        LIMIT :limit
        """
    )

    async with AsyncSessionLocal() as db:
        rows = (await db.execute(stmt, params)).mappings().all()

    filtered_rows = []
    required_tokens = [token.lower() for token in args.source_url_contains]
    for row in rows:
        source_url = row["source_url"] or ""
        if required_tokens and not all(token in source_url.lower() for token in required_tokens):
            continue
        score = _score_candidate(
            area_m2=float(row["area_m2"]) if row["area_m2"] is not None else None,
            current_use=row["current_use"],
            source_url=row["source_url"],
        )
        filtered_rows.append(
            QuarantineCandidate(
                dzialka_id=row["dzialka_id"],
                identyfikator=row["identyfikator"],
                teryt_gmina=row["teryt_gmina"],
                area_m2=float(row["area_m2"]) if row["area_m2"] is not None else None,
                current_use=row["current_use"],
                source_url=row["source_url"],
                has_planning_zones=bool(row["has_planning_zones"]),
                score=score.score,
                score_reasons=score.reasons,
            )
        )

    filtered_rows.sort(
        key=lambda item: (
            -item.score,
            item.teryt_gmina,
            -(item.area_m2 or 0.0),
            item.identyfikator,
        )
    )
    return filtered_rows[: args.limit]


async def _apply(args: argparse.Namespace, candidate: QuarantineCandidate) -> dict[str, Any]:
    async with AsyncSessionLocal() as db:
        result = await apply_manual_override(
            db,
            dzialka_id=UUID(candidate.dzialka_id),
            manual_przeznaczenie=args.manual_przeznaczenie,
        )
    return {
        "candidate": asdict(candidate),
        "manual_przeznaczenie": args.manual_przeznaczenie,
        "result": {
            "dzialka_id": str(result.dzialka_id),
            "lead_id": str(result.lead_id),
            "delta_result_id": str(result.delta_result_id),
            "planning_zone_id": str(result.planning_zone_id),
            "source_url": result.source_url,
            "lead_created": result.lead_created,
            "lead_updated": result.lead_updated,
        },
    }


async def _main_async(args: argparse.Namespace) -> int:
    candidates = await _fetch_candidates(args)

    if args.apply:
        selected = None
        if args.dzialka_id:
            selected = next((item for item in candidates if item.dzialka_id == args.dzialka_id), None)
        elif args.auto_pick and candidates:
            selected = candidates[0]

        if selected is None:
            raise SystemExit("No candidate selected for --apply. Pass --dzialka-id or use --auto-pick.")

        payload = await _apply(args, selected)
        print(json.dumps(payload, ensure_ascii=False, indent=2 if args.json else None))
        return 0

    payload = {
        "count": len(candidates),
        "candidates": [asdict(candidate) for candidate in candidates],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"count={payload['count']}")
        for candidate in candidates:
            reasons = ",".join(candidate.score_reasons) or "-"
            print(
                f"{candidate.score:6.1f}  {candidate.teryt_gmina}  {candidate.identyfikator}  "
                f"{candidate.dzialka_id}  area={candidate.area_m2}  current_use={candidate.current_use}  "
                f"zones={candidate.has_planning_zones}  reasons={reasons}  source={candidate.source_url}"
            )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
