from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.core.database import AsyncSessionLocal

_ASSESSMENT_QUERY = text(
    """
    SELECT
        fba.dzialka_id,
        d.identyfikator,
        d.numer_dzialki AS parcel_number,
        d.teryt_wojewodztwo,
        d.teryt_gmina,
        fba.current_buildable_status,
        fba.confidence_band,
        fba.future_signal_score,
        fba.cheapness_score,
        fba.overall_score,
        fba.dominant_future_signal,
        fba.signal_breakdown,
        fba.evidence_chain,
        fba.price_per_m2_zl
    FROM gold.future_buildability_assessments fba
    JOIN silver.dzialki d ON d.id = fba.dzialka_id
    ORDER BY fba.overall_score DESC NULLS LAST, d.identyfikator
    """
)

_PROVINCE_MAP = {"12": "malopolskie", "24": "slaskie"}
_HARD_NEGATIVE = {"forest", "water", "green", "cemetery", "infrastructure"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a seed validation corpus for future_buildable")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/future_buildability_validation_corpus.json"),
        help="Where to write the generated JSON seed",
    )
    return parser.parse_args()


def _province_name(prefix: str | None) -> str:
    return _PROVINCE_MAP.get((prefix or "").strip(), "other")


def _has_positive_kind(breakdown: list[dict[str, Any]], kinds: set[str]) -> bool:
    return any(item.get("kind") in kinds and float(item.get("weight") or 0.0) > 0 for item in breakdown)


def _has_hard_negative(breakdown: list[dict[str, Any]]) -> bool:
    return any(
        (item.get("designation_normalized") or "") in _HARD_NEGATIVE
        and float(item.get("weight") or 0.0) < 0
        for item in breakdown
    )


def _pick_balanced(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    per_province: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        per_province[row["province"]].append(row)

    picked: list[dict[str, Any]] = []
    round_robin_provinces = [province for province in ("slaskie", "malopolskie") if per_province.get(province)]
    while round_robin_provinces and len(picked) < limit:
        next_round: list[str] = []
        for province in round_robin_provinces:
            if len(picked) >= limit:
                break
            province_rows = per_province[province]
            if province_rows:
                picked.append(province_rows.pop(0))
            if province_rows:
                next_round.append(province)
        round_robin_provinces = next_round

    if len(picked) < limit:
        leftovers = [row for province_rows in per_province.values() for row in province_rows]
        picked.extend(leftovers[: limit - len(picked)])
    return picked[:limit]


def _extend_with_fallback(
    selected: list[dict[str, Any]],
    fallback_rows: list[dict[str, Any]],
    limit: int,
    used_ids: set[str],
) -> list[dict[str, Any]]:
    extended = list(selected)
    for row in fallback_rows:
        if len(extended) >= limit:
            break
        if row["dzialka_id"] in used_ids:
            continue
        extended.append(row)
        used_ids.add(row["dzialka_id"])
    return extended[:limit]


async def _main() -> None:
    args = parse_args()
    async with AsyncSessionLocal() as db:
        rows = (await db.execute(_ASSESSMENT_QUERY)).mappings().all()

    enriched: list[dict[str, Any]] = []
    for raw in rows:
        breakdown = list(raw["signal_breakdown"] or [])
        province = _province_name(raw["teryt_wojewodztwo"])
        enriched.append(
            {
                "dzialka_id": str(raw["dzialka_id"]),
                "identyfikator": raw["identyfikator"],
                "parcel_number": raw["parcel_number"],
                "province": province,
                "teryt_gmina": raw["teryt_gmina"],
                "current_buildable_status": raw["current_buildable_status"],
                "confidence_band": raw["confidence_band"],
                "future_signal_score": float(raw["future_signal_score"] or 0),
                "cheapness_score": float(raw["cheapness_score"] or 0),
                "overall_score": float(raw["overall_score"] or 0),
                "dominant_future_signal": raw["dominant_future_signal"],
                "price_per_m2_zl": float(raw["price_per_m2_zl"]) if raw["price_per_m2_zl"] is not None else None,
                "signal_breakdown": breakdown,
                "has_formal_geometry": _has_positive_kind(breakdown, {"pog_zone", "pog_ouz", "studium_zone"}),
                "has_supporting_formal": _has_positive_kind(breakdown, {"planning_resolution", "mpzp_project"}),
                "has_hard_negative": _has_hard_negative(breakdown),
            }
        )

    likely_true_positive = [
        row for row in enriched
        if row["province"] in {"slaskie", "malopolskie"}
        and row["current_buildable_status"] == "non_buildable"
        and not row["has_hard_negative"]
        and (
            row["confidence_band"] in {"formal", "supported"}
            or (row["overall_score"] >= 45 and row["has_supporting_formal"])
        )
    ]
    likely_true_positive.sort(
        key=lambda row: (
            0 if row["confidence_band"] in {"formal", "supported"} else 1,
            -row["overall_score"],
            -row["future_signal_score"],
        )
    )

    tempting_false_positive = [
        row for row in enriched
        if row["province"] in {"slaskie", "malopolskie"}
        and row["current_buildable_status"] == "non_buildable"
        and row["confidence_band"] is None
        and 20 <= row["overall_score"] <= 55
        and row["cheapness_score"] >= 10
        and row["has_supporting_formal"]
        and not row["has_formal_geometry"]
    ]
    tempting_false_positive.sort(
        key=lambda row: (-row["overall_score"], -row["cheapness_score"], row["identyfikator"])
    )

    true_negative = [
        row for row in enriched
        if row["province"] in {"slaskie", "malopolskie"}
        and (
            row["has_hard_negative"]
            or (row["overall_score"] <= 20 and row["future_signal_score"] <= 20)
        )
    ]
    true_negative.sort(
        key=lambda row: (0 if row["has_hard_negative"] else 1, row["overall_score"], row["identyfikator"])
    )

    used_ids: set[str] = set()
    selected_true_positive = _pick_balanced(likely_true_positive, 10)
    used_ids.update(row["dzialka_id"] for row in selected_true_positive)
    selected_true_positive = _extend_with_fallback(
        selected_true_positive,
        [
            row for row in enriched
            if row["province"] in {"slaskie", "malopolskie"}
            and row["current_buildable_status"] == "non_buildable"
            and not row["has_hard_negative"]
        ],
        10,
        used_ids,
    )

    selected_false_positive = _pick_balanced(tempting_false_positive, 10)
    used_ids.update(row["dzialka_id"] for row in selected_false_positive)
    selected_false_positive = _extend_with_fallback(
        selected_false_positive,
        [
            row for row in enriched
            if row["province"] in {"slaskie", "malopolskie"}
            and row["current_buildable_status"] == "non_buildable"
            and row["confidence_band"] is None
            and row["overall_score"] >= 15
        ],
        10,
        used_ids,
    )

    selected_true_negative = _pick_balanced(true_negative, 10)
    used_ids.update(row["dzialka_id"] for row in selected_true_negative)
    selected_true_negative = _extend_with_fallback(
        selected_true_negative,
        [
            row for row in sorted(
                enriched,
                key=lambda item: (
                    0 if item["has_hard_negative"] else 1,
                    item["overall_score"],
                    item["future_signal_score"],
                ),
            )
            if row["province"] in {"slaskie", "malopolskie"}
        ],
        10,
        used_ids,
    )

    payload: list[dict[str, Any]] = []
    for label, rows_for_label in (
        ("true_positive", selected_true_positive),
        ("tempting_false_positive", selected_false_positive),
        ("true_negative", selected_true_negative),
    ):
        for row in rows_for_label:
            payload.append(
                {
                    "dzialka_id": row["dzialka_id"],
                    "identyfikator": row["identyfikator"],
                    "parcel_number": row["parcel_number"],
                    "teryt_gmina": row["teryt_gmina"],
                    "province": row["province"],
                    "label": label,
                    "expected_band": (
                        row["confidence_band"]
                        if row["confidence_band"] is not None
                        else ("supported" if label == "true_positive" else None)
                    ),
                    "notes": "Seed candidate for manual validation/review.",
                    "source_hint": row["dominant_future_signal"],
                    "metrics": {
                        "overall_score": row["overall_score"],
                        "future_signal_score": row["future_signal_score"],
                        "cheapness_score": row["cheapness_score"],
                        "price_per_m2_zl": row["price_per_m2_zl"],
                        "has_formal_geometry": row["has_formal_geometry"],
                        "has_supporting_formal": row["has_supporting_formal"],
                        "has_hard_negative": row["has_hard_negative"],
                    },
                }
            )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    counts = defaultdict(int)
    provinces = defaultdict(int)
    for entry in payload:
        counts[entry["label"]] += 1
        provinces[entry["province"]] += 1

    print("future_buildability_validation_seed")
    print(f"  output: {args.output}")
    print(f"  total: {len(payload)}")
    print(f"  by_label: {dict(counts)}")
    print(f"  by_province: {dict(provinces)}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(_main())
