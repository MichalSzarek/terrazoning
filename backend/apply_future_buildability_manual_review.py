from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from sqlalchemy import text

from app.core.database import AsyncSessionLocal


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply manual review labels to the validation corpus")
    parser.add_argument(
        "--review-sheet",
        type=Path,
        default=Path("../docs/future_buildability_manual_review.md"),
        help="Markdown review sheet with review_label/review_expected_band columns filled in",
    )
    parser.add_argument(
        "--corpus-path",
        type=Path,
        default=Path("data/future_buildability_validation_corpus.json"),
        help="Validation corpus JSON to update",
    )
    parser.add_argument(
        "--append-missing",
        action="store_true",
        help="Append rows that are missing in the corpus but present in the review sheet",
    )
    return parser.parse_args()


def _parse_review_rows(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    header_map: dict[str, int] | None = None
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        parts = [part.strip() for part in line.strip("|").split("|")]
        if len(parts) < 6:
            continue
        if parts[0] == "#":
            header_map = {name: idx for idx, name in enumerate(parts)}
            continue
        if parts[0] == "-":
            continue
        if header_map:
            try:
                ident_idx = header_map["identyfikator"]
                label_idx = header_map.get("review_label")
                band_idx = header_map.get("review_expected_band")
                notes_idx = header_map.get("notes")
                rows.append(
                    {
                        "identyfikator": parts[ident_idx],
                        "review_label": parts[label_idx] if label_idx is not None else "",
                        "review_expected_band": parts[band_idx] if band_idx is not None else "",
                        "review_notes": parts[notes_idx] if notes_idx is not None else "",
                    }
                )
            except KeyError:
                continue
            continue
        if len(parts) >= 13:
            rows.append(
                {
                    "identyfikator": parts[1],
                    "review_label": parts[10],
                    "review_expected_band": parts[11],
                    "review_notes": parts[12],
                }
            )
    return rows


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
    WHERE d.identyfikator = :ident
    ORDER BY fba.overall_score DESC NULLS LAST
    LIMIT 1
    """
)


async def _fetch_assessment_rows(idents: list[str]) -> dict[str, dict]:
    if not idents:
        return {}
    results: dict[str, dict] = {}
    async with AsyncSessionLocal() as db:
        for ident in idents:
            row = (await db.execute(_ASSESSMENT_QUERY, {"ident": ident})).mappings().first()
            if row:
                results[ident] = dict(row)
    return results


def _main() -> None:
    args = _parse_args()
    if not args.review_sheet.exists():
        raise SystemExit(f"Review sheet not found: {args.review_sheet}")
    if not args.corpus_path.exists():
        raise SystemExit(f"Corpus not found: {args.corpus_path}")

    review_rows = _parse_review_rows(args.review_sheet.read_text(encoding="utf-8"))
    corpus = json.loads(args.corpus_path.read_text(encoding="utf-8"))
    if not isinstance(corpus, list):
        raise SystemExit("Corpus JSON must be an array")

    by_ident: dict[str, list[dict]] = {}
    for entry in corpus:
        ident = str(entry.get("identyfikator") or "")
        if not ident:
            continue
        by_ident.setdefault(ident, []).append(entry)
    updated = 0
    appended = 0
    skipped = 0

    missing_idents: list[str] = []
    review_map: dict[str, dict[str, str]] = {}
    for row in review_rows:
        ident = row["identyfikator"]
        review_map[ident] = row
        label = row["review_label"].strip()
        band = row["review_expected_band"].strip()
        notes = row["review_notes"].strip()
        if not label and not band and not notes:
            skipped += 1
            continue
        entries = by_ident.get(ident)
        if not entries:
            if args.append_missing:
                missing_idents.append(ident)
            else:
                skipped += 1
            continue
        for entry in entries:
            if label:
                entry["label"] = label
            if band:
                entry["expected_band"] = None if band.lower() == "none" else band
            if notes:
                entry["notes"] = notes
        updated += len(entries)

    if args.append_missing and missing_idents:
        assessments = asyncio.run(_fetch_assessment_rows(sorted(set(missing_idents))))
        for ident, assessment in assessments.items():
            review_row = review_map.get(ident, {})
            label = (review_row.get("review_label") or "").strip()
            band = (review_row.get("review_expected_band") or "").strip()
            notes = (review_row.get("review_notes") or "").strip()
            entry = {
                "dzialka_id": str(assessment["dzialka_id"]),
                "identyfikator": assessment["identyfikator"],
                "parcel_number": assessment["parcel_number"],
                "teryt_gmina": assessment["teryt_gmina"],
                "province": str(assessment["teryt_wojewodztwo"] or "").strip(),
                "label": label or None,
                "expected_band": None if not band or band.lower() == "none" else band,
                "notes": notes or assessment.get("notes") or "Manual review appended from DB.",
                "source_hint": assessment.get("dominant_future_signal"),
                "metrics": {
                    "overall_score": float(assessment.get("overall_score") or 0),
                    "future_signal_score": float(assessment.get("future_signal_score") or 0),
                    "cheapness_score": float(assessment.get("cheapness_score") or 0),
                    "price_per_m2_zl": float(assessment.get("price_per_m2_zl") or 0)
                    if assessment.get("price_per_m2_zl") is not None
                    else None,
                    "has_formal_geometry": any(
                        (item.get("kind") in {"pog_zone", "pog_ouz", "studium_zone"})
                        and float(item.get("weight") or 0) > 0
                        for item in (assessment.get("signal_breakdown") or [])
                    ),
                    "has_supporting_formal": any(
                        (item.get("kind") in {"planning_resolution", "mpzp_project"})
                        and float(item.get("weight") or 0) > 0
                        for item in (assessment.get("signal_breakdown") or [])
                    ),
                    "has_hard_negative": any(
                        (item.get("designation_normalized") or "") in {"forest", "water", "green", "cemetery", "infrastructure", "industrial"}
                        and float(item.get("weight") or 0) < 0
                        for item in (assessment.get("signal_breakdown") or [])
                    ),
                },
            }
            corpus.append(entry)
            by_ident.setdefault(ident, []).append(entry)
            appended += 1

    args.corpus_path.write_text(json.dumps(corpus, ensure_ascii=False, indent=2), encoding="utf-8")
    print("future_buildability_manual_review_apply")
    print(f"  review_sheet: {args.review_sheet}")
    print(f"  corpus_path: {args.corpus_path}")
    print(f"  updated: {updated}")
    print(f"  appended: {appended}")
    print(f"  skipped: {skipped}")


if __name__ == "__main__":
    _main()
