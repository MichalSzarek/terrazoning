from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.core.database import AsyncSessionLocal
from app.services.future_buildability_validation import load_validation_corpus

_ASSESSMENT_QUERY = text(
    """
    SELECT
        fba.dzialka_id,
        d.identyfikator,
        d.teryt_gmina,
        fba.current_buildable_status,
        fba.confidence_band,
        fba.future_signal_score,
        fba.cheapness_score,
        fba.overall_score,
        fba.dominant_future_signal,
        fba.signal_breakdown,
        fba.evidence_chain
    FROM gold.future_buildability_assessments fba
    JOIN silver.dzialki d ON d.id = fba.dzialka_id
    """
)

_HARD_NEGATIVE = {"forest", "water", "green", "cemetery", "infrastructure"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print red-team review for future_buildable")
    parser.add_argument(
        "--corpus-path",
        type=Path,
        default=Path("data/future_buildability_validation_corpus.json"),
        help="Validation corpus path",
    )
    parser.add_argument(
        "--markdown-out",
        type=Path,
        default=Path("../docs/audit/2026-04-10-future-buildability-red-team-report.md"),
        help="Optional markdown report output path",
    )
    return parser.parse_args()


def _has_hard_negative(breakdown: list[dict[str, Any]]) -> bool:
    return any(
        (item.get("designation_normalized") or "") in _HARD_NEGATIVE
        and float(item.get("weight") or 0.0) < 0
        for item in breakdown
    )


def _has_supporting_only(breakdown: list[dict[str, Any]]) -> bool:
    has_geometry = any(
        item.get("kind") in {"pog_zone", "pog_ouz", "studium_zone"} and float(item.get("weight") or 0.0) > 0
        for item in breakdown
    )
    has_supporting = any(
        item.get("kind") in {"planning_resolution", "mpzp_project"} and float(item.get("weight") or 0.0) > 0
        for item in breakdown
    )
    return has_supporting and not has_geometry


def _find_sections(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    def dedupe(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[str] = set()
        unique: list[dict[str, Any]] = []
        for row in items:
            key = str(row["dzialka_id"])
            if key in seen:
                continue
            seen.add(key)
            unique.append(row)
        return unique

    return {
        "forest_green_false_positives": dedupe([
            row for row in rows
            if _has_hard_negative(row["signal_breakdown"])
            and (row["confidence_band"] in {"formal", "supported"} or float(row["overall_score"] or 0) >= 40)
        ])[:10],
        "cheap_but_unjustified_false_positives": dedupe([
            row for row in rows
            if float(row["cheapness_score"] or 0) >= 20
            and float(row["future_signal_score"] or 0) <= 20
            and row["confidence_band"] is None
        ])[:10],
        "preparatory_document_overweighting": dedupe([
            row for row in rows
            if _has_supporting_only(row["signal_breakdown"])
            and float(row["overall_score"] or 0) >= 40
        ])[:10],
        "stale_or_invalid_source_promotion": dedupe([
            row for row in rows
            if any(
                "rejestrplanowogolnych.pl" in str((item.get("source_url") or ""))
                or "Błąd połączenia" in json.dumps(item, ensure_ascii=False)
                for item in row["signal_breakdown"]
            )
            and row["confidence_band"] in {"formal", "supported"}
        ])[:10],
    }


def _row_to_markdown(row: dict[str, Any]) -> str:
    return (
        f"- `{row['identyfikator']}` | gmina `{row['teryt_gmina']}` | "
        f"band `{row['confidence_band']}` | overall `{row['overall_score']}` | "
        f"signal `{row['future_signal_score']}` | cheapness `{row['cheapness_score']}` | "
        f"dominant `{row['dominant_future_signal'] or '-'}`"
    )


async def _main() -> None:
    args = parse_args()
    corpus = load_validation_corpus(args.corpus_path)
    async with AsyncSessionLocal() as db:
        rows = (await db.execute(_ASSESSMENT_QUERY)).mappings().all()

    assessment_by_id = {str(row["dzialka_id"]): dict(row) for row in rows}
    scoped_rows = [
        {
            **assessment_by_id[entry.dzialka_id],
            "label": entry.label,
            "province": entry.province,
        }
        for entry in corpus
        if entry.dzialka_id in assessment_by_id
    ]

    sections = _find_sections(scoped_rows)

    print("future_buildability_red_team_report")
    print(f"  corpus_path: {args.corpus_path}")
    print(f"  matched_rows: {len(scoped_rows)}")
    for name, items in sections.items():
        print(f"  {name}: {len(items)}")
        for row in items[:5]:
            print(
                "   - "
                f"{row['identyfikator']} | band={row['confidence_band']} | "
                f"overall={row['overall_score']} | dominant={row['dominant_future_signal'] or '-'}"
            )

    markdown_lines = [
        "# Future Buildability Red-Team Report",
        "",
        f"- corpus_path: `{args.corpus_path}`",
        f"- matched_rows: `{len(scoped_rows)}`",
        "",
    ]
    titles = {
        "forest_green_false_positives": "Forest / Green False Positives",
        "cheap_but_unjustified_false_positives": "Cheap But Unjustified False Positives",
        "preparatory_document_overweighting": "Preparatory Document Over-Weighting",
        "stale_or_invalid_source_promotion": "Stale Or Invalid Source Promotion",
    }
    for key, title in titles.items():
        markdown_lines.append(f"## {title}")
        items = sections[key]
        if not items:
            markdown_lines.append("- none flagged in the current corpus")
        else:
            markdown_lines.extend(_row_to_markdown(row) for row in items)
        markdown_lines.append("")

    args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_out.write_text("\n".join(markdown_lines), encoding="utf-8")


if __name__ == "__main__":
    asyncio.run(_main())
