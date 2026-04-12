from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.core.database import AsyncSessionLocal
from app.services.future_buildability_validation import load_validation_corpus

_ASSESSMENT_QUERY = text(
    """
    SELECT
        fba.dzialka_id,
        fba.confidence_band,
        fba.overall_score,
        fba.future_signal_score,
        fba.cheapness_score
    FROM gold.future_buildability_assessments fba
    """
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print threshold calibration summary for future_buildable")
    parser.add_argument(
        "--corpus-path",
        type=Path,
        default=Path("data/future_buildability_validation_corpus.json"),
        help="Path to the validation corpus JSON",
    )
    return parser.parse_args()


def _promoted(row: dict[str, Any], threshold: float) -> bool:
    overall = float(row.get("overall_score") or 0.0)
    band = row.get("confidence_band")
    return band in {"formal", "supported"} or overall >= threshold


async def _main() -> None:
    args = parse_args()
    corpus = load_validation_corpus(args.corpus_path)
    async with AsyncSessionLocal() as db:
        assessment_rows = (await db.execute(_ASSESSMENT_QUERY)).mappings().all()

    by_id = {str(row["dzialka_id"]): dict(row) for row in assessment_rows}
    matched_entries = [entry for entry in corpus if entry.dzialka_id in by_id]

    print("future_buildability_threshold_calibration")
    print(f"  corpus_path: {args.corpus_path}")
    print(f"  corpus_entries: {len(corpus)}")
    print(f"  matched_assessments: {len(matched_entries)}")

    if not matched_entries:
        print("  note: no matching assessments found")
        return

    for threshold in (40.0, 50.0, 60.0):
        tp = 0
        fp = 0
        fn = 0
        label_counts: dict[str, int] = defaultdict(int)

        for entry in matched_entries:
            row = by_id[entry.dzialka_id]
            promoted = _promoted(row, threshold)
            label_counts[entry.label] += 1
            if entry.label == "true_positive":
                if promoted:
                    tp += 1
                else:
                    fn += 1
            else:
                if promoted:
                    fp += 1

        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        print(f"  threshold={threshold:.0f}")
        print(f"    labels: {dict(label_counts)}")
        print(f"    tp={tp} fp={fp} fn={fn}")
        print(f"    precision={precision:.3f}")
        print(f"    recall={recall:.3f}")


if __name__ == "__main__":
    asyncio.run(_main())
