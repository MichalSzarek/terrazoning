from __future__ import annotations

import argparse
import json
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a manual review sheet for future-buildability")
    parser.add_argument(
        "--corpus-path",
        type=Path,
        default=Path("data/future_buildability_validation_corpus.json"),
        help="Input validation corpus JSON",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("../docs/future_buildability_manual_review.md"),
        help="Output markdown review sheet",
    )
    return parser.parse_args()


def _row_value(row: dict, key: str) -> str:
    value = row.get(key)
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _main() -> None:
    args = _parse_args()
    if not args.corpus_path.exists():
        raise SystemExit(f"Corpus file not found: {args.corpus_path}")

    payload = json.loads(args.corpus_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise SystemExit("Validation corpus must be a JSON array")

    lines: list[str] = []
    lines.append("# Future Buildability — Manual Review Sheet")
    lines.append("")
    lines.append("Fill in `review_label` and `review_expected_band` after inspecting each case.")
    lines.append("Valid labels: `true_positive`, `tempting_false_positive`, `true_negative`.")
    lines.append("Valid bands: `formal`, `supported`, or `none`.")
    lines.append("")
    lines.append("| # | identyfikator | province | teryt_gmina | label_seed | expected_band_seed | overall | future_signal | cheapness | dominant_future_signal | review_label | review_expected_band | notes |")
    lines.append("| - | - | - | - | - | - | - | - | - | - | - | - | - |")

    for idx, row in enumerate(payload, start=1):
        metrics = row.get("metrics", {}) if isinstance(row.get("metrics"), dict) else {}
        lines.append(
            "| {idx} | {identyfikator} | {province} | {teryt_gmina} | {label_seed} | {expected_band_seed} | {overall} | {future_signal} | {cheapness} | {dominant} |  |  |  |".format(
                idx=idx,
                identyfikator=_row_value(row, "identyfikator"),
                province=_row_value(row, "province"),
                teryt_gmina=_row_value(row, "teryt_gmina"),
                label_seed=_row_value(row, "label"),
                expected_band_seed=_row_value(row, "expected_band"),
                overall=_row_value(metrics, "overall_score"),
                future_signal=_row_value(metrics, "future_signal_score"),
                cheapness=_row_value(metrics, "cheapness_score"),
                dominant=_row_value(row, "source_hint"),
            )
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(lines), encoding="utf-8")

    print("future_buildability_review_sheet")
    print(f"  corpus: {args.corpus_path}")
    print(f"  output: {args.output}")
    print(f"  rows: {len(payload)}")


if __name__ == "__main__":
    _main()
