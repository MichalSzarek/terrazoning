from __future__ import annotations

import argparse
import json
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export an enriched manual review sheet for future-buildability")
    parser.add_argument(
        "--corpus-path",
        type=Path,
        default=Path("data/future_buildability_validation_corpus.json"),
        help="Input validation corpus JSON",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("../docs/future_buildability_manual_review_enriched.md"),
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


def _suggest_label(metrics: dict, confidence_band: str | None) -> tuple[str, str, str]:
    has_formal_geometry = bool(metrics.get("has_formal_geometry"))
    has_supporting_formal = bool(metrics.get("has_supporting_formal"))
    has_hard_negative = bool(metrics.get("has_hard_negative"))
    overall = float(metrics.get("overall_score") or 0.0)
    cheapness = float(metrics.get("cheapness_score") or 0.0)
    summary_parts: list[str] = []

    if has_formal_geometry:
        summary_parts.append("formal geometry")
    if has_supporting_formal:
        summary_parts.append("supporting formal")
    if cheapness >= 10:
        summary_parts.append("cheapness signal")
    if has_hard_negative:
        summary_parts.append("hard negative")
    if not summary_parts:
        summary_parts.append("weak signal mix")

    if has_hard_negative:
        return "true_negative", "none", "Hard negative signal present; deprioritize."
    if confidence_band == "formal":
        return "true_positive", "formal", "Formal signal supports future buildability."
    if has_formal_geometry and overall >= 55:
        return "true_positive", "supported", "Strong geometry-backed signal just below formal threshold."
    if has_supporting_formal and cheapness >= 10 and overall >= 20:
        return "tempting_false_positive", "none", "Directional signal + cheapness but lacks strong formal geometry."
    return "true_negative", "none", "Insufficient evidence for future buildability."


def _main() -> None:
    args = _parse_args()
    if not args.corpus_path.exists():
        raise SystemExit(f"Corpus file not found: {args.corpus_path}")

    payload = json.loads(args.corpus_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise SystemExit("Validation corpus must be a JSON array")

    lines: list[str] = []
    lines.append("# Future Buildability — Manual Review Sheet (Enriched)")
    lines.append("")
    lines.append("Fill in `review_label` and `review_expected_band` after inspecting each case.")
    lines.append("Suggested columns are heuristics only and must be confirmed by a human.")
    lines.append("")
    lines.append("| # | identyfikator | province | teryt_gmina | seed_label | seed_band | overall | future_signal | cheapness | formal_geom | supporting_formal | hard_negative | dominant_signal | suggested_label | suggested_band | suggested_summary | review_label | review_expected_band | notes |")
    lines.append("| - | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - |")

    for idx, row in enumerate(payload, start=1):
        metrics = row.get("metrics", {}) if isinstance(row.get("metrics"), dict) else {}
        confidence_band = row.get("expected_band") or row.get("confidence_band")
        suggested_label, suggested_band, suggested_summary = _suggest_label(metrics, confidence_band)
        lines.append(
            "| {idx} | {identyfikator} | {province} | {teryt_gmina} | {label_seed} | {expected_band_seed} | {overall} | {future_signal} | {cheapness} | {formal_geom} | {supporting_formal} | {hard_negative} | {dominant} | {suggested_label} | {suggested_band} | {suggested_summary} |  |  |  |".format(
                idx=idx,
                identyfikator=_row_value(row, "identyfikator"),
                province=_row_value(row, "province"),
                teryt_gmina=_row_value(row, "teryt_gmina"),
                label_seed=_row_value(row, "label"),
                expected_band_seed=_row_value(row, "expected_band"),
                overall=_row_value(metrics, "overall_score"),
                future_signal=_row_value(metrics, "future_signal_score"),
                cheapness=_row_value(metrics, "cheapness_score"),
                formal_geom=_row_value(metrics, "has_formal_geometry"),
                supporting_formal=_row_value(metrics, "has_supporting_formal"),
                hard_negative=_row_value(metrics, "has_hard_negative"),
                dominant=_row_value(row, "source_hint"),
                suggested_label=suggested_label,
                suggested_band=suggested_band,
                suggested_summary=suggested_summary,
            )
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(lines), encoding="utf-8")

    print("future_buildability_review_sheet_enriched")
    print(f"  corpus: {args.corpus_path}")
    print(f"  output: {args.output}")
    print(f"  rows: {len(payload)}")


if __name__ == "__main__":
    _main()
