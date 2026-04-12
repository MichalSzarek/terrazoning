from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from app.services.future_buildability_validation import (
    build_validation_corpus_report,
    load_validation_corpus,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print future-buildability validation corpus status")
    parser.add_argument(
        "--corpus-path",
        type=Path,
        default=Path("data/future_buildability_validation_corpus.json"),
        help="Path to a JSON validation corpus",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format",
    )
    return parser.parse_args(argv)


def _main() -> None:
    args = parse_args()
    entries = load_validation_corpus(args.corpus_path)
    report = build_validation_corpus_report(entries)

    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return

    summary = report["summary"]
    print("future_buildability_validation_report")
    print(f"  corpus_path: {args.corpus_path}")
    print(f"  total: {summary['total']}")
    print(f"  by_label: {summary['by_label']}")
    print(f"  by_province: {summary['by_province']}")
    print(f"  missing_expected_band: {summary['missing_expected_band']}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s | %(message)s")
    _main()
