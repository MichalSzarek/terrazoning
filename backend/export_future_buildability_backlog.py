from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.config import settings
from print_future_buildability_status import (
    _format_timestamp,
    _load_backlog_rows,
    _load_freshness_snapshot,
)
from app.services.operations_scope import province_display_name, province_teryt_prefix, provinces


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export future-buildability source discovery backlog")
    parser.add_argument(
        "--province",
        choices=provinces(),
        help="Optional province scope for the exported backlog",
    )
    parser.add_argument(
        "--format",
        choices=("csv", "json"),
        default="csv",
        help="Export format",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional output path; defaults to stdout",
    )
    return parser.parse_args(argv)


async def _build_export_payload(*, province_prefix: str | None) -> dict[str, Any]:
    rows = await _load_backlog_rows(province_prefix=province_prefix)
    freshness = await _load_freshness_snapshot(province_prefix=province_prefix)
    return {
        "generated_at": _format_timestamp(datetime.now(timezone.utc)),
        "scope": province_prefix,
        "freshness": {
            "planning_signals_last_successful_sync_at": _format_timestamp(
                freshness["planning_signals"].get("last_successful_sync_at")
            ),
            "future_buildability_last_successful_sync_at": _format_timestamp(
                freshness["future_buildability"].get("last_successful_sync_at")
            ),
        },
        "rows": rows,
    }


def _write_csv(rows: list[dict[str, Any]], stream) -> None:
    fieldnames = [
        "teryt_gmina",
        "parcel_count",
        "max_overall_score",
        "known_sources",
        "next_best_source_type",
        "operator_status",
        "html_index_status",
        "html_index_error",
        "last_assessment_at",
        "last_source_sync_at",
    ]
    writer = csv.DictWriter(stream, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                "teryt_gmina": row["teryt_gmina"],
                "parcel_count": row["parcel_count"],
                "max_overall_score": f"{row['max_overall_score']:.0f}",
                "known_sources": ",".join(row["known_sources"]),
                "next_best_source_type": row["next_best_source_type"],
                "operator_status": row["operator_status"],
                "html_index_status": row["html_index_status"] or "",
                "html_index_error": row["html_index_error"] or "",
                "last_assessment_at": row["last_assessment_at"],
                "last_source_sync_at": row["last_source_sync_at"],
            }
        )


async def _main() -> None:
    if not settings.future_buildability_enabled:
        print("future_buildability_backlog feature=disabled")
        return

    args = parse_args()
    province_prefix = province_teryt_prefix(args.province) if args.province else None
    province_label = province_display_name(args.province) if args.province else "Wszystkie skonfigurowane województwa"
    payload = await _build_export_payload(province_prefix=province_prefix)
    payload["scope"] = {
        "province": args.province,
        "province_label": province_label,
        "province_prefix": province_prefix,
    }

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        if args.format == "json":
            args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        else:
            with args.output.open("w", encoding="utf-8", newline="") as handle:
                _write_csv(payload["rows"], handle)
    else:
        if args.format == "json":
            json.dump(payload, sys.stdout, ensure_ascii=False, indent=2)
            sys.stdout.write("\n")
        else:
            _write_csv(payload["rows"], sys.stdout)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    asyncio.run(_main())
