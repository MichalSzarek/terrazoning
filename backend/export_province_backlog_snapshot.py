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

from app.services.operations_scope import province_display_name, province_teryt_prefix, provinces
from export_future_buildability_backlog import _build_export_payload as _build_future_backlog_payload
from print_future_buildability_status import _format_timestamp
from run_province_campaign import _delta_gap_snapshot, _status_snapshot


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export one operator snapshot for a province backlog")
    parser.add_argument("--province", required=True, choices=provinces())
    parser.add_argument("--format", choices=("csv", "json"), default="csv")
    parser.add_argument("--output", type=Path, help="Optional output path; defaults to stdout")
    parser.add_argument("--parallel", action="store_true", help="Gather province report queries in parallel")
    return parser.parse_args(argv)


async def _build_snapshot(*, province: str, parallel: bool) -> dict[str, Any]:
    before = await _status_snapshot(province, parallel=parallel)
    delta_gap = await _delta_gap_snapshot(province)
    province_prefix = province_teryt_prefix(province)
    future_backlog = await _build_future_backlog_payload(province_prefix=province_prefix)

    coverage_rows: list[dict[str, Any]] = []
    for row in delta_gap.backlog_hints:
        teryt = row.get("teryt") or row.get("teryt_gmina") or row.get("covered_via") or ""
        parcel_count = row.get("dzialki_count")
        if parcel_count in (None, ""):
            parcel_count = row.get("parcel_count")
        if parcel_count in (None, ""):
            parcel_count = row.get("dzialki")
        planning_zones = row.get("planning_zones", "")
        delta_rows = row.get("delta_rows", "")
        coverage_rows.append(
            {
                "backlog_family": "coverage_gap",
                "teryt_gmina": teryt,
                "bucket": row.get("coverage_category") or row.get("backlog_status"),
                "why_no_lead": row.get("why_no_lead", ""),
                "parcel_count": parcel_count or 0,
                "planning_zones": planning_zones,
                "delta_rows": delta_rows,
                "max_overall_score": row.get("max_overall_score"),
                "known_sources": "",
                "next_best_source_type": "",
                "operator_status": "",
                "covered_via": row.get("covered_via", ""),
                "localities": row.get("localities", ""),
                "next_action": row.get("next_action") or row.get("operator_hint") or "",
                "sample_url": row.get("sample_url", ""),
                "last_assessment_at": "",
                "last_source_sync_at": "",
            }
        )

    future_rows: list[dict[str, Any]] = []
    for row in future_backlog["rows"]:
        future_rows.append(
            {
                "backlog_family": "future_signal",
                "teryt_gmina": row["teryt_gmina"],
                "bucket": row["operator_status"],
                "why_no_lead": "",
                "parcel_count": row["parcel_count"],
                "planning_zones": "",
                "delta_rows": "",
                "max_overall_score": row["max_overall_score"],
                "known_sources": ",".join(row["known_sources"]),
                "next_best_source_type": row["next_best_source_type"],
                "operator_status": row["operator_status"],
                "covered_via": "",
                "localities": "",
                "next_action": row.get("html_index_error") or "",
                "sample_url": "",
                "last_assessment_at": row["last_assessment_at"],
                "last_source_sync_at": row["last_source_sync_at"],
            }
        )

    return {
        "generated_at": _format_timestamp(datetime.now(timezone.utc)),
        "scope": {
            "province": province,
            "province_label": province_display_name(province) or province,
            "province_prefix": province_prefix,
        },
        "counts": {
            "bronze": before.bronze_listings,
            "pending": before.bronze_pending,
            "silver": before.silver_dzialki,
            "dlq": before.silver_dlq,
            "planning_zones": before.gold_planning_zones,
            "leads": before.gold_investment_leads,
            "manual_backlog_count": before.manual_backlog_count,
            "coverage_gap_rows": len(coverage_rows),
            "future_backlog_rows": len(future_rows),
        },
        "rows": coverage_rows + future_rows,
    }


def _write_csv(rows: list[dict[str, Any]], stream) -> None:
    fieldnames = [
        "backlog_family",
        "teryt_gmina",
        "bucket",
        "why_no_lead",
        "parcel_count",
        "planning_zones",
        "delta_rows",
        "max_overall_score",
        "known_sources",
        "next_best_source_type",
        "operator_status",
        "covered_via",
        "localities",
        "next_action",
        "sample_url",
        "last_assessment_at",
        "last_source_sync_at",
    ]
    writer = csv.DictWriter(stream, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)


async def _main() -> None:
    args = parse_args()
    payload = await _build_snapshot(province=args.province, parallel=args.parallel)
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
