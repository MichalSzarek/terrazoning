from __future__ import annotations

import argparse
import asyncio
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import bindparam, text

from app.core.database import AsyncSessionLocal
from app.services.operations_scope import province_display_name, province_teryt_prefix, provinces

_MISSING_ROWS_QUERY = text(
    """
    SELECT
        identyfikator,
        teryt_gmina,
        numer_dzialki,
        COALESCE(NULLIF(BTRIM(current_use), ''), '') AS current_use
    FROM silver.dzialki
    WHERE NULLIF(BTRIM(current_use), '') IS NULL
      AND (
        CAST(:province_prefix AS text) IS NULL
        OR substr(teryt_gmina, 1, 2) = CAST(:province_prefix AS text)
      )
    ORDER BY teryt_gmina, identyfikator
    """
)

_LOOKUP_EXISTING_QUERY = text(
    """
    SELECT identyfikator, COALESCE(NULLIF(BTRIM(current_use), ''), '') AS current_use
    FROM silver.dzialki
    WHERE identyfikator IN :identifiers
    """
).bindparams(bindparam("identifiers", expanding=True))

_UPDATE_QUERY = text(
    """
    UPDATE silver.dzialki
    SET
        current_use = :current_use,
        updated_at = NOW()
    WHERE identyfikator = :identyfikator
      AND (:overwrite OR NULLIF(BTRIM(current_use), '') IS NULL)
    """
)

_LISTING_HEURISTIC_QUERY = text(
    """
    SELECT
        d.identyfikator,
        d.teryt_gmina,
        rl.source_type,
        COALESCE(rl.title, '') AS title,
        COALESCE(rl.raw_text, '') AS raw_text
    FROM silver.dzialki d
    JOIN silver.listing_parcels lp ON lp.dzialka_id = d.id
    JOIN bronze.raw_listings rl ON rl.id = lp.listing_id
    WHERE (
        CAST(:province_prefix AS text) IS NULL
        OR substr(d.teryt_gmina, 1, 2) = CAST(:province_prefix AS text)
    )
    ORDER BY d.identyfikator, lp.created_at DESC
    """
)


@dataclass(frozen=True)
class CurrentUseRow:
    identyfikator: str
    current_use: str


_HEURISTIC_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("LS", (" las", "leśn", "drzewost", "zales", "lasek")),
    ("Ł", ("łąk", "laka", "łąka")),
    ("PS", ("pastw",)),
    ("S", (" sad", "sady", "sadown",)),
    ("N", ("nieużyt", "nieuzyt")),
    ("BP", ("zurbanizowane niezabudowane", "teren budowlany niezabudowany")),
    ("R", ("grunty orne", "grunt roln", "rola", "nieruchomość gruntowa niezabudowana", "nieruchomosc gruntowa niezabudowana", "działka niezabudowana", "dzialka niezabudowana", "grunt niezabudowany")),
    ("B", ("nieruchomość zabudowana", "nieruchomosc zabudowana", "budynkiem", "budynkiem mieszkalnym", "budynek mieszkalny", "dom mieszkal", "lokal mieszkal", "garaż", "garaz", "kamienic")),
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill silver.dzialki.current_use from a reviewed CSV file")
    parser.add_argument(
        "--province",
        choices=provinces(),
        help="Optional province scope",
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="CSV with columns identyfikator,current_use",
    )
    parser.add_argument(
        "--export-template",
        type=Path,
        help="Write a CSV template of parcels still missing current_use",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit for export-template",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist updates; default mode is dry-run only",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Also replace non-empty current_use values; default updates only NULL/blank rows",
    )
    parser.add_argument(
        "--infer-from-listings",
        action="store_true",
        help="Build current_use rows from deterministic auction-text heuristics",
    )
    return parser.parse_args(argv)


def _normalize_current_use(value: str) -> str:
    normalized = value.strip().upper()
    if not normalized:
        raise ValueError("current_use cannot be empty")
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/.-ŁŚĆŃÓŻŹĘĄ")
    if any(char not in allowed for char in normalized):
        raise ValueError(f"current_use contains unsupported characters: {value!r}")
    return normalized


def _infer_current_use_from_text(*, title: str, raw_text: str) -> str:
    text = re.sub(r"\s+", " ", f"{title} {raw_text}".strip().lower())
    for code, needles in _HEURISTIC_RULES:
        if any(needle in text for needle in needles):
            return code
    # Conservative fallback for vacant land auctions: treat unknown, non-built
    # land as arable/rural until a better EGiB source is available.
    return "R"


def _load_rows_from_csv(path: Path) -> list[CurrentUseRow]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("CSV file is missing a header row")
        expected = {"identyfikator", "current_use"}
        missing = expected.difference(reader.fieldnames)
        if missing:
            raise ValueError(f"CSV is missing required columns: {', '.join(sorted(missing))}")

        rows: list[CurrentUseRow] = []
        seen: set[str] = set()
        for index, row in enumerate(reader, start=2):
            ident = (row.get("identyfikator") or "").strip()
            if not ident:
                raise ValueError(f"Row {index}: identyfikator cannot be empty")
            if ident in seen:
                raise ValueError(f"Row {index}: duplicate identyfikator {ident}")
            seen.add(ident)
            rows.append(
                CurrentUseRow(
                    identyfikator=ident,
                    current_use=_normalize_current_use(row.get("current_use") or ""),
                )
            )
        return rows


async def _export_template(*, province_prefix: str | None, output: Path, limit: int | None) -> int:
    async with AsyncSessionLocal() as db:
        rows = [
            dict(row)
            for row in (
                await db.execute(_MISSING_ROWS_QUERY, {"province_prefix": province_prefix})
            ).mappings().all()
        ]

    if limit is not None:
        rows = rows[:limit]

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["identyfikator", "current_use", "teryt_gmina", "numer_dzialki"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return len(rows)


async def _run_backfill(
    *,
    rows: list[CurrentUseRow],
    overwrite: bool,
    apply: bool,
) -> dict[str, Any]:
    identifiers = [row.identyfikator for row in rows]
    if not identifiers:
        return {
            "requested": 0,
            "eligible_updates": 0,
            "applied_updates": 0,
            "skipped_existing": 0,
            "missing_in_db": [],
        }
    async with AsyncSessionLocal() as db:
        existing = {
            row["identyfikator"]: row["current_use"]
            for row in (
                await db.execute(_LOOKUP_EXISTING_QUERY, {"identifiers": identifiers})
            ).mappings().all()
        }

        missing_in_db = [ident for ident in identifiers if ident not in existing]
        updates: list[CurrentUseRow] = []
        skipped_existing = 0
        for row in rows:
            current = existing.get(row.identyfikator, "")
            if not overwrite and current:
                skipped_existing += 1
                continue
            updates.append(row)

        applied = 0
        if apply:
            for row in updates:
                result = await db.execute(
                    _UPDATE_QUERY,
                    {
                        "identyfikator": row.identyfikator,
                        "current_use": row.current_use,
                        "overwrite": overwrite,
                    },
                )
                applied += int(result.rowcount or 0)
            await db.commit()

    return {
        "requested": len(rows),
        "eligible_updates": len(updates),
        "applied_updates": applied,
        "skipped_existing": skipped_existing,
        "missing_in_db": missing_in_db,
    }


async def _infer_rows_from_listings(*, province_prefix: str | None, overwrite: bool) -> list[CurrentUseRow]:
    async with AsyncSessionLocal() as db:
        listing_rows = [
            dict(row)
            for row in (
                await db.execute(_LISTING_HEURISTIC_QUERY, {"province_prefix": province_prefix})
            ).mappings().all()
        ]
        existing = {}
        identifiers = sorted({row["identyfikator"] for row in listing_rows})
        if identifiers:
            existing = {
                row["identyfikator"]: row["current_use"]
                for row in (
                    await db.execute(_LOOKUP_EXISTING_QUERY, {"identifiers": identifiers})
                ).mappings().all()
            }

    rows_by_ident: dict[str, CurrentUseRow] = {}
    for row in listing_rows:
        ident = row["identyfikator"]
        if ident in rows_by_ident:
            continue
        current = existing.get(ident, "")
        if current and not overwrite:
            continue
        inferred = _infer_current_use_from_text(
            title=str(row.get("title") or ""),
            raw_text=str(row.get("raw_text") or ""),
        )
        rows_by_ident[ident] = CurrentUseRow(
            identyfikator=ident,
            current_use=inferred,
        )
    return list(rows_by_ident.values())


async def _main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    province_prefix = province_teryt_prefix(args.province) if args.province else None
    province_label = province_display_name(args.province) if args.province else "all provinces"

    if args.export_template:
        exported = await _export_template(
            province_prefix=province_prefix,
            output=args.export_template,
            limit=args.limit,
        )
        print(
            f"current_use_template_exported rows={exported} scope={province_label} "
            f"path={args.export_template}"
        )

    if args.input:
        rows = _load_rows_from_csv(args.input)
        report = await _run_backfill(rows=rows, overwrite=args.overwrite, apply=args.apply)
        mode = "apply" if args.apply else "dry-run"
        print(f"current_use_backfill mode={mode} scope={province_label}")
        print(f"  requested         : {report['requested']}")
        print(f"  eligible_updates  : {report['eligible_updates']}")
        print(f"  skipped_existing  : {report['skipped_existing']}")
        print(f"  applied_updates   : {report['applied_updates']}")
        if report["missing_in_db"]:
            print(f"  missing_in_db     : {len(report['missing_in_db'])}")
            for ident in report["missing_in_db"][:10]:
                print(f"    - {ident}")

    if args.infer_from_listings:
        rows = await _infer_rows_from_listings(
            province_prefix=province_prefix,
            overwrite=args.overwrite,
        )
        report = await _run_backfill(rows=rows, overwrite=args.overwrite, apply=args.apply)
        mode = "apply" if args.apply else "dry-run"
        print(f"current_use_heuristic_backfill mode={mode} scope={province_label}")
        print("  heuristic_provider : listing_text_fallback")
        print(f"  requested         : {report['requested']}")
        print(f"  eligible_updates  : {report['eligible_updates']}")
        print(f"  skipped_existing  : {report['skipped_existing']}")
        print(f"  applied_updates   : {report['applied_updates']}")

    if not args.export_template and not args.input and not args.infer_from_listings:
        print("current_use_backfill requires --export-template, --input, and/or --infer-from-listings")


if __name__ == "__main__":
    asyncio.run(_main())
