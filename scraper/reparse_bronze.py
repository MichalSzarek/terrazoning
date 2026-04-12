"""One-off Bronze reparsing script for improved parcel/locality extraction.

Usage:
    uv run python reparse_bronze.py
    uv run python reparse_bronze.py --dry-run --verbose
"""

from __future__ import annotations

import argparse
import asyncio
from difflib import SequenceMatcher
import logging
from dataclasses import dataclass
import re

from sqlalchemy import delete, func, select

from app.core.database import AsyncSessionLocal
from app.models.bronze import RawListing
from app.models.silver import DlqParcel
from scraper.extractors.llm_extractor import LLMExtractor, extract_with_fallback
from scraper.extractors.parcel import ParcelMatch, extract_obreb

logger = logging.getLogger("reparse_bronze")

_SUSPECT_OBREB = (
    "objet",
    "objęt",
    "skladaj",
    "składaj",
    "obejmuj",
    "wraz",
    "tereny",
    "drodze",
    "licytacji",
    "elektronicz",
    "siec",
    "sieć",
    "budowie",
    "bezpo",
    "bezps",
    "ksied",
    "księd",
    "wojewodzt",
    "województ",
    "przeznacz",
    # Court/legal text snippets that get concatenated into obreb
    "rejonow",        # "sądzie rejonowym"
    "urządzon",       # "urządzona jest"
    "numer",          # "onumerze", "numerze"
    "rejonie",        # "w rejonie"
    "sadzie",         # "sądzie" (ASCII fallback)
    "sądzie",
    "uzytkowan",      # "użytkowanie"
    "użytkowan",
    "wieczyst",       # "wieczysta", "wieczystej"
    "katastral",
    "wpisana",
    "wsadzie",        # "w sądzie" concatenated
    "wrejonie",       # "w rejonie" concatenated
    # Street names that should never survive as cadastral localities
    "bartnicz",
    "graniczn",
    "goscinn",
    "gościnn",
    "niedurn",
    "ofiar wrzesnia",
    "ofiar września",
    "stodolsk",
    "tysiaclec",
    "tysiąclec",
)

@dataclass
class ReparseReport:
    scanned: int = 0
    updated: int = 0
    parcel_updated: int = 0
    obreb_updated: int = 0
    dlq_rows_deleted: int = 0
    reset_processed: int = 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Re-parse bronze.raw_listings with improved parcel/locality extractor",
    )
    parser.add_argument("--limit", type=int, default=None, help="Limit processed Bronze rows")
    parser.add_argument("--dry-run", action="store_true", help="Compute repairs without writing")
    parser.add_argument(
        "--disable-llm",
        action="store_true",
        help="Disable Gemini fallback and use regex-only reparsing",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable DEBUG logging")
    return parser.parse_args()


def _normalize(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(value.split()).strip()


def _normalized_ascii(value: str | None) -> str:
    import unicodedata

    normalized = unicodedata.normalize("NFKD", _normalize(value))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()


def _is_suspicious_obreb(value: str | None) -> bool:
    normalized = _normalized_ascii(value)
    if not normalized:
        return True
    if normalized.isdigit():
        return True
    # Real obreb names are short; very long strings are concatenated court/legal text
    if len(normalized) > 60:
        return True
    return any(token in normalized for token in _SUSPECT_OBREB)


def _has_strong_locality_context(candidate: str, raw_text: str) -> bool:
    candidate_re = re.escape(candidate)
    patterns = (
        rf"(?:adres\s+nieruchomo(?:ś|s)ci|miejsce\s+ogl[eę]dzin:\s*pod\s+adresem)\s+"
        rf"(?:\d{{2}}-\d{{3}}\s+)?{candidate_re}(?=,\s*poczta\b|\s+poczta\b)",
        rf"jedn\.\s*ewid\.\s+{candidate_re}(?=,\s*obr(?:ę|e)?b\b|\s+obr(?:ę|e)?b\b|,|\.)",
    )
    return any(re.search(pattern, raw_text, flags=re.IGNORECASE) for pattern in patterns)


def _has_address_locality_context(candidate: str, raw_text: str) -> bool:
    candidate_re = re.escape(candidate)
    pattern = (
        rf"(?:adres\s+nieruchomo(?:ś|s)ci|miejsce\s+ogl[eę]dzin:\s*pod\s+adresem)\s+"
        rf"(?:\d{{2}}-\d{{3}}\s+)?{candidate_re}(?=,\s*poczta\b|\s+poczta\b)"
    )
    return bool(re.search(pattern, raw_text, flags=re.IGNORECASE))


def _has_ewid_unit_context(candidate: str, raw_text: str) -> bool:
    candidate_re = re.escape(candidate)
    pattern = rf"jedn\.\s*ewid\.\s+{candidate_re}(?=,\s*obr(?:ę|e)?b\b|\s+obr(?:ę|e)?b\b|,|\.)"
    return bool(re.search(pattern, raw_text, flags=re.IGNORECASE))


def _has_explicit_locality_mention(candidate: str, raw_text: str) -> bool:
    candidate_re = re.escape(candidate)
    patterns = (
        rf"\bw\s+miejscow(?:o(?:ś|s)ci|osci)\s+{candidate_re}\b",
        rf"\bpo(?:ł|l)(?:oż|oz)on\w*\s+w\s+{candidate_re}\b",
        rf"\bnieruchomo(?:ś|s)ć\w*\s+gruntow\w*\s+w\s+{candidate_re}\b",
    )
    return any(re.search(pattern, raw_text, flags=re.IGNORECASE) for pattern in patterns)


def _should_replace_obreb(current: str | None, candidate: str | None, raw_text: str) -> bool:
    if not candidate:
        return False
    if _normalize(current) == _normalize(candidate):
        return False
    if _is_suspicious_obreb(candidate):
        return False
    if not current:
        return True
    if _is_suspicious_obreb(current) and not _is_suspicious_obreb(candidate):
        return True
    if current.isdigit() and any(ch.isalpha() for ch in candidate):
        return True
    if _has_ewid_unit_context(candidate, raw_text):
        return True
    if _has_address_locality_context(candidate, raw_text):
        return not _has_explicit_locality_mention(current or "", raw_text)
    if _has_strong_locality_context(candidate, raw_text):
        return True
    if len(_normalize(candidate)) > len(_normalize(current)) + 3 and any(ch.isalpha() for ch in candidate):
        return True
    return False


def _should_replace_parcel(current: str | None, candidate: ParcelMatch | None) -> bool:
    if candidate is None:
        return False
    if _normalize(current) == candidate.numer:
        return False
    if not current:
        return True
    current_norm = _normalize(current)
    if candidate.confidence < 0.70:
        if not (
            candidate.confidence >= 0.66
            and current_norm.isdigit()
            and len(current_norm) <= 2
            and "/" in candidate.numer
            and _normalize(candidate.raw_value) != candidate.numer
        ):
            return False

    if "/" in candidate.numer and "/" not in current_norm:
        return True
    if current_norm.isdigit() and len(current_norm) <= 2 and len(candidate.numer) >= 4:
        return True
    if len(candidate.numer) > len(current_norm) + 2:
        return True
    return False


def _should_clear_parcel(
    current: str | None,
    candidate: ParcelMatch | None,
    raw_text: str,
) -> bool:
    if candidate is not None or not current:
        return False

    current_norm = _normalize(current)
    if any(ch.isalpha() for ch in current_norm):
        return True

    if not current_norm.isdigit() or len(current_norm) > 2:
        return False
    if not re.search(rf"\b{re.escape(current_norm)}-\d{{3}}\b", raw_text):
        return False
    if re.search(
        rf"(?:dz\.?|dzia(?:ł|l)k\w*|ewidencyjn\w*|nr|numer)\D{{0,16}}{re.escape(current_norm)}(?:/\d+)?\b",
        raw_text,
        flags=re.IGNORECASE,
    ):
        return False
    if re.search(
        rf"nieruchomo(?:ść|ści|sc)\s+{re.escape(current_norm)}\s+dzia(?:ł|l)k",
        raw_text,
        flags=re.IGNORECASE,
    ):
        return True
    return True


def _usable_parcel_obreb(candidate: ParcelMatch | None) -> str | None:
    if (
        candidate
        and candidate.obreb_raw
        and candidate.confidence >= 0.70
        and len(candidate.obreb_raw) >= 4
        and not candidate.obreb_raw.isdigit()
    ):
        return candidate.obreb_raw
    return None


def _canonicalize_against_gmina(candidate: str | None, raw_gmina: str | None) -> str | None:
    if not candidate or not raw_gmina or _is_suspicious_obreb(raw_gmina):
        return candidate
    ratio = SequenceMatcher(None, _normalized_ascii(candidate), _normalized_ascii(raw_gmina)).ratio()
    return raw_gmina if ratio >= 0.84 else candidate


async def _count_backlog() -> tuple[int, int]:
    async with AsyncSessionLocal() as db:
        bronze_pending = await db.execute(
            select(func.count()).select_from(RawListing).where(RawListing.is_processed == False)  # noqa: E712
        )
        dlq_count = await db.execute(select(func.count()).select_from(DlqParcel))
        return int(bronze_pending.scalar_one()), int(dlq_count.scalar_one())


async def _derive_candidate_fields(
    text: str,
    raw_gmina: str | None,
    *,
    llm_extractor: LLMExtractor | None,
) -> tuple[ParcelMatch | None, str | None, bool]:
    parcel_result = await extract_with_fallback(
        text,
        raw_gmina=raw_gmina,
        llm_extractor=llm_extractor,
    )
    primary_parcel = parcel_result.primary_parcel
    regex_obreb, _ = extract_obreb(text)
    derived_obreb = parcel_result.obreb_name or _usable_parcel_obreb(primary_parcel)
    if _is_suspicious_obreb(derived_obreb) and not _is_suspicious_obreb(regex_obreb):
        derived_obreb = regex_obreb
    derived_obreb = _canonicalize_against_gmina(derived_obreb, raw_gmina)
    return primary_parcel, derived_obreb, parcel_result.llm_used


async def run_reparse(
    limit: int | None = None,
    dry_run: bool = False,
    *,
    enable_llm: bool = True,
) -> ReparseReport:
    report = ReparseReport()
    llm_extractor = LLMExtractor() if enable_llm else None

    try:
        async with AsyncSessionLocal() as db:
            stmt = select(RawListing).order_by(RawListing.created_at.asc())
            if limit:
                stmt = stmt.limit(limit)
            result = await db.execute(stmt)
            listings = list(result.scalars().all())
            logger.info("[Reparse] Loaded %d Bronze rows", len(listings))

            for listing in listings:
                report.scanned += 1
                raw_text = listing.raw_text or ""
                if not raw_text.strip():
                    continue

                primary_parcel, derived_obreb, llm_used = await _derive_candidate_fields(
                    raw_text,
                    listing.raw_gmina,
                    llm_extractor=llm_extractor,
                )
                replace_parcel = _should_replace_parcel(listing.raw_numer_dzialki, primary_parcel)
                clear_parcel = _should_clear_parcel(listing.raw_numer_dzialki, primary_parcel, raw_text)
                replace_obreb = _should_replace_obreb(
                    listing.raw_obreb,
                    derived_obreb,
                    raw_text,
                )
                if not replace_parcel and not clear_parcel and not replace_obreb:
                    continue

                before_parcel = listing.raw_numer_dzialki
                before_obreb = listing.raw_obreb
                after_parcel = (
                    primary_parcel.numer
                    if replace_parcel and primary_parcel
                    else None if clear_parcel else before_parcel
                )
                after_obreb = derived_obreb if replace_obreb else before_obreb

                dlq_deleted = 0
                if not dry_run:
                    if replace_parcel and primary_parcel:
                        listing.raw_numer_dzialki = primary_parcel.numer
                    elif clear_parcel:
                        listing.raw_numer_dzialki = None
                    if replace_obreb:
                        listing.raw_obreb = derived_obreb
                    listing.is_processed = False
                    delete_result = await db.execute(
                        delete(DlqParcel).where(DlqParcel.listing_id == listing.id)
                    )
                    dlq_deleted = int(delete_result.rowcount or 0)
                    await db.commit()

                report.updated += 1
                report.dlq_rows_deleted += dlq_deleted
                report.reset_processed += 0 if dry_run else 1
                if replace_parcel or clear_parcel:
                    report.parcel_updated += 1
                if replace_obreb:
                    report.obreb_updated += 1

                logger.info(
                    "[Reparse] listing=%s parcel %r -> %r | obreb %r -> %r | "
                    "dlq_deleted=%d | confidence=%.2f | llm_used=%s | snippet=%r",
                    listing.id,
                    before_parcel,
                    after_parcel,
                    before_obreb,
                    after_obreb,
                    dlq_deleted,
                    primary_parcel.confidence if primary_parcel else 0.0,
                    llm_used,
                    primary_parcel.snippet if primary_parcel else raw_text[:120],
                )

        return report
    finally:
        if llm_extractor is not None:
            await llm_extractor.aclose()


def _print_report(report: ReparseReport, backlog_before: tuple[int, int], backlog_after: tuple[int, int]) -> None:
    pending_before, dlq_before = backlog_before
    pending_after, dlq_after = backlog_after

    print(f"\n{'=' * 60}")
    print("BRONZE REPARSE COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Rows scanned         : {report.scanned}")
    print(f"  Rows updated         : {report.updated}")
    print(f"  Parcel fixes         : {report.parcel_updated}")
    print(f"  Oreb/locality fixes  : {report.obreb_updated}")
    print(f"  Reset is_processed   : {report.reset_processed}")
    print(f"  DLQ rows deleted     : {report.dlq_rows_deleted}")
    print(f"  Pending before/after : {pending_before} -> {pending_after}")
    print(f"  DLQ before/after     : {dlq_before} -> {dlq_after}")
    print(f"{'=' * 60}")


async def main() -> None:
    args = _parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    backlog_before = await _count_backlog()
    report = await run_reparse(
        limit=args.limit,
        dry_run=args.dry_run,
        enable_llm=not args.disable_llm,
    )
    backlog_after = await _count_backlog()
    _print_report(report, backlog_before, backlog_after)


if __name__ == "__main__":
    asyncio.run(main())
