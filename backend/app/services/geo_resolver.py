"""GeoResolver — Bronze → Silver pipeline orchestrator.

Consumes unprocessed rows from bronze.raw_listings, resolves each to
a geometry via ULDKClient, and writes results to silver.dzialki +
silver.listing_parcels. Failed resolutions go to silver.dlq_parcels.

GIS Specialist Commandments enforced here:
  - geometry stored exclusively in EPSG:2180
  - area_m2 is computed by PostGIS (GENERATED column), not Python
  - ST_GeomFromWKB(…, 2180) used for explicit SRID injection on insert
  - confidence_score reflects the resolution method used

Backend Lead Commandments:
  - all DB operations via async SQLAlchemy (no raw psycopg2)
  - ON CONFLICT DO NOTHING for idempotent upserts on silver.dzialki
  - batch processing with configurable page size
  - structured logging with correlation ID (scrape_run_id)

DLQ retry schedule (silver.dlq_parcels):
  attempt 1  → next_retry_at = now + 1h
  attempt 2  → next_retry_at = now + 4h
  attempt 3  → next_retry_at = now + 24h
  attempt 4  → next_retry_at = now + 72h
  attempt 5  → manual review required (attempt_count capped at 5)
"""

from __future__ import annotations

import asyncio
import logging
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Literal
from uuid import UUID

from geoalchemy2.shape import from_shape
from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.bronze import RawListing
from app.models.silver import DlqParcel, Dzialka, ListingParcel
from app.services.komornik_notice_enricher import KomornikNoticeEnricher
from app.services.krakow_msip_resolver import KrakowMsipResolver
from app.services.powiat_wfs_parcel_resolver import PowiatWfsParcelResolver
from app.services.uldk import (
    GeometryValidationError,
    ULDKAPIError,
    ULDKClient,
    ULDKGeometryMissingError,
    ULDKNotFoundError,
    ULDKParcel,  # noqa: F401 (re-exported for callers)
    ULDKRegion,
    ULDKTransientError,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Province → voivodeship TERYT code (2-digit)
# ---------------------------------------------------------------------------

_PROVINCE_TO_WOJ: dict[str, str] = {
    "śląskie":           "24",
    "slaskie":           "24",
    "małopolskie":       "12",
    "malopolskie":       "12",
    "mazowieckie":       "14",
    "łódzkie":           "10",
    "lodzkie":           "10",
    "dolnośląskie":      "02",
    "dolnoslaskie":      "02",
    "wielkopolskie":     "30",
    "pomorskie":         "22",
    "lubelskie":         "06",
    "podkarpackie":      "18",
    "kujawsko-pomorskie": "04",
    "lubuskie":          "08",
    "warmińsko-mazurskie": "28",
    "warminsko-mazurskie": "28",
    "podlaskie":         "20",
    "opolskie":          "16",
    "świętokrzyskie":    "26",
    "swietokrzyskie":    "26",
    "zachodniopomorskie": "32",
}

# Strips leading 4-digit obreb code from strings like "0014 Szklary"
_RE_OBREB_CODE_PREFIX = re.compile(r"^\d{4}\s+")

# Polish locative/instrumental → nominative for common obreb/city names.
# These are the forms that appear in auction text ("w Zakopanem" → ULDK needs "Zakopane").
_OBREB_NOMINATIVE: dict[str, str] = {
    "zakopanem":           "Zakopane",
    "zakopane":            "Zakopane",
    "nowym saczu":         "Nowy Sącz",
    "nowym sączu":         "Nowy Sącz",
    "dabrowie gorniczej":  "Dąbrowa Górnicza",
    "dąbrowie górniczej":  "Dąbrowa Górnicza",
    "dąbrowie gorniczej":  "Dąbrowa Górnicza",
    "dabrowie gorniczej":  "Dąbrowa Górnicza",
    "brzeszczach":         "Brzeszcze",
    "imielinie":           "Imielin",
    "rudzie slaskiej":     "Ruda Śląska",
    "rudzie śląskiej":     "Ruda Śląska",
    "jeleśni":             "Jeleśnia",
    "jalesni":             "Jeleśnia",
    "krakowie":            "Kraków",
    "bielsku bialej":      "Bielsko-Biała",
    "bielsku białej":      "Bielsko-Biała",
    "czestochowie":        "Częstochowa",
    "częstochowie":        "Częstochowa",
    "grzegorzowicach wielkich": "Grzegorzowice Wielkie",
    "orzesze":             "Orzesze",
    # -owie locative form of -ów city names
    "knurowie":            "Knurów",
    "frelichowie":         "Frelichów",
    "andrychowie":         "Andrychów",
    "tysiąclecia":         None,   # street name, not obreb
    "goscinna":            None,   # street name
    "gościnna":            None,
    "stodolska":           None,   # street name
    "niedurnego":          None,   # street genitive (ul. Niedurnego)
    "ofiar wrzesnia":      None,   # street name
    "ofiar września":      None,
}

# Simple suffix rules for -anem → -ane (instrumental of -ane words like Zakopane)
_RE_INSTRUMENTAL_ANE = re.compile(r"(?i)anem$")

# Locative singular of -ów city names → nominative: -owie → -ów
# E.g. Knurowie → Knurów, Frelichowie → Frelichów
_RE_LOCATIVE_OW = re.compile(r"(?i)owie$")

# Strip trailing stray consonants appended to place names (e.g. "Ropica Górnaw" → "Ropica Górna").
# Intentionally excludes Polish endings like "-ów", which are legitimate city names.
_RE_TRAILING_STRAY_CONSONANT = re.compile(r"(?i)([aeiyąę])w$")
_RE_MULTI_SPACE = re.compile(r"\s+")
_RE_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_RE_EXPLICIT_REGION = re.compile(
    r"(?i)\bobr(?:ę|e)?b(?:ie)?\.?\s*(?:nr\s*)?(?P<code>\d{1,4})\b"
)
_RE_ADDRESS_LOCALITY = re.compile(
    r"(?is)(?:adres nieruchomości|miejsce oględzin:\s*pod adresem)\s+"
    r"(?:\d{2}-\d{3}\s+)?(?P<name>[A-ZĄĆĘŁŃÓŚŹŻ][A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż .-]{1,80}?)"
    r"(?=,\s*poczta\b|\s+poczta\b)"
)
_ADDRESS_STOPWORDS = {
    "aleja",
    "aleje",
    "osiedle",
    "plac",
    "pl",
    "rondo",
    "ul",
    "ulica",
}

# Urban municipalities need one extra hop: city name → commune TERYT → all cadastral
# regions for that commune. The values below are 7-digit official TERYT commune codes.
_CITY_TO_COMMUNE_TERYT: dict[str, str] = {
    "andrychow": "1218013",
    "czestochowa": "2464011",
    "dabrowa gornicza": "2465011",
    "dabrowa tarnowska": "1204023",
    "gliwice": "2466011",
    "imielin": "2414021",
    "knurow": "2405011",
    "krakow": "1261011",
    "nowy sacz": "1262011",
    "orzesze": "2408031",
    "ruda slaska": "2472011",
    "wodzislaw slaski": "2415041",
}

_AREA_TIEBREAK_TOLERANCE = Decimal("0.05")


def _normalize_obreb_to_nominative(name: str) -> str | None:
    """Try to convert inflected Polish place name to nominative form for ULDK.

    Returns None if the name looks like a street/address rather than an obreb.
    Returns the original name if no conversion is needed or known.
    """
    key = name.lower().strip()
    if key in _OBREB_NOMINATIVE:
        return _OBREB_NOMINATIVE[key]
    # Strip trailing stray consonant appended to last vowel (e.g. "Ropica Górnaw" → "Ropica Górna")
    stray = _RE_TRAILING_STRAY_CONSONANT.search(name)
    if stray:
        name = name[:stray.start() + 1]
        key = name.lower().strip()
        if key in _OBREB_NOMINATIVE:
            return _OBREB_NOMINATIVE[key]
    # -anem → -ane (Zakopanem → Zakopane)
    if _RE_INSTRUMENTAL_ANE.search(name):
        return _RE_INSTRUMENTAL_ANE.sub("ane", name)
    # -owie → -ów (Knurowie → Knurów, Frelichowie → Frelichów)
    if _RE_LOCATIVE_OW.search(name):
        return _RE_LOCATIVE_OW.sub("ów", name)
    return name


def _admin_key(name: str) -> str:
    """Normalise a place name for dictionary lookups."""
    text = name.strip().replace("-", " ")
    text = text.replace("ł", "l").replace("Ł", "L")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return _RE_MULTI_SPACE.sub(" ", text).lower()


def _extract_obreb_name(raw_obreb: str) -> str | None:
    """Extract the text name from raw_obreb strings.

    '0014 Szklary'  → 'Szklary'
    'Szklary'       → 'Szklary'
    '0014'          → None  (code only, no usable name)
    'Wola Ducka'    → 'Wola Ducka'
    """
    name = _RE_OBREB_CODE_PREFIX.sub("", raw_obreb.strip())
    # If only digits remain (pure TERYT code, no name component) return None
    if not name or name.isdigit():
        return None
    return name


def _city_commune_code(
    *,
    obreb_name: str | None,
    raw_gmina: str | None,
) -> str | None:
    """Resolve known city / town names to their commune TERYT code."""
    for candidate in (raw_gmina, obreb_name):
        if not candidate:
            continue
        nominative = _normalize_obreb_to_nominative(candidate)
        if not nominative:
            continue
        code = _CITY_TO_COMMUNE_TERYT.get(_admin_key(nominative))
        if code:
            return code
    return None


def _province_code(raw_woj: str | None) -> str | None:
    """Return 2-digit voivodeship code for a raw province label."""
    if not raw_woj:
        return None
    return _PROVINCE_TO_WOJ.get(raw_woj.lower().strip())


def _commune_code_matches_province(commune_code: str, raw_woj: str | None) -> bool:
    """Check whether a 7-digit commune TERYT matches the expected province."""
    woj_code = _province_code(raw_woj)
    return not woj_code or commune_code.startswith(woj_code)


def _filter_parcels_by_area_hint(
    parcels: list[ULDKParcel],
    area_hint: Decimal | None,
) -> list[ULDKParcel]:
    """Keep only parcel candidates matching the extracted area within +/- 5%."""
    if area_hint is None:
        return []

    hint = Decimal(str(area_hint))
    if hint <= 0:
        return []

    lower = hint * (Decimal("1.00") - _AREA_TIEBREAK_TOLERANCE)
    upper = hint * (Decimal("1.00") + _AREA_TIEBREAK_TOLERANCE)
    return [parcel for parcel in parcels if lower <= parcel.area_m2 <= upper]


def _filter_region_matches_by_area_hint(
    matches: list[tuple[ULDKRegion, ULDKParcel]],
    area_hint: Decimal | None,
) -> list[tuple[ULDKRegion, ULDKParcel]]:
    """Area-aware tie-break for urban region enumeration results."""
    if area_hint is None:
        return []

    hint = Decimal(str(area_hint))
    if hint <= 0:
        return []

    lower = hint * (Decimal("1.00") - _AREA_TIEBREAK_TOLERANCE)
    upper = hint * (Decimal("1.00") + _AREA_TIEBREAK_TOLERANCE)
    return [
        match for match in matches
        if lower <= match[1].area_m2 <= upper
    ]


def _city_commune_code_for_candidate(
    candidate: str | None,
    raw_woj: str | None,
) -> str | None:
    """Resolve a single place-name candidate to a commune TERYT code."""
    if not candidate:
        return None
    nominative = _normalize_obreb_to_nominative(candidate)
    if not nominative:
        return None
    code = _CITY_TO_COMMUNE_TERYT.get(_admin_key(nominative))
    if not code:
        return None
    return code if _commune_code_matches_province(code, raw_woj) else None


def _truncate_property_context(text: str | None) -> str | None:
    """Trim trailing court boilerplate to focus on the property description."""
    if not text:
        return None
    for marker in ("Komornik Sądowy", "Kancelaria Komornicza", "Komornik przy"):
        idx = text.find(marker)
        if idx > 0:
            return text[:idx]
    return text


def _city_commune_codes_in_text(
    text: str | None,
    raw_woj: str | None,
) -> list[str]:
    """Find city mentions in free text, preferring longer place names first."""
    if not text:
        return []

    haystack = _RE_NON_ALNUM.sub(" ", _admin_key(text)).strip()
    if not haystack:
        return []

    seen: set[str] = set()
    matches: list[str] = []
    for city_key, commune_code in sorted(
        _CITY_TO_COMMUNE_TERYT.items(),
        key=lambda item: (-len(item[0]), item[0]),
    ):
        if commune_code in seen or not _commune_code_matches_province(commune_code, raw_woj):
            continue
        variants = {city_key}
        for inflected, nominative in _OBREB_NOMINATIVE.items():
            if nominative and _admin_key(nominative) == city_key:
                variants.add(_admin_key(inflected))
        if any(
            re.search(rf"(?<![a-z0-9]){re.escape(variant)}(?![a-z0-9])", haystack)
            for variant in variants
        ):
            seen.add(commune_code)
            matches.append(commune_code)
    return matches


def _infer_commune_code_for_listing(
    *,
    raw_gmina: str | None,
    obreb_name: str | None,
    raw_text: str | None,
    title: str | None,
    raw_woj: str | None,
) -> str | None:
    """Infer commune code using direct fields first, then textual city mentions."""
    for candidate in (raw_gmina, obreb_name):
        code = _city_commune_code_for_candidate(candidate, raw_woj)
        if code:
            return code

    text_windows = [
        _truncate_property_context(raw_text),
        raw_text,
        title,
    ]
    for window in text_windows:
        matches = _city_commune_codes_in_text(window, raw_woj)
        if matches:
            return matches[0]

    for locality in _extract_text_locality_candidates(raw_text):
        code = _city_commune_code_for_candidate(locality, raw_woj)
        if code:
            return code
    return None


def _extract_explicit_region_code(raw_text: str | None) -> str | None:
    """Extract explicit cadastral region code from listing prose.

    Examples:
      "obręb 32"     -> "0032"
      "obr 94"       -> "0094"
      "obręb nr 5"   -> "0005"
    """
    if not raw_text:
        return None
    match = _RE_EXPLICIT_REGION.search(raw_text)
    if not match:
        return None
    return match.group("code").zfill(4)


def _clean_locality_candidate(candidate: str) -> str | None:
    """Normalize a locality candidate extracted from listing address prose."""
    text = _RE_MULTI_SPACE.sub(" ", candidate).strip(" ,.")
    if not text:
        return None
    if any(ch.isdigit() for ch in text):
        return None
    if "," in text:
        return None
    words = text.split()
    if not words or len(words) > 4:
        return None
    if words[0].lower().rstrip(".") in _ADDRESS_STOPWORDS:
        return None
    normalized = _normalize_obreb_to_nominative(text)
    return normalized.strip() if normalized else None


def _extract_text_locality_candidates(raw_text: str | None) -> list[str]:
    """Extract likely locality / village names from address blocks in the listing text."""
    if not raw_text:
        return []

    candidates: list[str] = []
    seen: set[str] = set()
    for match in _RE_ADDRESS_LOCALITY.finditer(raw_text):
        cleaned = _clean_locality_candidate(match.group("name"))
        if not cleaned:
            continue
        key = _admin_key(cleaned)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(cleaned)
    return candidates


def _filter_by_province(
    parcels: list[ULDKParcel], raw_woj: str | None
) -> list[ULDKParcel]:
    """Filter parcel list to those matching the expected voivodeship.

    Used to disambiguate GetParcelByIdOrNr results when the obreb name
    is not unique across Poland.  Returns full list if province unknown.
    """
    if not raw_woj:
        return parcels
    woj_code = _PROVINCE_TO_WOJ.get(raw_woj.lower().strip())
    if not woj_code:
        return parcels
    filtered = [p for p in parcels if p.teryt_wojewodztwo == woj_code]
    return filtered if filtered else parcels


# ---------------------------------------------------------------------------
# DLQ retry schedule
# ---------------------------------------------------------------------------

_DLQ_RETRY_DELAYS: list[timedelta] = [
    timedelta(hours=1),    # attempt 1 → retry in 1h
    timedelta(hours=4),    # attempt 2 → retry in 4h
    timedelta(hours=24),   # attempt 3 → retry in 24h
    timedelta(hours=72),   # attempt 4 → retry in 72h
    # attempt 5 → no auto-retry, requires manual intervention
]
_DLQ_MAX_ATTEMPTS = 5

# Throttle between ULDK calls (GUGiK enforces ~2 req/s)
_ULDK_INTER_REQUEST_DELAY_S = 0.6
_ULDK_REGION_FALLBACK_DELAY_S = 0.15

# Confidence scores by resolution method
_CONFIDENCE = {
    "kw_lookup":     Decimal("0.92"),  # KW → ULDK: deterministic, single parcel or few
    "teryt_exact":   Decimal("0.98"),  # TERYT ID exact match: canonical
    "address_fuzzy": Decimal("0.55"),  # address-derived parcel ID: uncertain
    "uldk_partial":  Decimal("0.70"),  # resolved but check digit was invalid in source
}


def _next_retry_at(attempt: int) -> datetime:
    """Compute next_retry_at for a given attempt number (1-indexed)."""
    idx = attempt - 1
    if idx < len(_DLQ_RETRY_DELAYS):
        return datetime.now(timezone.utc) + _DLQ_RETRY_DELAYS[idx]
    # Beyond schedule → sentinel datetime (manual review)
    return datetime(9999, 12, 31, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Resolution report
# ---------------------------------------------------------------------------

@dataclass
class ResolutionReport:
    """Aggregate result from GeoResolver.process_pending_listings()."""
    total_processed: int = 0
    resolved: int = 0
    sent_to_dlq: int = 0
    already_resolved: int = 0
    errors: list[str] = field(default_factory=list)
    duration_s: float = 0.0

    @property
    def success_rate(self) -> float:
        if self.total_processed == 0:
            return 0.0
        return round(self.resolved / self.total_processed, 3)


# ---------------------------------------------------------------------------
# GeoResolver
# ---------------------------------------------------------------------------

class GeoResolver:
    """Orchestrates Bronze → Silver geometry resolution.

    Usage:
        async with AsyncSessionLocal() as db:
            async with ULDKClient() as uldk:
                resolver = GeoResolver(db, uldk)
                report = await resolver.process_pending_listings(batch_size=50)
    """

    def __init__(self, db: AsyncSession, uldk: ULDKClient) -> None:
        self.db = db
        self.uldk = uldk
        self.notice_enricher = KomornikNoticeEnricher()
        self.krakow_msip = KrakowMsipResolver()
        self.powiat_wfs = PowiatWfsParcelResolver()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def process_pending_listings(
        self,
        batch_size: int = 50,
        listing_ids: list[UUID] | None = None,
    ) -> ResolutionReport:
        """Process a batch of unresolved raw_listings → silver.dzialki.

        Queries bronze.raw_listings WHERE is_processed = false,
        ordered by created_at ASC (oldest first — FIFO).
        """
        t_start = asyncio.get_event_loop().time()
        report = ResolutionReport()

        listings = await self._fetch_pending_listings(batch_size, listing_ids=listing_ids)
        logger.info("[GeoResolver] Processing %d pending listings", len(listings))

        for listing in listings:
            outcome = await self._resolve_listing(listing)

            if outcome == "resolved":
                report.resolved += 1
            elif outcome == "dlq":
                report.sent_to_dlq += 1
            else:  # 'skip'
                report.already_resolved += 1

            report.total_processed += 1

            # Throttle ULDK calls — respect GUGiK rate limits
            await asyncio.sleep(_ULDK_INTER_REQUEST_DELAY_S)

        report.duration_s = round(asyncio.get_event_loop().time() - t_start, 2)
        logger.info(
            "[GeoResolver] Complete — resolved=%d dlq=%d total=%d in %.1fs "
            "(success_rate=%.1f%%)",
            report.resolved, report.sent_to_dlq, report.total_processed,
            report.duration_s, report.success_rate * 100,
        )
        return report

    # ------------------------------------------------------------------
    # Per-listing resolution
    # ------------------------------------------------------------------

    async def _resolve_listing(
        self, listing: RawListing
    ) -> Literal["resolved", "dlq", "skip"]:
        """Try all resolution strategies for a single listing.

        Strategy:
          1. GetParcelById via TERYT (raw_numer_dzialki + raw_obreb as TERYT code)
          2. DLQ if no resolvable data

        KW-based resolution (GetParcelByKW) is NOT supported — that ULDK endpoint
        does not exist (confirmed April 2026). KW-only listings go straight to DLQ
        with KW_RESOLUTION_UNSUPPORTED until ekw.ms.gov.pl integration is built.

        Always marks listing.is_processed = True after this method,
        regardless of outcome — the DLQ handles retries, not this method.
        """
        listing_id = listing.id
        log_prefix = f"[GeoResolver] listing={listing_id}"

        try:
            parcels: list[ULDKParcel] = []
            match_method = "teryt_exact"
            geometry_missing = False
            transient_failure: str | None = None

            # NOTE: ULDK GetParcelByKW does NOT exist (confirmed April 2026).
            # Any call to ?request=GetParcelByKW returns the plain-text error
            # "niepoprawny parametr GetParcelByKW". KW → parcel resolution
            # requires a two-step lookup via ekw.ms.gov.pl (not yet implemented).
            # Strategy: use GetParcelById with TERYT data from raw_numer_dzialki + raw_obreb.

            if listing.raw_kw and not listing.raw_numer_dzialki and not listing.raw_obreb:
                # Truly KW-only listing — no parcel identifier data at all.
                # GetParcelByKW does not exist in ULDK. Cannot resolve until
                # ekw.ms.gov.pl integration is implemented.
                logger.info(
                    "%s KW-only listing (kw=%s) — no parcel data. "
                    "Sending to DLQ: KW_RESOLUTION_UNSUPPORTED.",
                    log_prefix, listing.raw_kw,
                )
                await self._send_to_dlq(
                    listing_id,
                    f"KW_RESOLUTION_UNSUPPORTED: listing has raw_kw={listing.raw_kw!r} "
                    "but no parcel number or obreb. GetParcelByKW does not exist "
                    "in ULDK (confirmed April 2026). Requires ekw.ms.gov.pl integration.",
                    attempt=1,
                )
                await self._mark_processed(listing_id)
                return "dlq"

            # Strategy 1: resolve by parcel TERYT ID
            if listing.raw_numer_dzialki and listing.raw_obreb:
                parcel_id = self._build_uldk_id(listing)
                if parcel_id:
                    logger.info("%s trying parcel ID lookup: %s", log_prefix, parcel_id)
                    try:
                        p = await self.uldk.resolve_parcel_by_id(parcel_id)
                        if p:
                            parcels = [p]
                            match_method = "teryt_exact"
                    except ULDKGeometryMissingError as exc:
                        logger.warning(
                            "%s [SEVERITY:MEDIUM] Ghost Parcel — parcel ID %s missing "
                            "geometry in ULDK. Will NOT retry. error=%s",
                            log_prefix, parcel_id, exc,
                        )
                        geometry_missing = True
                    except ULDKNotFoundError:
                        logger.info("%s parcel ID not found: %s", log_prefix, parcel_id)
                    except ULDKTransientError as exc:
                        transient_failure = (
                            f"ULDK_TRANSIENT: parcel ID lookup failed for {parcel_id}: {exc}"
                        )
                        logger.warning("%s parcel ID lookup transient failure: %s", log_prefix, exc)
                    except (ULDKAPIError, GeometryValidationError) as exc:
                        logger.warning("%s parcel ID lookup failed: %s", log_prefix, exc)

            # Strategy 2: GetParcelByIdOrNr — obreb name + parcel number
            # Fires when Strategy 1 failed (no TERYT code) but raw_obreb
            # contains a human-readable name like "0014 Szklary" → "Szklary".
            # Does NOT require commune code — ULDK searches nationwide by name.
            # Risk: name may not be unique; disambiguate by province.
            if not parcels and listing.raw_numer_dzialki and listing.raw_obreb:
                obreb_name = _extract_obreb_name(listing.raw_obreb)
                if obreb_name:
                    obreb_name = _normalize_obreb_to_nominative(obreb_name)
                if obreb_name:
                    logger.info(
                        "%s trying GetParcelByIdOrNr: region=%r numer=%r",
                        log_prefix, obreb_name, listing.raw_numer_dzialki,
                    )
                    try:
                        candidates = await self.uldk.resolve_parcel_by_nr(
                            obreb_name, listing.raw_numer_dzialki,
                        )
                        if len(candidates) == 1:
                            parcels = candidates
                            match_method = "teryt_exact"
                            logger.info(
                                "%s GetParcelByIdOrNr: unique match → %s",
                                log_prefix, candidates[0].identyfikator,
                            )
                        elif len(candidates) > 1:
                            logger.info(
                                "%s GetParcelByIdOrNr: %d candidates, "
                                "disambiguating by province=%r",
                                log_prefix, len(candidates), listing.raw_wojewodztwo,
                            )
                            filtered = _filter_by_province(candidates, listing.raw_wojewodztwo)
                            usable = filtered or candidates
                            if len(usable) == 1:
                                parcels = usable
                                match_method = "teryt_exact"
                                logger.info(
                                    "%s GetParcelByIdOrNr: disambiguated by province → %s",
                                    log_prefix, parcels[0].identyfikator,
                                )
                            else:
                                area_filtered = _filter_parcels_by_area_hint(
                                    usable,
                                    listing.area_m2,
                                )
                                if len(area_filtered) == 1:
                                    parcels = area_filtered
                                    match_method = "uldk_partial"
                                    logger.info(
                                        "%s GetParcelByIdOrNr: area tie-break resolved → %s "
                                        "(area_hint=%s m2, candidates=%d)",
                                        log_prefix,
                                        parcels[0].identyfikator,
                                        listing.area_m2,
                                        len(usable),
                                    )
                                elif area_filtered:
                                    logger.info(
                                        "%s GetParcelByIdOrNr: area tie-break still ambiguous "
                                        "(area_hint=%s m2, matches=%d/%d)",
                                        log_prefix,
                                        listing.area_m2,
                                        len(area_filtered),
                                        len(usable),
                                    )
                                else:
                                    logger.info(
                                        "%s GetParcelByIdOrNr: still ambiguous after province filter "
                                        "(candidates=%d)",
                                        log_prefix,
                                        len(usable),
                                    )
                            if parcels:
                                logger.info(
                                    "%s GetParcelByIdOrNr: final disambiguated → %s (method=%s)",
                                    log_prefix,
                                    parcels[0].identyfikator,
                                    match_method,
                                )
                    except ULDKNotFoundError:
                        logger.info(
                            "%s GetParcelByIdOrNr: not found (region=%r numer=%r)",
                            log_prefix, obreb_name, listing.raw_numer_dzialki,
                        )
                    except ULDKTransientError as exc:
                        transient_failure = (
                            f"ULDK_TRANSIENT: GetParcelByIdOrNr failed for "
                            f"region={obreb_name!r} numer={listing.raw_numer_dzialki!r}: {exc}"
                        )
                        logger.warning(
                            "%s GetParcelByIdOrNr transient failure: %s", log_prefix, exc,
                        )
                    except (ULDKAPIError, GeometryValidationError) as exc:
                        logger.warning(
                            "%s GetParcelByIdOrNr failed: %s", log_prefix, exc,
                        )

            # Strategy 2b: derive locality from address prose ("Adres nieruchomości
            # 33-250 Kłyż, poczta Otfinów") when raw_obreb captured the wrong city.
            if not parcels and listing.raw_numer_dzialki and transient_failure is None:
                text_parcels = await self._try_text_locality_fallback(listing, log_prefix)
                if text_parcels:
                    parcels = text_parcels
                    match_method = "uldk_partial"

            # Strategy 2c: official notice enrichment from the current portal.
            # This helps when Bronze captured only the SSR excerpt or a false-positive
            # bare number from the postal code, while the notice backend exposes
            # richer HTML or we have a confirmed archived KW override.
            if not parcels and listing.source_url and transient_failure is None:
                notice_parcels = await self._try_notice_enrichment_fallback(listing, log_prefix)
                if notice_parcels:
                    parcels = notice_parcels
                    match_method = "kw_lookup" if listing.raw_kw else "uldk_partial"

            # Strategy 2d: public Kraków MSIP fallback for listings whose official
            # notice exposes "jedn. ewid. Podgórze, obręb 94" style context.
            if (
                not parcels
                and listing.source_url
                and transient_failure is None
                and _admin_key(listing.raw_wojewodztwo or "") == "malopolskie"
            ):
                krakow_parcels = await self._try_krakow_msip_fallback(listing, log_prefix)
                if krakow_parcels:
                    parcels = krakow_parcels
                    match_method = "kw_lookup" if listing.raw_kw else "manual"

            # Strategy 2e: selected Małopolskie powiat WFS fallbacks (Zakopane,
            # Andrychów). These public cadastral services expose parcel geometry
            # directly and help where ULDK remains ambiguous.
            if (
                not parcels
                and listing.source_url
                and transient_failure is None
                and _admin_key(listing.raw_wojewodztwo or "") == "malopolskie"
            ):
                powiat_wfs_parcels = await self._try_powiat_wfs_fallback(listing, log_prefix)
                if powiat_wfs_parcels:
                    parcels = powiat_wfs_parcels
                    match_method = "kw_lookup" if listing.raw_kw else "manual"

            # Strategy 3: urban fallback — city/gmina name → commune TERYT → all regions
            # This solves cases like "Gliwice, działka 44", where the listing gives a
            # city name instead of the cadastral region required by ULDK.
            if not parcels and listing.raw_numer_dzialki and transient_failure is None:
                urban_parcels = await self._try_city_region_fallback(listing, log_prefix)
                if urban_parcels:
                    parcels = urban_parcels
                    match_method = "uldk_partial"

            # Strategy 3b: if the listing already resolved to a single parcel but
            # the official notice hints at a multi-parcel complex, try to enrich
            # the result set before saving to Silver. This keeps simple listings
            # fast while recovering extra parcels for "działki nr 198/3, 200, ..."
            # style notices without requiring a second manual replay.
            if (
                parcels
                and len(parcels) == 1
                and listing.source_url
                and transient_failure is None
                and "dział" in (listing.raw_text or "").lower()
            ):
                notice_parcels = await self._try_notice_enrichment_fallback(listing, log_prefix)
                if notice_parcels:
                    merged: dict[str, ULDKParcel] = {
                        parcel.identyfikator: parcel for parcel in parcels
                    }
                    for parcel in notice_parcels:
                        merged.setdefault(parcel.identyfikator, parcel)
                    if len(merged) > len(parcels):
                        parcels = list(merged.values())
                        logger.info(
                            "%s notice enrichment augmented resolved parcel set %d -> %d",
                            log_prefix,
                            1,
                            len(parcels),
                        )
                        if listing.raw_kw:
                            match_method = "kw_lookup"

            # Outcome: resolved
            if parcels:
                await self._save_to_silver(listing, parcels, match_method)
                await self._mark_processed(listing_id)
                return "resolved"

            # Outcome: Ghost Parcel — geometry permanently missing, remove from retry cycle
            if geometry_missing:
                await self._mark_geometry_missing(listing_id, listing.raw_kw)
                await self._mark_processed(listing_id)
                return "dlq"

            if transient_failure:
                await self._send_to_dlq(listing_id, transient_failure, attempt=1)
                await self._mark_processed(listing_id)
                return "dlq"

            # Outcome: unresolvable — classify and send to DLQ
            numer = listing.raw_numer_dzialki
            obreb = listing.raw_obreb
            kw = listing.raw_kw

            if numer and obreb:
                # Had TERYT data but both strategies failed:
                # Strategy 1: obreb is not parseable as a TERYT code
                # Strategy 2: obreb name resolved but ULDK returned not-found
                obreb_name_attempt = _extract_obreb_name(obreb)
                reason = (
                    f"ULDK_NOT_FOUND: raw_obreb={obreb!r} (name={obreb_name_attempt!r}) "
                    f"raw_numer_dzialki={numer!r} — not found via GetParcelById "
                    "or GetParcelByIdOrNr. Parcel may be unregistered or name ambiguous. "
                    f"raw_kw={kw!r}, raw_gmina={listing.raw_gmina!r}"
                )
            elif numer and not obreb:
                # Has parcel number but no obreb name/code — both strategies failed.
                # Strategy 1 needs full TERYT, Strategy 2 needs an obreb name.
                reason = (
                    f"TERYT_INCOMPLETE: raw_numer_dzialki={numer!r} extracted but "
                    f"raw_obreb is missing — GetParcelById needs commune.obreb.numer, "
                    f"GetParcelByIdOrNr needs obreb_name+numer. "
                    f"raw_kw={kw!r}, raw_gmina={listing.raw_gmina!r}. "
                    "Fix: extract obreb name from detail page HTML."
                )
            elif not numer and not obreb and not kw:
                reason = (
                    "NO_RESOLVABLE_DATA: listing has neither raw_kw, raw_numer_dzialki, "
                    f"nor raw_obreb. source_url={listing.source_url!r}"
                )
            else:
                # Had a buildable parcel ID but ULDK returned not-found
                parcel_id_attempt = self._build_uldk_id(listing)
                reason = (
                    f"ULDK_NOT_FOUND: parcel_id={parcel_id_attempt!r} not found in ULDK. "
                    f"raw_kw={kw!r}, raw_obreb={obreb!r}"
                )
            await self._send_to_dlq(listing_id, reason, attempt=1)
            await self._mark_processed(listing_id)
            return "dlq"

        except Exception as exc:
            logger.error(
                "%s Unexpected error — sending to DLQ: %s", log_prefix, exc, exc_info=True,
            )
            await self.db.rollback()
            await self._send_to_dlq(
                listing_id,
                f"Unexpected error: {type(exc).__name__}: {exc}",
                attempt=1,
            )
            await self._mark_processed(listing_id)
            return "dlq"

    # ------------------------------------------------------------------
    # Silver layer persistence
    # ------------------------------------------------------------------

    async def _save_to_silver(
        self,
        listing: RawListing,
        parcels: list[ULDKParcel],
        match_method: str,
    ) -> None:
        """Write resolved parcels to silver.dzialki + silver.listing_parcels.

        Uses ON CONFLICT DO NOTHING on identyfikator (unique constraint) so
        re-running the resolver is safe and idempotent.

        Geometry is injected as a GeoAlchemy2 WKBElement via from_shape(),
        which constructs the correct EWKB payload with SRID=2180.
        PostGIS will compute area_m2 via the GENERATED ALWAYS AS column.
        """
        confidence = _CONFIDENCE.get(match_method, Decimal("0.70"))

        for parcel in parcels:
            # --- Convert Shapely → GeoAlchemy2 WKBElement (SRID 2180) ---
            # from_shape() produces EWKB with embedded SRID.
            # PostGIS stores it as GEOMETRY(MultiPolygon, 2180).
            # area_m2 is GENERATED — we do NOT set it here.
            geom_element = from_shape(parcel.geom_shape, srid=2180)

            logger.debug(
                "[GIS] Inserting Dzialka identyfikator=%s area=%.2f m² "
                "was_made_valid=%s SRID=2180",
                parcel.identyfikator,
                float(parcel.area_m2),
                parcel.was_made_valid,
            )
            if parcel.was_made_valid:
                logger.warning(
                    "[GIS] SEVERITY:MEDIUM — ST_MakeValid() was applied to %s "
                    "before insert. Geometry was invalid from ULDK.",
                    parcel.identyfikator,
                )

            # Upsert into silver.dzialki (idempotent)
            dzialka_stmt = (
                pg_insert(Dzialka)
                .values(
                    teryt_wojewodztwo=parcel.teryt_wojewodztwo,
                    teryt_powiat=parcel.teryt_powiat,
                    teryt_gmina=parcel.teryt_gmina,
                    teryt_obreb=parcel.teryt_obreb,
                    numer_dzialki=parcel.numer_dzialki,
                    identyfikator=parcel.identyfikator,
                    geom=geom_element,
                    # area_m2: GENERATED ALWAYS AS (ST_Area(geom)) — not set here
                    uldk_response_date=parcel.fetched_at,
                    uldk_raw_response={
                        "identifier": parcel.identifier,
                        "voivodeship": parcel.voivodeship,
                        "county": parcel.county,
                        "commune": parcel.commune,
                        "region": parcel.region,
                        "parcel": parcel.parcel,
                        "geom_wkb_hex": parcel.geom_wkb_hex[:40] + "…",   # truncated for JSON
                        "area_m2_computed": float(parcel.area_m2),
                        "was_made_valid": parcel.was_made_valid,
                    },
                    match_confidence=float(confidence),
                    resolution_status="resolved",
                )
                .on_conflict_do_update(
                    index_elements=["identyfikator"],
                    set_={
                        # On conflict: refresh geometry and metadata, keep original created_at
                        "geom": geom_element,
                        "uldk_response_date": parcel.fetched_at,
                        "match_confidence": float(confidence),
                        "resolution_status": "resolved",
                        "failure_reason": None,
                        "updated_at": datetime.now(timezone.utc),
                    },
                )
                .returning(Dzialka.id)
            )

            result = await self.db.execute(dzialka_stmt)
            dzialka_row = result.fetchone()
            if dzialka_row is None:
                logger.error(
                    "[GeoResolver] Dzialka upsert returned no ID for %s",
                    parcel.identyfikator,
                )
                continue

            dzialka_id: UUID = dzialka_row[0]

            # Insert into silver.listing_parcels (Bronze → Silver link)
            lp_stmt = (
                pg_insert(ListingParcel)
                .values(
                    listing_id=listing.id,
                    dzialka_id=dzialka_id,
                    match_confidence=float(confidence),
                    match_method=match_method,
                )
                .on_conflict_do_nothing(
                    index_elements=["listing_id", "dzialka_id"]
                )
            )
            await self.db.execute(lp_stmt)

        dlq_delete = await self.db.execute(
            delete(DlqParcel).where(DlqParcel.listing_id == listing.id)
        )
        await self.db.commit()
        logger.info(
            "[GeoResolver] Saved %d parcel(s) to Silver for listing %s "
            "(cleared_dlq=%d)",
            len(parcels), listing.id, dlq_delete.rowcount or 0,
        )

    # ------------------------------------------------------------------
    # DLQ
    # ------------------------------------------------------------------

    async def _send_to_dlq(
        self,
        listing_id: UUID,
        error: str,
        attempt: int = 1,
    ) -> None:
        """Insert or increment a DLQ entry for the given listing.

        If a DLQ row already exists for this listing (from a previous run),
        increments attempt_count and updates next_retry_at using the
        exponential backoff schedule.
        """
        from sqlalchemy import select

        existing = await self.db.execute(
            select(DlqParcel).where(DlqParcel.listing_id == listing_id)
        )
        row = existing.scalar_one_or_none()

        if row is not None:
            new_attempt = min(row.attempt_count + 1, _DLQ_MAX_ATTEMPTS)
            row.attempt_count = new_attempt
            row.last_error = error
            row.next_retry_at = _next_retry_at(new_attempt)
            row.updated_at = datetime.now(timezone.utc)
            logger.info(
                "[GeoResolver] DLQ increment listing=%s attempt=%d next_retry=%s",
                listing_id, new_attempt, row.next_retry_at.isoformat(),
            )
        else:
            # Fetch the raw_teryt_input to store in DLQ for traceability
            listing_q = await self.db.execute(
                select(RawListing).where(RawListing.id == listing_id)
            )
            listing = listing_q.scalar_one_or_none()
            raw_input = (
                listing.raw_kw or listing.raw_numer_dzialki or str(listing_id)
                if listing else str(listing_id)
            )

            dlq = DlqParcel(
                listing_id=listing_id,
                raw_teryt_input=raw_input,
                attempt_count=attempt,
                last_error=error,
                next_retry_at=_next_retry_at(attempt),
            )
            self.db.add(dlq)
            logger.info(
                "[GeoResolver] DLQ new entry listing=%s attempt=1 "
                "next_retry=%s error=%s",
                listing_id, dlq.next_retry_at.isoformat(), error[:80],
            )

        await self.db.commit()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _mark_geometry_missing(
        self,
        listing_id: UUID,
        raw_kw: str | None,
    ) -> None:
        """Record a Ghost Parcel — geometry permanently absent from ULDK.

        Inserts into silver.dlq_parcels with attempt_count=5 (=max) so the
        DLQ retry job skips it. The status 'geometry_missing' is persisted
        for analyst review via the dashboard.

        Resolution path: analyst checks geoportal.gov.pl or the relevant
        powiat's own WFS service and manually inserts geometry if found.
        """
        dlq = DlqParcel(
            listing_id=listing_id,
            raw_teryt_input=raw_kw or str(listing_id),
            attempt_count=_DLQ_MAX_ATTEMPTS,  # at max → DLQ job won't retry
            last_error=(
                "GEOMETRY_MISSING: ULDK returned status=-1 for a valid KW. "
                "Parcel is unspatialized in GUGiK. Requires manual geoportal lookup."
            ),
            next_retry_at=datetime(9999, 12, 31, tzinfo=timezone.utc),  # sentinel
        )
        self.db.add(dlq)
        await self.db.commit()
        logger.info(
            "[GeoResolver] Ghost Parcel recorded: listing=%s kw=%r "
            "→ dlq(attempt_count=%d, manual_review_required)",
            listing_id, raw_kw, _DLQ_MAX_ATTEMPTS,
        )

    async def _fetch_pending_listings(
        self,
        limit: int,
        listing_ids: list[UUID] | None = None,
    ) -> list[RawListing]:
        """Fetch unprocessed listings from bronze, oldest first."""
        stmt = (
            select(RawListing)
            .where(RawListing.is_processed == False)  # noqa: E712
            .order_by(RawListing.created_at.asc())
            .limit(limit)
        )
        if listing_ids:
            stmt = stmt.where(RawListing.id.in_(listing_ids))

        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def _try_city_region_fallback(
        self,
        listing: RawListing,
        log_prefix: str,
    ) -> list[ULDKParcel]:
        """Expand an urban commune into all cadastral regions and probe parcel IDs."""
        numer = listing.raw_numer_dzialki
        if not numer:
            return []

        obreb_name = _extract_obreb_name(listing.raw_obreb) if listing.raw_obreb else None
        if obreb_name:
            obreb_name = _normalize_obreb_to_nominative(obreb_name)

        commune_code = _infer_commune_code_for_listing(
            raw_gmina=listing.raw_gmina,
            obreb_name=obreb_name,
            raw_text=listing.raw_text,
            title=listing.title,
            raw_woj=listing.raw_wojewodztwo,
        )
        if not commune_code:
            return []

        logger.info(
            "%s trying urban region fallback: place=%r commune=%s numer=%r",
            log_prefix,
            obreb_name or listing.raw_gmina or listing.raw_obreb,
            commune_code,
            numer,
        )

        explicit_region = _extract_explicit_region_code(listing.raw_text)
        if explicit_region:
            try:
                parcel = await self.uldk.resolve_parcel_by_commune_region(
                    commune_code=commune_code,
                    region_code=explicit_region,
                    parcel_nr=numer,
                )
                if parcel:
                    logger.info(
                        "%s urban fallback: explicit region %s → %s",
                        log_prefix, explicit_region, parcel.identyfikator,
                    )
                    return [parcel]
            except ULDKNotFoundError:
                logger.info(
                    "%s urban fallback: explicit region %s not found for parcel=%r",
                    log_prefix, explicit_region, numer,
                )
            except ULDKTransientError as exc:
                logger.warning(
                    "%s urban fallback: explicit region %s transient failure for parcel=%r: %s",
                    log_prefix, explicit_region, numer, exc,
                )
                return []
            except (ULDKAPIError, GeometryValidationError) as exc:
                logger.warning(
                    "%s urban fallback: explicit region %s failed for parcel=%r: %s",
                    log_prefix, explicit_region, numer, exc,
                )

        try:
            regions = await self.uldk.list_regions_for_commune(commune_code)
        except ULDKTransientError as exc:
            logger.warning(
                "%s urban fallback: region enumeration failed for commune=%s: %s",
                log_prefix, commune_code, exc,
            )
            return []
        except (ULDKAPIError, ULDKNotFoundError) as exc:
            logger.warning(
                "%s urban fallback: region enumeration failed for commune=%s: %s",
                log_prefix, commune_code, exc,
            )
            return []

        matches: list[tuple[ULDKRegion, ULDKParcel]] = []
        for region in regions:
            try:
                parcel = await self.uldk.resolve_parcel_by_commune_region(
                    commune_code=commune_code,
                    region_code=region.region_code,
                    parcel_nr=numer,
                )
                if parcel:
                    matches.append((region, parcel))
            except ULDKGeometryMissingError as exc:
                logger.warning(
                    "%s urban fallback: geometry missing for region=%s parcel=%r: %s",
                    log_prefix, region.identifier, numer, exc,
                )
            except ULDKNotFoundError:
                continue
            except ULDKTransientError as exc:
                logger.warning(
                    "%s urban fallback transient failure for region=%s parcel=%r: %s",
                    log_prefix, region.identifier, numer, exc,
                )
                return []
            except (ULDKAPIError, GeometryValidationError) as exc:
                logger.warning(
                    "%s urban fallback failed for region=%s parcel=%r: %s",
                    log_prefix, region.identifier, numer, exc,
                )

            await asyncio.sleep(_ULDK_REGION_FALLBACK_DELAY_S)

        if not matches:
            logger.info(
                "%s urban fallback: no parcel found across %d region(s) for commune=%s",
                log_prefix, len(regions), commune_code,
            )
            return []

        if len(matches) == 1:
            region, parcel = matches[0]
            logger.info(
                "%s urban fallback: unique match via region=%s (%s) → %s",
                log_prefix, region.region_name or region.region_code,
                region.region_code, parcel.identyfikator,
            )
            return [parcel]

        text_hits = self._filter_matches_by_text_context(listing.raw_text, matches)
        if len(text_hits) == 1:
            region, parcel = text_hits[0]
            logger.info(
                "%s urban fallback: text disambiguated region=%s (%s) → %s",
                log_prefix, region.region_name or region.region_code,
                region.region_code, parcel.identyfikator,
            )
            return [parcel]

        area_hits = _filter_region_matches_by_area_hint(matches, listing.area_m2)
        if len(area_hits) == 1:
            region, parcel = area_hits[0]
            logger.info(
                "%s urban fallback: area tie-break region=%s (%s) → %s "
                "(area_hint=%s m2)",
                log_prefix,
                region.region_name or region.region_code,
                region.region_code,
                parcel.identyfikator,
                listing.area_m2,
            )
            return [parcel]

        logger.warning(
            "%s urban fallback: ambiguous parcel number %r across %d region(s); "
            "leaving in DLQ",
            log_prefix, numer, len(matches),
        )
        return []

    async def _try_notice_enrichment_fallback(
        self,
        listing: RawListing,
        log_prefix: str,
    ) -> list[ULDKParcel]:
        """Fetch the official notice HTML and retry parcel resolution from it.

        The official item-back notice is a better source than the SSR search-page
        excerpt we originally scraped. For some legacy/edge cases we also keep a
        tiny evidence-backed KW override registry when the current portal hides the
        cadastral breakdown but older public copies exposed it.
        """
        hint = await self.notice_enricher.fetch_notice_hint(
            source_url=listing.source_url,
            raw_kw=listing.raw_kw,
        )
        if hint is None or not hint.parcel_numbers:
            return []

        obreb_name = hint.obreb_name
        if obreb_name is None and listing.raw_obreb:
            obreb_name = _extract_obreb_name(listing.raw_obreb)
            if obreb_name:
                obreb_name = _normalize_obreb_to_nominative(obreb_name)

        if not obreb_name:
            logger.info(
                "%s notice enrichment (%s) found parcel hints %s but no usable locality/obręb",
                log_prefix,
                hint.source,
                ", ".join(hint.parcel_numbers),
            )
            return []

        logger.info(
            "%s notice enrichment via %s: obreb=%r parcels=%s",
            log_prefix,
            hint.source,
            obreb_name,
            ", ".join(hint.parcel_numbers),
        )

        resolved: list[ULDKParcel] = []
        seen_ids: set[str] = set()

        for numer in hint.parcel_numbers:
            try:
                candidates = await self.uldk.resolve_parcel_by_nr(obreb_name, numer)
            except ULDKNotFoundError:
                logger.info(
                    "%s notice enrichment: parcel=%r not found for obreb=%r",
                    log_prefix,
                    numer,
                    obreb_name,
                )
                await asyncio.sleep(_ULDK_REGION_FALLBACK_DELAY_S)
                continue
            except ULDKTransientError as exc:
                logger.warning(
                    "%s notice enrichment transient failure for parcel=%r obreb=%r: %s",
                    log_prefix,
                    numer,
                    obreb_name,
                    exc,
                )
                break
            except (ULDKAPIError, GeometryValidationError) as exc:
                logger.warning(
                    "%s notice enrichment failed for parcel=%r obreb=%r: %s",
                    log_prefix,
                    numer,
                    obreb_name,
                    exc,
                )
                await asyncio.sleep(_ULDK_REGION_FALLBACK_DELAY_S)
                continue

            filtered = _filter_by_province(candidates, listing.raw_wojewodztwo)
            usable = filtered or candidates
            if len(usable) != 1:
                area_filtered = _filter_parcels_by_area_hint(usable, listing.area_m2)
                if len(area_filtered) == 1:
                    usable = area_filtered
                    logger.info(
                        "%s notice enrichment: area tie-break parcel=%r obreb=%r → %s",
                        log_prefix,
                        numer,
                        obreb_name,
                        usable[0].identyfikator,
                    )
                else:
                    logger.warning(
                        "%s notice enrichment: parcel=%r obreb=%r ambiguous (%d candidate(s))",
                        log_prefix,
                        numer,
                        obreb_name,
                        len(usable),
                    )
                    await asyncio.sleep(_ULDK_REGION_FALLBACK_DELAY_S)
                    continue

            parcel = usable[0]
            if parcel.identyfikator not in seen_ids:
                resolved.append(parcel)
                seen_ids.add(parcel.identyfikator)
            await asyncio.sleep(_ULDK_REGION_FALLBACK_DELAY_S)

        return resolved

    async def _try_krakow_msip_fallback(
        self,
        listing: RawListing,
        log_prefix: str,
    ) -> list[ULDKParcel]:
        """Resolve Kraków parcels from the public MSIP EGiB service.

        This is intentionally narrow and only activates when the official notice
        contains explicit cadastral context such as:
            "jedn. ewid. Podgórze, obręb 94"
        """
        hint = await self.notice_enricher.fetch_notice_hint(
            source_url=listing.source_url,
            raw_kw=listing.raw_kw,
        )
        if hint is None or not hint.parcel_numbers or not hint.plain_text:
            return []

        try:
            parcels = await self.krakow_msip.resolve_from_notice(
                plain_text=hint.plain_text,
                parcel_numbers=hint.parcel_numbers,
            )
        except GeometryValidationError as exc:
            logger.warning("%s Kraków MSIP fallback failed: %s", log_prefix, exc)
            return []
        except Exception as exc:
            logger.warning("%s Kraków MSIP fallback transport failure: %s", log_prefix, exc)
            return []

        if parcels:
            logger.info(
                "%s Kraków MSIP fallback resolved %d parcel(s): %s",
                log_prefix,
                len(parcels),
                ", ".join(parcel.identyfikator for parcel in parcels),
            )
        return parcels

    async def _try_powiat_wfs_fallback(
        self,
        listing: RawListing,
        log_prefix: str,
    ) -> list[ULDKParcel]:
        """Resolve selected Małopolskie listings from public powiat WFS sources."""
        hint = await self.notice_enricher.fetch_notice_hint(
            source_url=listing.source_url,
            raw_kw=listing.raw_kw,
        )
        if hint is None or not hint.parcel_numbers or not hint.plain_text:
            return []

        try:
            parcels = await self.powiat_wfs.resolve(
                raw_obreb=listing.raw_obreb,
                raw_gmina=listing.raw_gmina,
                plain_text=hint.plain_text,
                parcel_numbers=hint.parcel_numbers,
            )
        except GeometryValidationError as exc:
            logger.warning("%s powiat WFS fallback failed: %s", log_prefix, exc)
            return []
        except Exception as exc:
            logger.warning("%s powiat WFS fallback transport failure: %s", log_prefix, exc)
            return []

        if parcels:
            logger.info(
                "%s powiat WFS fallback resolved %d parcel(s): %s",
                log_prefix,
                len(parcels),
                ", ".join(parcel.identyfikator for parcel in parcels),
            )
        return parcels

    async def _try_text_locality_fallback(
        self,
        listing: RawListing,
        log_prefix: str,
    ) -> list[ULDKParcel]:
        """Try GetParcelByIdOrNr with locality names mined from address prose."""
        numer = listing.raw_numer_dzialki
        if not numer:
            return []

        candidates = _extract_text_locality_candidates(listing.raw_text)
        if not candidates:
            return []

        raw_obreb_name = _extract_obreb_name(listing.raw_obreb) if listing.raw_obreb else None
        raw_obreb_key = _admin_key(raw_obreb_name) if raw_obreb_name else None

        for locality in candidates:
            if raw_obreb_key and _admin_key(locality) == raw_obreb_key:
                continue

            logger.info(
                "%s trying text-locality fallback: locality=%r numer=%r",
                log_prefix, locality, numer,
            )
            try:
                parcel_matches = await self.uldk.resolve_parcel_by_nr(locality, numer)
            except ULDKNotFoundError:
                continue
            except (ULDKAPIError, GeometryValidationError) as exc:
                logger.warning(
                    "%s text-locality fallback failed for locality=%r numer=%r: %s",
                    log_prefix, locality, numer, exc,
                )
                continue

            if len(parcel_matches) == 1:
                logger.info(
                    "%s text-locality fallback: unique match via %r → %s",
                    log_prefix, locality, parcel_matches[0].identyfikator,
                )
                return parcel_matches

            filtered = _filter_by_province(parcel_matches, listing.raw_wojewodztwo)
            if len(filtered) == 1:
                logger.info(
                    "%s text-locality fallback: province-disambiguated via %r → %s",
                    log_prefix, locality, filtered[0].identyfikator,
                )
                return [filtered[0]]
            area_filtered = _filter_parcels_by_area_hint(
                filtered or parcel_matches,
                listing.area_m2,
            )
            if len(area_filtered) == 1:
                logger.info(
                    "%s text-locality fallback: area tie-break via %r → %s",
                    log_prefix, locality, area_filtered[0].identyfikator,
                )
                return [area_filtered[0]]

        return []

    @staticmethod
    def _filter_matches_by_text_context(
        raw_text: str | None,
        matches: list[tuple[ULDKRegion, ULDKParcel]],
    ) -> list[tuple[ULDKRegion, ULDKParcel]]:
        """Use listing text as a weak tie-breaker when multiple city regions match."""
        if not raw_text:
            return []

        haystack = _admin_key(raw_text)
        narrowed = [
            match for match in matches
            if match[0].region_name and _admin_key(match[0].region_name) in haystack
        ]
        return narrowed

    async def _mark_processed(self, listing_id: UUID) -> None:
        """Mark a raw_listing as processed (regardless of outcome)."""
        await self.db.execute(
            update(RawListing)
            .where(RawListing.id == listing_id)
            .values(is_processed=True)
        )
        await self.db.commit()

    @staticmethod
    def _build_uldk_id(listing: RawListing) -> str | None:
        """Attempt to construct a ULDK parcel ID from raw listing fields.

        ULDK format: '{commune}.{obreb}.{numer_dzialki}'
        e.g., '1412011.0001.123/4'

        This is a best-effort construction — the raw fields may be incomplete
        or fuzzy-matched. Confidence will be set to 'address_fuzzy' (<0.7)
        if the ID was constructed this way.
        """
        # If raw_obreb looks like a 9-digit TERYT code, we can split it
        obreb = listing.raw_obreb or ""
        numer = listing.raw_numer_dzialki or ""

        if not numer:
            return None

        # Case 1: raw_obreb is a 9-digit TERYT code
        if obreb.isdigit() and len(obreb) == 9:
            commune = obreb[:7]
            obreb4 = obreb[7:].zfill(4)
            return f"{commune}.{obreb4}.{numer}"

        # Case 2: raw_obreb has the full GUGiK dot-notation
        if "." in obreb and obreb.replace(".", "").isdigit():
            return f"{obreb}.{numer}"

        # Cannot build a reliable ULDK ID — return None (will go to DLQ)
        return None


# ---------------------------------------------------------------------------
# Standalone runner (for testing / one-off runs)
# ---------------------------------------------------------------------------

async def run_geo_resolver(
    batch_size: int = 20,
    listing_ids: list[UUID] | None = None,
) -> ResolutionReport:
    """Run one resolution cycle — useful for Cloud Run Jobs and CLI invocation."""
    from app.core.database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        async with ULDKClient() as uldk:
            resolver = GeoResolver(db, uldk)
            return await resolver.process_pending_listings(
                batch_size=batch_size,
                listing_ids=listing_ids,
            )


if __name__ == "__main__":
    import asyncio as _asyncio
    import logging as _logging

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    report = _asyncio.run(run_geo_resolver(batch_size=10))

    print(f"\n{'='*60}")
    print("GEO RESOLVER COMPLETE")
    print(f"{'='*60}")
    print(f"  Total processed : {report.total_processed}")
    print(f"  Resolved        : {report.resolved}")
    print(f"  Sent to DLQ     : {report.sent_to_dlq}")
    print(f"  Success rate    : {report.success_rate:.1%}")
    print(f"  Duration        : {report.duration_s}s")
    if report.errors:
        print("\n  ERRORS:")
        for e in report.errors:
            print(f"    - {e}")
    print(f"{'='*60}")
