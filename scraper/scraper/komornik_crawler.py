"""KomornikCrawler — live crawler for licytacje.komornik.pl (2026 portal structure).

== PORTAL STRUCTURE (confirmed via Google index + web research, April 2026) ==

Old /Notice/Search → 307 redirect → /wyszukiwarka/obwieszczenia-o-licytacji
Old /Notice/Details/{id} → still alive (legacy IDs ~600k+)
New detail URL: /wyszukiwarka/obwieszczenia-o-licytacji/{id}/{slug}

ENTRY POINTS — SSR-rendered, Google-indexed, no JS required:
  /Notice/Filter/-3      — wszystkie nieruchomości (all real estate)
  /Notice/Filter/28      — grunty (land/plots) — most relevant for TerraZoning
  /Notice/Filter/29      — domy (houses)
  /Notice/Filter/30      — mieszkania (apartments)

Province filtering via ProvinceId URL param is UNCONFIRMED for Filter pages.
Strategy: crawl all-Poland, post-filter by extracting raw_wojewodztwo from text
and matching against target province patterns.

== PIPELINE ==
  1. GET /Notice/Filter/{category_id}[?ProvinceId={id}]  (try province param, fallback without)
  2. Parse HTML for listing links — both old and new URL formats
  3. Follow pagination (look for next-page links in HTML)
  4. For each detail URL: GET, parse, extract KW + sygnatura + działka + price
  5. Province post-filter: skip if extracted province doesn't match targets
  6. save_listing() → bronze.raw_listings (dedup + druga licytacja detection)

== RATE LIMITING ==
  1.5 s between detail requests. Search/filter pages: 0.5 s (fewer, less risk).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional
from uuid import UUID

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.bronze import RawListing, ScrapeRun
from scraper.extractors.llm_extractor import LLMExtractor, extract_with_fallback
from scraper.extractors.kw import ExtractionSource, extract_kw_from_text
from scraper.extractors.price import parse_polish_decimal, parse_price_value
from scraper.main import (
    ExtractionPayload,
    ScrapeResult,
    _extract_area,
    _extract_date,
    _extract_location,
    _extract_price,
    _extract_sygnatura,
    _serialise_kw,
    _serialise_parcel,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Portal constants
# ---------------------------------------------------------------------------

_BASE_URL = "https://licytacje.komornik.pl"

# SSR listing page — confirmed April 2026.
# Supports province + subCategory filtering and offset-based pagination.
#   Province:    ?province=śląskie  (URL-encoded Polish name, case-sensitive)
#   Category:    &subCategory=LAND  (uppercase; omit for all types)
#   Pagination:  &offset=N          (N = page_index * 20, 0-indexed)
_LISTING_URL = f"{_BASE_URL}/wyszukiwarka/obwieszczenia-o-licytacji"
_PAGE_SIZE = 20

# Category subCategory values (case-sensitive).
# "grunty" = LAND is the primary TerraZoning target.
# "" = all real estate (broad sweep fallback).
_CATEGORIES: dict[str, str] = {
    "grunty": "LAND",
}

# Province URL names (exact strings the portal accepts in ?province= param).
# URL-encoded by httpx at request time.
_PROVINCE_IDS: dict[str, str] = {
    "małopolskie": "małopolskie",
    "śląskie":     "śląskie",
}

# Province detection: match in extracted text (post-filter fallback)
# Handles both accented and ASCII forms, all grammatical cases
_RE_PROVINCE_TARGET = re.compile(
    r"\b(?:ma[łl]opolsk\w*|[śs][lł][aą]sk\w*)\b",
    re.IGNORECASE,
)

# Property-type filter: TerraZoning targets land (grunty/działki), not apartments.
# Listings that are clearly non-land (lokal mieszkalny, mieszkanie, garaż…) are
# skipped even if they are in the target province.
# "nieruchomość gruntowa zabudowana" is kept — it's land, even with a building.
_RE_LAND_SIGNAL = re.compile(
    r"\b(?:gruntow[aąeę]|grunt(?:u|y|ów)?|dzia[lł]k[aąęi]|rolny|rolna|niezabudowan)\b",
    re.IGNORECASE,
)
_RE_NON_LAND_SIGNAL = re.compile(
    r"\b(?:lokal\s+mieszkal\w*|lokal\s+u[sś]ługow\w*|mieszkani[ea]\b|garaz|gara[żz]|"
    r"udzia[lł]\s+w\s+wysoko[śs]ci)",
    re.IGNORECASE,
)

# Link patterns — support BOTH URL generations
_RE_NEW_DETAIL = re.compile(
    r"/wyszukiwarka/obwieszczenia-o-licytacji/(\d+)/[\w\-]+",
    re.IGNORECASE,
)
_RE_OLD_DETAIL = re.compile(
    r"/Notice/Details/(\d+)",
    re.IGNORECASE,
)

# Pagination: next-page link patterns (Bootstrap / Polish text)
_RE_NEXT_PAGE_HREF = re.compile(r"[?&][Pp]age=(\d+)")
_NEXT_PAGE_TEXTS = {"następna", "next", "»", "›"}

# Price label in detail page table rows
_RE_PRICE_LABEL = re.compile(
    r"cena\s+(?:wy(?:wo[lł][aą]wcz[ay]|wołania|woławcza|woławcze)|wywoł\.?|wywołania)",
    re.IGNORECASE,
)

# Rate limits
_DETAIL_DELAY_S: float = 1.5
_SEARCH_DELAY_S: float = 0.5
_MAX_PAGES: int = 2

# Minimum content length to consider a detail page usable
_MIN_CONTENT_CHARS = 80

_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


# ---------------------------------------------------------------------------
# KomornikCrawler
# ---------------------------------------------------------------------------


class KomornikCrawler:
    """Live async crawler for licytacje.komornik.pl.

    Crawls SSR Filter pages (grunty + all nieruchomości), supports both old
    /Notice/Details/{id} and new /wyszukiwarka/obwieszczenia-o-licytacji/{id}/
    detail URL formats. Province filtering is applied post-extraction.

    Usage:
        async with AsyncSessionLocal() as db:
            crawler = KomornikCrawler(db)
            result = await crawler.run()
    """

    SOURCE_NAME = "licytacje.komornik.pl"

    def __init__(
        self,
        db: AsyncSession,
        categories: Optional[dict[str, int]] = None,
        provinces: Optional[dict[str, int]] = None,
        max_pages: int = _MAX_PAGES,
        detail_delay_s: float = _DETAIL_DELAY_S,
        skip_province_filter: bool = False,
    ) -> None:
        self.db = db
        self.categories = categories or _CATEGORIES
        self.provinces = provinces or _PROVINCE_IDS
        self.max_pages = max_pages
        self.detail_delay_s = detail_delay_s
        # skip_province_filter=True: save all listings regardless of province
        self.skip_province_filter = skip_province_filter
        self._http: Optional[httpx.AsyncClient] = None
        self._llm_extractor = LLMExtractor()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run(self) -> ScrapeResult:
        t_start = asyncio.get_event_loop().time()
        scrape_run = await self._create_scrape_run()
        logger.info(
            "[Crawler] Live run id=%s categories=%s provinces=%s max_pages=%d",
            scrape_run.id, list(self.categories.keys()),
            list(self.provinces.keys()), self.max_pages,
        )

        saved = skipped = failed = province_filtered = 0
        errors: list[str] = []

        async with httpx.AsyncClient(
            headers=_HEADERS,
            timeout=httpx.Timeout(30.0),
            follow_redirects=True,
        ) as self._http:
            # Warm-up: get session cookie from landing page
            try:
                warmup = await self._http.get(_BASE_URL)
                logger.debug(
                    "[Crawler] Warm-up: %s → HTTP %d (final: %s)",
                    _BASE_URL, warmup.status_code, str(warmup.url),
                )
            except Exception as exc:
                logger.warning("[Crawler] Warm-up failed (non-fatal): %s", exc)

            # Collect unique detail URLs from all categories × provinces × pages
            detail_urls = await self._collect_detail_urls(errors)

            logger.info(
                "[Crawler] Collected %d unique detail URLs across all categories",
                len(detail_urls),
            )

            # Fetch and process each detail page
            for idx, url in enumerate(detail_urls, start=1):
                logger.info("[Crawler] %d/%d → %s", idx, len(detail_urls), url)
                try:
                    payload = await self._fetch_and_parse_detail(url)

                    if payload is None:
                        failed += 1
                        errors.append(f"[{url}] parse returned None")
                        continue

                    # Province post-filter (unless disabled)
                    if not self.skip_province_filter and not self._matches_target_province(payload):
                        province_filtered += 1
                        logger.debug(
                            "[Crawler] Province filtered out: url=%s woj=%s",
                            url, payload.raw_wojewodztwo or "—",
                        )
                        continue

                    # Property-type filter: skip non-land listings (apartments, garages…)
                    if not _is_land_listing(payload):
                        province_filtered += 1   # reuse counter — "filtered" bucket
                        logger.debug(
                            "[Crawler] Non-land listing filtered out: url=%s title=%s",
                            url, payload.title or "—",
                        )
                        continue

                    outcome = await self._save_listing(scrape_run.id, payload)
                    if outcome == "saved":
                        saved += 1
                        logger.info(
                            "[Crawler] SAVED kw=%s sygnatura=%s area=%s price=%s "
                            "woj=%s confidence=%.2f",
                            payload.raw_kw or "—",
                            payload.sygnatura_akt or "—",
                            f"{payload.area_m2} m²" if payload.area_m2 else "—",
                            f"{payload.price_zl} zł" if payload.price_zl else "—",
                            payload.raw_wojewodztwo or "—",
                            payload.extraction_confidence,
                        )
                    else:
                        skipped += 1

                except Exception as exc:
                    failed += 1
                    msg = f"[{url}] {type(exc).__name__}: {exc}"
                    errors.append(msg)
                    logger.error("[Crawler] Detail failed: %s", msg, exc_info=True)

                # Polite delay between detail requests
                if idx < len(detail_urls):
                    await asyncio.sleep(self.detail_delay_s)

        await self._llm_extractor.aclose()

        status = "completed" if failed == 0 else "partial"
        await self._update_scrape_run(
            scrape_run.id,
            status=status,
            records_found=len(detail_urls),
            records_saved=saved,
        )

        duration = round(asyncio.get_event_loop().time() - t_start, 2)
        logger.info(
            "[Crawler] Done: found=%d saved=%d skipped_dedup=%d "
            "province_filtered=%d failed=%d in %.1fs",
            len(detail_urls), saved, skipped, province_filtered, failed, duration,
        )
        return ScrapeResult(
            scrape_run_id=scrape_run.id,
            source=self.SOURCE_NAME,
            listings_found=len(detail_urls),
            listings_saved=saved,
            listings_skipped_dedup=skipped,
            listings_failed=failed,
            errors=errors,
            duration_s=duration,
        )

    # ------------------------------------------------------------------
    # URL collection
    # ------------------------------------------------------------------

    async def _collect_detail_urls(self, errors: list[str]) -> list[str]:
        """Collect unique detail page URLs from all configured categories × provinces.

        Uses /wyszukiwarka/obwieszczenia-o-licytacji with:
          - province={name}        — server-side province filter (Polish name)
          - subCategory={LAND}     — server-side category filter
          - offset=N               — pagination (0-indexed, step=20)
        """
        seen_ids: set[str] = set()
        ordered_urls: list[str] = []

        for cat_name, cat_sub in self.categories.items():
            logger.info("[Crawler] Category: %s (subCategory=%s)", cat_name, cat_sub or "all")

            for prov_name, prov_param in self.provinces.items():
                for page_idx in range(self.max_pages):
                    await asyncio.sleep(_SEARCH_DELAY_S)
                    try:
                        page_urls, has_more = await self._fetch_listing_page(
                            province_param=prov_param,
                            sub_category=cat_sub,
                            page_idx=page_idx,
                        )

                        new_count = 0
                        for url, notice_id in page_urls:
                            if notice_id not in seen_ids:
                                seen_ids.add(notice_id)
                                ordered_urls.append(url)
                                new_count += 1

                        logger.info(
                            "[Crawler] cat=%s prov=%s page=%d → %d new URLs "
                            "(total=%d) has_more=%s",
                            cat_name, prov_name, page_idx + 1, new_count,
                            len(ordered_urls), has_more,
                        )

                        if not has_more or not page_urls:
                            break

                    except Exception as exc:
                        msg = f"[cat={cat_name} prov={prov_name} page={page_idx+1}] {exc}"
                        errors.append(msg)
                        logger.error("[Crawler] Listing page failed: %s", msg)
                        break

        return ordered_urls

    async def _fetch_listing_page(
        self,
        province_param: str,
        sub_category: str,
        page_idx: int,
    ) -> tuple[list[tuple[str, str]], bool]:
        """Fetch one SSR listing page with province + category + offset filtering.

        Returns:
            (list of (full_url, notice_id) tuples, has_more_pages)
        """
        assert self._http is not None

        params: dict[str, str] = {
            "mainCategory": "REAL_ESTATE",
            "province":     province_param,
            "offset":       str(page_idx * _PAGE_SIZE),
        }
        if sub_category:
            params["subCategory"] = sub_category

        resp = await self._http.get(_LISTING_URL, params=params)
        resp.raise_for_status()

        logger.debug(
            "[Crawler] Listing %s → HTTP %d",
            str(resp.url), resp.status_code,
        )

        soup = BeautifulSoup(resp.text, "lxml")
        results = _extract_listing_links(soup)

        # has_more: assume more pages if we got a full page of results
        has_more = len(results) >= _PAGE_SIZE

        if not results:
            _log_empty_diagnostics(resp.text, province_param, page_idx + 1, sub_category)

        return results, has_more

    # ------------------------------------------------------------------
    # Detail page parsing
    # ------------------------------------------------------------------

    async def _fetch_and_parse_detail(self, url: str) -> Optional[ExtractionPayload]:
        """Fetch one detail page and extract all fields.

        Supports both:
          - Old: /Notice/Details/{id}         (ASP.NET MVC layout)
          - New: /wyszukiwarka/.../{id}/{slug} (new portal layout)
        """
        assert self._http is not None

        try:
            resp = await self._http.get(url)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error("[Crawler] Detail HTTP %d: %s", exc.response.status_code, url)
            return None
        except httpx.RequestError as exc:
            logger.error("[Crawler] Detail request error %s: %s", url, exc)
            return None

        soup = BeautifulSoup(resp.text, "lxml")

        title = _extract_title(soup)

        # Content area — try specific selectors before full body
        content_el = (
            soup.find("div", class_=re.compile(r"\bcol-(?:sm|md|lg)-(?:8|9|10)\b", re.I))
            or soup.find("div", id=re.compile(r"^(?:content|main|notice|ogłoszenie)", re.I))
            or soup.find("main")
            or soup.find("article")
            or soup.find("div", class_=re.compile(
                r"notice|listing|detail|obwieszczenie|treść|tresc", re.I
            ))
            or soup.body
        )

        if content_el is None:
            logger.warning("[Crawler] No content element found: %s", url)
            return None

        raw_text = content_el.get_text(separator=" ", strip=True)
        raw_text = unicodedata.normalize("NFC", raw_text)

        if len(raw_text) < _MIN_CONTENT_CHARS:
            logger.warning(
                "[Crawler] Content too short (%d chars) for %s — possible SPA shell",
                len(raw_text), url,
            )
            # Last resort: use full page text
            raw_text = soup.get_text(separator=" ", strip=True)
            raw_text = unicodedata.normalize("NFC", raw_text)
            if len(raw_text) < _MIN_CONTENT_CHARS:
                logger.error("[Crawler] Page content unusable (<80 chars): %s", url)
                return None

        # --- Structured price (more reliable than free-text regex) ---
        price = _extract_price_structured(soup) or _extract_price(raw_text)

        # --- KW + parcel extraction pipeline ---
        kw_matches = extract_kw_from_text(raw_text, source=ExtractionSource.FREE_TEXT_REGEX)
        primary_kw = kw_matches[0] if kw_matches else None

        gmina, powiat, woj = _extract_location(raw_text)
        parcel_result = await extract_with_fallback(
            raw_text,
            title=title,
            raw_gmina=gmina,
            raw_kw=primary_kw.normalized if primary_kw else None,
            llm_extractor=self._llm_extractor,
        )
        parcel_matches = parcel_result.parcel_matches
        primary_parcel = parcel_result.primary_parcel
        obreb_name = parcel_result.obreb_name
        if not gmina and parcel_result.municipality:
            gmina = parcel_result.municipality
        area = _extract_area(raw_text)
        if area is None and parcel_result.area_text:
            llm_area = parse_polish_decimal(parcel_result.area_text)
            if llm_area is not None and Decimal("0.5") <= llm_area <= Decimal("10000000"):
                area = llm_area
        auction_date = _extract_date(raw_text)
        sygnatura_akt = _extract_sygnatura(raw_text)

        kw_conf = primary_kw.confidence if primary_kw else 0.0
        parcel_conf = primary_parcel.confidence if primary_parcel else 0.0
        extraction_confidence = round(
            (kw_conf + parcel_conf) / 2 if (kw_conf or parcel_conf) else 0.0,
            2,
        )

        if not primary_kw and not primary_parcel:
            logger.warning(
                "[Crawler] SEVERITY:MEDIUM — no KW / no działka extracted from %s "
                "(confidence=0.00)",
                url,
            )

        return ExtractionPayload(
            source_url=url,
            title=title,
            raw_text=raw_text,
            price_zl=price,
            area_m2=area,
            auction_date=auction_date,
            raw_kw=primary_kw.normalized if primary_kw else None,
            raw_numer_dzialki=primary_parcel.numer if primary_parcel else None,
            raw_obreb=obreb_name,
            raw_gmina=gmina,
            raw_powiat=powiat,
            raw_wojewodztwo=woj,
            sygnatura_akt=sygnatura_akt,
            all_kw_matches=[_serialise_kw(m) for m in kw_matches],
            all_parcel_matches=[_serialise_parcel(m) for m in parcel_matches],
            kw_check_valid=primary_kw.check_valid if primary_kw else False,
            kw_court_known=primary_kw.court_known if primary_kw else False,
            extraction_confidence=extraction_confidence,
            llm_fallback_used=parcel_result.llm_used,
            llm_extraction=parcel_result.llm_extraction,
        )

    # ------------------------------------------------------------------
    # Province post-filter
    # ------------------------------------------------------------------

    def _matches_target_province(self, payload: ExtractionPayload) -> bool:
        """Return True if the listing appears to be in a target province.

        Checks (in order of reliability):
          1. raw_wojewodztwo field (regex-extracted from structured location text)
          2. raw_text full-text search for province name patterns
          3. title search

        If NO province information is found at all, allow through with a warning
        (better to over-include than silently drop hard-to-geocode listings).
        """
        # Check extracted province field first
        if payload.raw_wojewodztwo:
            if _RE_PROVINCE_TARGET.search(payload.raw_wojewodztwo):
                return True
            # Province found but doesn't match → definitive exclusion
            logger.debug(
                "[Crawler] Province mismatch: extracted woj=%r",
                payload.raw_wojewodztwo,
            )
            return False

        # Province not extracted by regex — search in full text
        text_to_search = (payload.raw_text or "") + " " + (payload.title or "")
        if _RE_PROVINCE_TARGET.search(text_to_search):
            return True

        # No province information at all — allow through, flag for review
        logger.debug(
            "[Crawler] No province detected in listing %s — allowing through (review)",
            payload.source_url,
        )
        return True

    # ------------------------------------------------------------------
    # Database operations
    # ------------------------------------------------------------------

    async def _create_scrape_run(self) -> ScrapeRun:
        run = ScrapeRun(
            source_name=self.SOURCE_NAME,
            status="running",
            job_metadata={
                "scraper_version": "0.3.0",
                "mode": "live",
                "categories": list(self.categories.keys()),
                "provinces": list(self.provinces.keys()),
                "max_pages": self.max_pages,
                "detail_delay_s": self.detail_delay_s,
            },
        )
        self.db.add(run)
        await self.db.flush()
        await self.db.commit()
        await self.db.refresh(run)
        logger.info("[Crawler] Created scrape_run id=%s", run.id)
        return run

    async def _update_scrape_run(
        self,
        run_id: UUID,
        status: str,
        records_found: int = 0,
        records_saved: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        result = await self.db.execute(
            select(ScrapeRun).where(ScrapeRun.id == run_id)
        )
        run = result.scalar_one_or_none()
        if run is None:
            logger.error("[Crawler] scrape_run %s not found", run_id)
            return
        run.status = status
        run.records_found = records_found
        run.records_saved = records_saved
        run.finished_at = datetime.now(timezone.utc)
        if error_message:
            run.error_message = error_message
        await self.db.commit()

    async def _save_listing(
        self, scrape_run_id: UUID, payload: ExtractionPayload
    ) -> str:
        """Save listing to bronze.raw_listings with dedup + druga licytacja logic."""

        # --- Druga licytacja detection ---
        if payload.sygnatura_akt and payload.raw_kw:
            existing_result = await self.db.execute(
                select(RawListing)
                .where(
                    RawListing.sygnatura_akt == payload.sygnatura_akt,
                    RawListing.raw_kw == payload.raw_kw,
                )
                .limit(1)
            )
            existing = existing_result.scalar_one_or_none()
            if existing is not None:
                logger.info(
                    "[Crawler] DRUGA LICYTACJA: sygnatura=%s kw=%s "
                    "updating listing %s price %s→%s date %s→%s",
                    payload.sygnatura_akt, payload.raw_kw, existing.id,
                    existing.price_zl, payload.price_zl,
                    existing.auction_date, payload.auction_date,
                )
                existing.price_zl = payload.price_zl
                existing.auction_date = payload.auction_date
                existing.source_url = payload.source_url
                existing.is_processed = False
                await self.db.commit()
                return "saved"

        # --- Normal dedup insert ---
        dedup_hash = _compute_dedup_hash(payload.source_url, payload.raw_text)
        evidence_ref = f"local://extraction_meta/{dedup_hash[:12]}.json"

        stmt = (
            pg_insert(RawListing)
            .values(
                scrape_run_id=scrape_run_id,
                source_url=payload.source_url,
                source_type="licytacja_komornicza",
                title=payload.title,
                raw_text=payload.raw_text,
                price_zl=payload.price_zl,
                area_m2=payload.area_m2,
                auction_date=payload.auction_date,
                raw_kw=payload.raw_kw,
                raw_numer_dzialki=payload.raw_numer_dzialki,
                raw_obreb=payload.raw_obreb,
                raw_gmina=payload.raw_gmina,
                raw_powiat=payload.raw_powiat,
                raw_wojewodztwo=payload.raw_wojewodztwo,
                sygnatura_akt=payload.sygnatura_akt,
                raw_html_ref=evidence_ref,
                dedup_hash=dedup_hash,
                is_processed=False,
            )
            .on_conflict_do_nothing(index_elements=["dedup_hash"])
            .returning(RawListing.id)
        )

        result = await self.db.execute(stmt)
        await self.db.commit()

        row = result.fetchone()
        if row is None:
            logger.debug("[Crawler] Dedup skipped: %s", payload.source_url)
            return "skipped"
        return "saved"


# ---------------------------------------------------------------------------
# Module-level HTML parsing helpers
# ---------------------------------------------------------------------------

def _extract_listing_links(soup: BeautifulSoup) -> list[tuple[str, str]]:
    """Extract all detail page links from a filter/search results page.

    Returns list of (full_url, notice_id) tuples.
    Supports both:
      - New: /wyszukiwarka/obwieszczenia-o-licytacji/{id}/{slug}
      - Old: /Notice/Details/{id}
    """
    results: list[tuple[str, str]] = []
    seen_ids: set[str] = set()

    for a_tag in soup.find_all("a", href=True):
        href: str = a_tag["href"]

        # Try new URL format first (portal >= Feb 2026)
        m = _RE_NEW_DETAIL.search(href)
        if m:
            notice_id = m.group(1)
            if notice_id not in seen_ids:
                seen_ids.add(notice_id)
                full_url = href if href.startswith("http") else f"{_BASE_URL}{href}"
                results.append((full_url, notice_id))
            continue

        # Fall back to old URL format
        m = _RE_OLD_DETAIL.search(href)
        if m:
            notice_id = f"old_{m.group(1)}"   # prefix prevents collision with new IDs
            if notice_id not in seen_ids:
                seen_ids.add(notice_id)
                full_url = href if href.startswith("http") else f"{_BASE_URL}{href}"
                results.append((full_url, notice_id))

    return results


def _has_next_page(soup: BeautifulSoup, current_page: int) -> bool:
    """Detect whether the results page has a 'next page' link.

    Checks (in order):
      1. Pagination links with text matching NEXT_PAGE_TEXTS
      2. A[href] containing Page={current_page+1}
      3. rel="next" on any link
    """
    next_page_num = current_page + 1

    for a_tag in soup.find_all("a", href=True):
        href: str = a_tag["href"]
        text = a_tag.get_text(strip=True).lower()

        # Text-based detection (Polish/standard)
        if text in _NEXT_PAGE_TEXTS:
            return True

        # aria-label detection
        aria = (a_tag.get("aria-label") or "").lower()
        if "next" in aria or "następna" in aria:
            return True

        # URL-based detection (Page=N+1 in href)
        m = _RE_NEXT_PAGE_HREF.search(href)
        if m and int(m.group(1)) == next_page_num:
            return True

    # Check <link rel="next"> in head
    if soup.find("link", rel="next"):
        return True

    return False


# The portal sets og:title to the generic site name on all pages — skip it.
_GENERIC_SITE_TITLE_FRAGMENT = "Portal obwieszczeń i licytacji"


def _is_land_listing(payload: "ExtractionPayload") -> bool:
    """Return True if the listing appears to be about land (grunty/działki).

    TerraZoning targets land parcels only. Apartments, garages, and commercial
    units are excluded even if they are in the correct province.

    Logic:
      - Definite non-land signals in title/text → False
      - Land signal present → True
      - Neither signal found → True (allow through, better to over-include)
    """
    text = " ".join(filter(None, [payload.title, payload.raw_text]))
    if _RE_NON_LAND_SIGNAL.search(text):
        return False
    return True   # ambiguous or confirmed land → keep


def _extract_title(soup: BeautifulSoup) -> Optional[str]:
    """Extract listing title — try multiple heading/meta selectors.

    og:title is intentionally skipped: the portal sets it to the generic
    site name ("Portal obwieszczeń i licytacji — System elektronicznych licytacji")
    on every page, so it provides zero listing-specific signal.
    h1 is the authoritative listing title on the new portal layout.
    """
    for sel in (
        {"name": "h1"},
        {"class_": re.compile(r"notice.?title|listing.?title|page.?title|tytul", re.I)},
        {"name": "h2"},
        {"name": "h3"},
    ):
        el = soup.find(**sel)
        if el:
            text = el.get_text(strip=True)
            if text and _GENERIC_SITE_TITLE_FRAGMENT not in text:
                return text

    # Last resort: og:title (filtered)
    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        t = og_title["content"].strip()
        if _GENERIC_SITE_TITLE_FRAGMENT not in t:
            return t
    return None


def _extract_price_structured(soup: BeautifulSoup) -> Optional[Decimal]:
    """Extract cena wywołania from structured table/definition-list cells.

    More reliable than free-text regex: avoids false matches on
    hipoteka values, opłaty sądowe, or other monetary figures.
    """
    # Strategy 1: table row with price label in first cell
    for row in soup.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) >= 2:
            label_text = cells[0].get_text(strip=True)
            if _RE_PRICE_LABEL.search(label_text):
                price = _parse_price_value(cells[1].get_text(strip=True))
                if price is not None:
                    logger.debug("[Crawler] Structured price from table: %s", price)
                    return price

    # Strategy 2: <dt>label</dt><dd>value</dd>
    for dt in soup.find_all("dt"):
        if _RE_PRICE_LABEL.search(dt.get_text()):
            dd = dt.find_next_sibling("dd")
            if dd:
                price = _parse_price_value(dd.get_text(strip=True))
                if price is not None:
                    return price

    # Strategy 3: <span>/<div> labelled via class "price" / "cena"
    for el in soup.find_all(class_=re.compile(r"\b(?:price|cena|wywołania)\b", re.I)):
        price = _parse_price_value(el.get_text(strip=True))
        if price is not None:
            return price

    return None


def _parse_price_value(text: str) -> Optional[Decimal]:
    """Parse Polish price string → Decimal.

    Handles: '853 333,33 zł', '853333.33', '1.234.567,89 PLN', '853 333 zł'
    """
    return parse_price_value(text)


def _compute_dedup_hash(source_url: str, raw_text: str) -> str:
    payload = (source_url + raw_text).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _log_empty_diagnostics(
    html: str, province_name: str, page: int, category_id: object
) -> None:
    """Log structured diagnostics when a filter page returns 0 listing links."""
    soup = BeautifulSoup(html, "lxml")
    title_el = soup.find("title")
    title_text = title_el.get_text(strip=True) if title_el else "—"

    has_new_links = bool(soup.find("a", href=_RE_NEW_DETAIL))
    has_old_links = bool(soup.find("a", href=_RE_OLD_DETAIL))
    has_table = bool(soup.find("table"))
    body_snippet = soup.get_text()[:300].strip().replace("\n", " ")

    logger.warning(
        "[Crawler] SEVERITY:LOW — 0 listings: cat=%d prov=%s page=%d "
        "page_title=%r has_new_links=%s has_old_links=%s has_table=%s",
        category_id, province_name, page, title_text,
        has_new_links, has_old_links, has_table,
    )
    logger.debug("[Crawler] Body snippet: %r", body_snippet[:200])

    # Detect SPA shell (minimal text, lots of <script>)
    script_count = len(soup.find_all("script"))
    text_len = len(soup.get_text())
    if script_count > 5 and text_len < 2000:
        logger.warning(
            "[Crawler] SEVERITY:MEDIUM — Possible SPA shell detected: "
            "scripts=%d text_len=%d — portal may require Playwright for JS rendering",
            script_count, text_len,
        )


# ---------------------------------------------------------------------------
# Standalone helpers for dry-run mode
# ---------------------------------------------------------------------------

async def _fetch_and_parse_detail_standalone(
    http: httpx.AsyncClient,
    url: str,
) -> Optional[ExtractionPayload]:
    """Non-class version for dry-run / testing without a DB session."""
    llm_extractor = LLMExtractor()
    try:
        resp = await http.get(url)
        resp.raise_for_status()
    except (httpx.HTTPStatusError, httpx.RequestError) as exc:
        logger.error("[Standalone] Detail fetch failed %s: %s", url, exc)
        await llm_extractor.aclose()
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    title = _extract_title(soup)

    content_el = (
        soup.find("div", class_=re.compile(r"\bcol-(?:sm|md|lg)-(?:8|9|10)\b", re.I))
        or soup.find("div", id=re.compile(r"^(?:content|main|notice|ogłoszenie)", re.I))
        or soup.find("main")
        or soup.find("article")
        or soup.body
    )
    if content_el is None:
        return None

    raw_text = unicodedata.normalize("NFC", content_el.get_text(separator=" ", strip=True))
    if len(raw_text) < _MIN_CONTENT_CHARS:
        raw_text = unicodedata.normalize("NFC", soup.get_text(separator=" ", strip=True))
    if len(raw_text) < _MIN_CONTENT_CHARS:
        await llm_extractor.aclose()
        return None

    price = _extract_price_structured(soup) or _extract_price(raw_text)
    kw_matches = extract_kw_from_text(raw_text, source=ExtractionSource.FREE_TEXT_REGEX)
    primary_kw = kw_matches[0] if kw_matches else None
    gmina, powiat, woj = _extract_location(raw_text)
    parcel_result = await extract_with_fallback(
        raw_text,
        title=title,
        raw_gmina=gmina,
        raw_kw=primary_kw.normalized if primary_kw else None,
        llm_extractor=llm_extractor,
    )
    parcel_matches = parcel_result.parcel_matches
    primary_parcel = parcel_result.primary_parcel
    obreb_name = parcel_result.obreb_name
    if not gmina and parcel_result.municipality:
        gmina = parcel_result.municipality
    area = _extract_area(raw_text)
    if area is None and parcel_result.area_text:
        llm_area = parse_polish_decimal(parcel_result.area_text)
        if llm_area is not None and Decimal("0.5") <= llm_area <= Decimal("10000000"):
            area = llm_area
    kw_conf = primary_kw.confidence if primary_kw else 0.0
    parcel_conf = primary_parcel.confidence if primary_parcel else 0.0

    payload = ExtractionPayload(
        source_url=url,
        title=title,
        raw_text=raw_text,
        price_zl=price,
        area_m2=area,
        auction_date=_extract_date(raw_text),
        raw_kw=primary_kw.normalized if primary_kw else None,
        raw_numer_dzialki=primary_parcel.numer if primary_parcel else None,
        raw_obreb=obreb_name,
        raw_gmina=gmina,
        raw_powiat=powiat,
        raw_wojewodztwo=woj,
        sygnatura_akt=_extract_sygnatura(raw_text),
        all_kw_matches=[_serialise_kw(m) for m in kw_matches],
        all_parcel_matches=[_serialise_parcel(m) for m in parcel_matches],
        kw_check_valid=primary_kw.check_valid if primary_kw else False,
        kw_court_known=primary_kw.court_known if primary_kw else False,
        extraction_confidence=round(
            (kw_conf + parcel_conf) / 2 if (kw_conf or parcel_conf) else 0.0, 2
        ),
        llm_fallback_used=parcel_result.llm_used,
        llm_extraction=parcel_result.llm_extraction,
    )
    await llm_extractor.aclose()
    return payload
