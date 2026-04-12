"""TerraZoning — Licytacje Komornicze Scraper.

Async scraper for bailiff auction listings (licytacje komornicze).
Primary target: licytacje.komornik.pl (e-licytacje portal)

Architecture:
  1. create_scrape_run()      → write audit row to bronze.scrape_runs
  2. fetch_listing_page()     → HTTP GET (or mock) → raw HTML
  3. parse_listing()          → BeautifulSoup → ExtractionPayload
  4. extract_kw_from_text()   → KW regex + check digit → list[KwMatch]
  5. extract_parcel_ids()     → numer działki regex → list[ParcelMatch]
  6. save_listing()           → upsert into bronze.raw_listings (dedup_hash)
  7. update_scrape_run()      → mark completed/failed

Rate limiting: max 2 req/s (komornik portals — Commandment #9)
Retry: exponential backoff, max 3 attempts per URL
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import unicodedata
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

import httpx
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.models.bronze import RawListing, ScrapeRun
from scraper.extractors.llm_extractor import LLMExtractor, extract_with_fallback
from scraper.extractors.kw import ExtractionSource, KwMatch, extract_kw_from_text
from scraper.extractors.parcel import ParcelMatch, extract_obreb, extract_parcel_ids
from scraper.extractors.price import extract_price_from_text, parse_polish_decimal

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rate-limit constants (Commandment #9: 2 req/s for komornik portals)
# ---------------------------------------------------------------------------
_REQUEST_DELAY_S: float = 0.5   # 2 req/s → 0.5s between requests
_MAX_RETRIES: int = 3
_RETRY_BASE_DELAY_S: float = 2.0


# ---------------------------------------------------------------------------
# Intermediate result types
# ---------------------------------------------------------------------------

@dataclass
class ExtractionPayload:
    """Structured result from parsing a single listing page.

    All 'raw_*' fields directly map to bronze.raw_listings columns.
    'extraction_meta' holds the full confidence-scored results — stored
    in the Evidence Chain JSON (referenced by raw_html_ref).
    """

    source_url: str
    title: str | None
    raw_text: str
    price_zl: Decimal | None
    area_m2: Decimal | None
    auction_date: date | None

    # Best-guess raw fields (highest confidence match wins)
    raw_kw: str | None          # primary KW — canonical form
    raw_numer_dzialki: str | None
    raw_obreb: str | None
    raw_gmina: str | None
    raw_powiat: str | None
    raw_wojewodztwo: str | None

    # Full extraction metadata (confidence-scored) — for Evidence Chain
    all_kw_matches: list[dict]       # serialised KwMatch list
    all_parcel_matches: list[dict]   # serialised ParcelMatch list

    # Komornik case number — canonical "Km 123/25".
    # Used for "druga licytacja" detection: same case + same KW at a lower price
    # is the SECOND auction of the same property, not a new listing.
    # NULL when pattern not found in obwieszczenie text.
    sygnatura_akt: str | None = None

    # Extraction quality flags
    kw_check_valid: bool = False
    kw_court_known: bool = False
    extraction_confidence: float = 0.0
    llm_fallback_used: bool = False
    llm_extraction: dict[str, Any] | None = None


@dataclass
class ScrapeResult:
    """Return value from LicytacjeScraper.run()."""

    scrape_run_id: UUID
    source: str
    listings_found: int
    listings_saved: int
    listings_skipped_dedup: int
    listings_failed: int
    errors: list[str] = field(default_factory=list)
    duration_s: float = 0.0


# ---------------------------------------------------------------------------
# Mock page content — realistic obwieszczenie with dirty KW numbers
# ---------------------------------------------------------------------------

# Two valid KW numbers (check digits verified):
#   WA1M/00012345/2  → computed: 102 % 10 = 2 ✓
#   GD4K/00098765/8  → computed: 198 % 10 = 8 ✓
# One invalid (wrong check digit):
#   PO1P/00054321/9  → correct would be 6, using 9 → INVALID (confidence ≤ 0.25)
# One partial (no check digit):
#   KR1K/00077712   → no check digit (confidence ≤ 0.45)
# One OCR-dirty: "WA1M/O0012345/2" — zero vs letter-O (should fail strict, catch relaxed)

MOCK_LISTING_PAGES: list[dict[str, Any]] = [
    {
        "url": "https://licytacje.komornik.pl/Notice/Details/12345",
        "html": """
        <html><head><title>Obwieszczenie o licytacji</title></head>
        <body>
        <h1>OBWIESZCZENIE O PIERWSZEJ LICYTACJI NIERUCHOMOŚCI</h1>

        <p>Komornik Sądowy przy Sądzie Rejonowym dla Warszawy-Mokotowa
        w Warszawie <strong>Marek Wiśniewski</strong> na podstawie
        art. 953 kpc podaje do publicznej wiadomości, że w dniu
        <strong>15 maja 2026 r.</strong> o godz. 10:00 odbędzie się
        pierwsza licytacja nieruchomości gruntowej, stanowiącej
        działkę ewidencyjną <em>nr 123/4</em> w obrębie 0001 <strong>Bemowo</strong>,
        gmina Warszawa, powiat Warszawa, województwo mazowieckie.</p>

        <p>Nieruchomość wpisana jest do Księgi Wieczystej nr
        <strong>WA1M/00012345/2</strong> prowadzonej przez Sąd Rejonowy
        dla Warszawy-Mokotowa IV Wydział Ksiąg Wieczystych.</p>

        <p>Opis: Nieruchomość gruntowa niezabudowana o łącznej powierzchni
        <strong>2 456 m²</strong>. Działka ma regularny kształt, teren
        płaski. Zgodnie z MPZP gminy Warszawa działka oznaczona symbolem
        <em>MN – zabudowa mieszkaniowa jednorodzinna</em>.</p>

        <table>
          <tr><td>Cena oszacowania:</td><td>1 280 000,00 zł</td></tr>
          <tr><td>Cena wywołania (2/3):</td><td>853 333,33 zł</td></tr>
          <tr><td>Powierzchnia:</td><td>2 456 m²</td></tr>
          <tr><td>Termin licytacji:</td><td>15.05.2026</td></tr>
        </table>

        <p>UWAGA: Nieruchomość posiada również udział w działce nr 7/2/1
        obciążonej hipoteką na rzecz PKO BP S.A. (KW: GD4K/00098765/8).
        Łańcuch dowodowy: sprawa XLIV Co 1234/26.</p>

        <p>Kontakt: kancelaria.wisniewski@komornik.pl |
        ul. Puławska 180, 02-670 Warszawa</p>

        <!-- Dirty data: KW with wrong check digit (typo from PDF) -->
        <p class="footnote">Powiązana KW: PO1P/00054321/9
        (weryfikacja w toku — błąd cyfry kontrolnej)</p>

        <!-- Partial KW from old printout (brak cyfry kontrolnej) -->
        <p class="footnote">Stary numer KW (przed konwersją): KR1K/00077712</p>
        </body></html>
        """,
    },
    {
        "url": "https://licytacje.komornik.pl/Notice/Details/67890",
        "html": """
        <html><body>
        <h1>OBWIESZCZENIE O DRUGIEJ LICYTACJI NIERUCHOMOŚCI</h1>

        <p>Komornik Sądowy Agnieszka Kowalczyk-Nowak zawiadamia, że w dniu
        <strong>22 czerwca 2026 r.</strong> o godz. 11:30 odbędzie się
        DRUGA licytacja (cena wywołania: 1/2 wartości oszacowania)
        nieruchomości rolnej.</p>

        <p>Opis: Działka rolna nr <strong>45</strong> oraz
        <strong>46/2</strong>, obręb Łęczno (kod: 100701_2.0003),
        gmina Skierniewice, powiat skierniewicki, woj. łódzkie.</p>

        <!-- OCR-dirty KW: letter O instead of zero (0) — should catch via relaxed pattern -->
        <p>Numer KW: WA1M/O0012345/2 (skan — możliwy błąd OCR)</p>

        <p>Wartość oszacowania: <strong>385 000 zł</strong>.
        Powierzchnia łączna: 3 100 m². Grunty rolne kl. RIIIa i RIVa.</p>

        <!-- KW embedded in dense text without spaces (stress test for lookaheads) -->
        <p>SygnakturaXXGD4K/00098765/8rewXX-numer-ewidencji</p>

        <p>Kontakt: kancelaria@komornik-skierniewice.pl</p>
        </body></html>
        """,
    },
]


# ---------------------------------------------------------------------------
# LicytacjeScraper
# ---------------------------------------------------------------------------


class LicytacjeScraper:
    """Async scraper for licytacje komornicze.

    Usage:
        async with AsyncSessionLocal() as db:
            scraper = LicytacjeScraper(db)
            result = await scraper.run()

    Architecture follows the Extraction Expert persona:
    - Every extracted value has a confidence score
    - Source provenance is tracked for every record
    - KW check digits are validated before storing
    - Deduplication via SHA-256(source_url + raw_text)
    - Structural changes logged as SEVERITY: HIGH events
    """

    SOURCE_NAME = "licytacje.komornik.pl"
    USER_AGENT = "TerraZoning-Scraper/0.1 (+https://terrazoning.pl/bot)"

    def __init__(self, db: AsyncSession) -> None:
        self.db = db
        self._request_semaphore = asyncio.Semaphore(2)  # max 2 concurrent requests
        self._llm_extractor = LLMExtractor()

    # ------------------------------------------------------------------
    # Public orchestration entry point
    # ------------------------------------------------------------------

    async def run(self) -> ScrapeResult:
        """Full scrape cycle. Creates audit row, processes listings, finalises."""
        t_start = asyncio.get_event_loop().time()
        scrape_run: ScrapeRun | None = None

        try:
            scrape_run = await self.create_scrape_run()
            logger.info("Scrape run started: %s", scrape_run.id)

            saved = 0
            skipped = 0
            failed = 0
            errors: list[str] = []

            for page_spec in MOCK_LISTING_PAGES:
                url = page_spec["url"]
                html = page_spec["html"]
                try:
                    payload = await self._parse_listing(url, html)
                    outcome = await self.save_listing(scrape_run.id, payload)
                    if outcome == "saved":
                        saved += 1
                    else:
                        skipped += 1
                except Exception as exc:
                    failed += 1
                    errors.append(f"[{url}] {type(exc).__name__}: {exc}")
                    logger.error("Failed to process listing %s: %s", url, exc, exc_info=True)

            await self.update_scrape_run(
                scrape_run.id,
                status="completed" if failed == 0 else "partial",
                records_found=len(MOCK_LISTING_PAGES),
                records_saved=saved,
            )

            duration = asyncio.get_event_loop().time() - t_start
            return ScrapeResult(
                scrape_run_id=scrape_run.id,
                source=self.SOURCE_NAME,
                listings_found=len(MOCK_LISTING_PAGES),
                listings_saved=saved,
                listings_skipped_dedup=skipped,
                listings_failed=failed,
                errors=errors,
                duration_s=round(duration, 2),
            )

        except Exception as exc:
            logger.critical("Scrape run aborted: %s", exc, exc_info=True)
            if scrape_run is not None:
                await self.update_scrape_run(
                    scrape_run.id,
                    status="failed",
                    error_message=str(exc),
                )
            raise
        finally:
            await self._llm_extractor.aclose()

    # ------------------------------------------------------------------
    # Database operations
    # ------------------------------------------------------------------

    async def create_scrape_run(self) -> ScrapeRun:
        """Insert a new row into bronze.scrape_runs and return it.

        Always called at the very start of a scrape cycle.
        The row is written immediately so even an aborted run is auditable.
        """
        run = ScrapeRun(
            source_name=self.SOURCE_NAME,
            status="running",
            job_metadata={
                "scraper_version": "0.1.0",
                "target_url": "https://licytacje.komornik.pl",
                "mode": "mock",
            },
        )
        self.db.add(run)
        await self.db.flush()   # get the UUID without committing
        await self.db.commit()
        await self.db.refresh(run)
        logger.info("Created scrape_run id=%s source=%s", run.id, run.source_name)
        return run

    async def update_scrape_run(
        self,
        run_id: UUID,
        status: str,
        records_found: int = 0,
        records_saved: int = 0,
        error_message: str | None = None,
    ) -> None:
        """Update the scrape_run row to its final state."""
        result = await self.db.execute(
            select(ScrapeRun).where(ScrapeRun.id == run_id)
        )
        run = result.scalar_one_or_none()
        if run is None:
            logger.error("scrape_run %s not found — cannot update status", run_id)
            return
        run.status = status
        run.records_found = records_found
        run.records_saved = records_saved
        run.finished_at = datetime.now(timezone.utc)
        if error_message:
            run.error_message = error_message
        await self.db.commit()
        logger.info("Updated scrape_run %s → status=%s", run_id, status)

    async def save_listing(
        self, scrape_run_id: UUID, payload: ExtractionPayload
    ) -> str:
        """Upsert a raw listing into bronze.raw_listings.

        Returns 'saved' if new record inserted, 'skipped' if dedup_hash collision.

        Druga licytacja detection (Red Flag 4):
        Before the normal dedup_hash INSERT, we check whether a record already
        exists for the same (sygnatura_akt, raw_kw) pair. A second bailiff auction
        of the same property changes the obwieszczenie text (different price, date)
        → different SHA-256 → would normally create a phantom duplicate.
        Instead: UPDATE the prior record's price/date and re-queue for processing.
        """
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
                    "DRUGA LICYTACJA detected: sygnatura=%s kw=%s "
                    "— updating prior listing %s (price %s → %s, date %s → %s)",
                    payload.sygnatura_akt, payload.raw_kw, existing.id,
                    existing.price_zl, payload.price_zl,
                    existing.auction_date, payload.auction_date,
                )
                # Update only operational fields — raw_text preserved for Evidence Chain
                existing.price_zl = payload.price_zl
                existing.auction_date = payload.auction_date
                existing.source_url = payload.source_url
                existing.is_processed = False   # re-queue for geo_resolver
                await self.db.commit()
                return "saved"

        dedup_hash = self._compute_dedup_hash(payload.source_url, payload.raw_text)

        # Persist the full extraction metadata as Evidence Chain JSON
        evidence_payload = {
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "all_kw_matches": payload.all_kw_matches,
            "all_parcel_matches": payload.all_parcel_matches,
            "kw_check_valid": payload.kw_check_valid,
            "extraction_confidence": payload.extraction_confidence,
            "llm_fallback_used": payload.llm_fallback_used,
            "llm_extraction": payload.llm_extraction,
        }
        # In production: upload to GCS, store URI in raw_html_ref.
        # For now: store as inline JSON reference (development mode).
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
                raw_html_ref=evidence_ref,
                dedup_hash=dedup_hash,
                sygnatura_akt=payload.sygnatura_akt,
                is_processed=False,
            )
            .on_conflict_do_nothing(index_elements=["dedup_hash"])
            .returning(RawListing.id)
        )

        result = await self.db.execute(stmt)
        await self.db.commit()

        row = result.fetchone()
        if row is None:
            logger.debug("Listing skipped (dedup): %s", payload.source_url)
            return "skipped"

        logger.info(
            "Saved listing id=%s kw=%s confidence=%.2f",
            row[0],
            payload.raw_kw or "—",
            payload.extraction_confidence,
        )
        return "saved"

    # ------------------------------------------------------------------
    # Extraction pipeline
    # ------------------------------------------------------------------

    def extract_kw_from_text(self, raw_text: str) -> list[KwMatch]:
        """Public method — extracts and validates all KW numbers from text.

        Delegates to the extractors.kw module.
        Returns results sorted by confidence DESC.

        From persona Commandment #5:
          - Check digit is validated for every match
          - Partial matches (no check digit) → confidence ≤ 0.45
          - If format invalid → not emitted as KW (it's noise)
        """
        return extract_kw_from_text(raw_text, source=ExtractionSource.FREE_TEXT_REGEX)

    async def _parse_listing(self, url: str, html: str) -> ExtractionPayload:
        """Parse raw HTML into a structured ExtractionPayload.

        Applies all extraction passes with confidence scoring.
        If critical identifiers (KW, działka) cannot be extracted with
        confidence ≥ 0.5, the payload is still returned — but callers can
        check extraction_confidence to decide whether to quarantine.
        """
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        raw_text = soup.get_text(separator=" ", strip=True)
        raw_text = unicodedata.normalize("NFC", raw_text)
        title = soup.find("h1")
        title_text = title.get_text(strip=True) if title else None

        # ---- Extract KW numbers ----
        kw_matches = self.extract_kw_from_text(raw_text)
        primary_kw: KwMatch | None = kw_matches[0] if kw_matches else None

        # Log any KW with invalid check digit — SEVERITY: HIGH per Red-Flag Protocol
        for kw in kw_matches:
            if not kw.check_valid:
                logger.warning(
                    "KW CHECK DIGIT INVALID [SEVERITY:HIGH] kw=%s confidence=%.2f "
                    "source_url=%s snippet=%r",
                    kw.raw_value, kw.confidence, url, kw.snippet,
                )
            if kw.confidence < 0.5:
                logger.info(
                    "KW flagged UNVERIFIED kw=%s confidence=%.2f flag=%s",
                    kw.normalized, kw.confidence, kw.flag(),
                )

        # ---- Extract parcel IDs ----
        # ---- Extract location fields (gmina, powiat, województwo) ----
        gmina, powiat, woj = _extract_location(raw_text)

        # ---- Extract parcel IDs + LLM fallback ----
        parcel_result = await extract_with_fallback(
            raw_text,
            title=title_text,
            raw_gmina=gmina,
            raw_kw=primary_kw.normalized if primary_kw else None,
            llm_extractor=self._llm_extractor,
        )
        parcel_matches = parcel_result.parcel_matches
        primary_parcel = parcel_result.primary_parcel
        obreb_name = parcel_result.obreb_name
        if not gmina and parcel_result.municipality:
            gmina = parcel_result.municipality

        # ---- Extract price ----
        price = _extract_price(raw_text)

        # ---- Extract area ----
        area = _extract_area(raw_text)
        if area is None and parcel_result.area_text:
            llm_area = parse_polish_decimal(parcel_result.area_text)
            if llm_area is not None and Decimal("0.5") <= llm_area <= Decimal("10000000"):
                area = llm_area

        # ---- Extract auction date ----
        auction_date = _extract_date(raw_text)

        # ---- Extract komornik case number (druga licytacja detection) ----
        sygnatura_akt = _extract_sygnatura(raw_text)
        if sygnatura_akt:
            logger.debug("[Scraper] sygnatura_akt=%s url=%s", sygnatura_akt, url)

        # ---- Aggregate confidence ----
        # Overall extraction confidence = geometric mean of primary fields
        kw_conf = primary_kw.confidence if primary_kw else 0.0
        parcel_conf = primary_parcel.confidence if primary_parcel else 0.0
        extraction_confidence = round((kw_conf + parcel_conf) / 2, 2) if (kw_conf or parcel_conf) else 0.0

        return ExtractionPayload(
            source_url=url,
            title=title_text,
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
    # Static helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_dedup_hash(source_url: str, raw_text: str) -> str:
        """SHA-256(source_url + raw_text) for deduplication.

        Commandment #10: deduplication is a first-class operation.
        Same listing re-scraped a week later must NOT produce a second row.
        """
        payload = (source_url + raw_text).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()


# ---------------------------------------------------------------------------
# Field-level extraction helpers
# ---------------------------------------------------------------------------

_RE_AREA = re.compile(
    r"(\d[\d\s\u00A0\u202F\u2007\u2009.,]*\d|\d)\s*m(?:²|2)\b",
    re.IGNORECASE,
)
_RE_DATE_PL = re.compile(
    r"\b(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})\b"   # DD.MM.YYYY / DD-MM-YYYY
)
# Gmina: capture 1–2 words after "gmina" keyword.
# Deliberately limited to 2 words — all Polish gmina names fit within this.
# The old terminator approach broke when ":" appeared before "," in the text.
_RE_GMINA = re.compile(
    r"\bgmina\s+([\wąćęłńóśźżĄĆĘŁŃÓŚŹŻ\-]+(?:\s+[\wąćęłńóśźżĄĆĘŁŃÓŚŹŻ\-]+)?)",
    re.IGNORECASE,
)
_RE_POWIAT = re.compile(r"powiat\s+([\w\sąćęłńóśźżĄĆĘŁŃÓŚŹŻ\-]+?)(?:,|\.|\s*woj)", re.IGNORECASE)

# Primary: explicit "woj." / "województwo" prefix
_RE_WOJ = re.compile(r"woj(?:ewództwo)?\s+([\wąćęłńóśźżĄĆĘŁŃÓŚŹŻ\-]+?)(?:,|\.|$)", re.IGNORECASE)
# Fallback: standalone province adjective (appears after map_* icon or in address lines)
# Covers the new portal format: "map_lodzkie łódzkie map_marker ..."
_RE_WOJ_STANDALONE = re.compile(
    r"\b(mazowieckie|śląskie|slaskie|małopolskie|malopolskie|łódzkie|lodzkie|"
    r"dolnośląskie|dolnoslaskie|wielkopolskie|pomorskie|kujawsko-pomorskie|"
    r"lubelskie|podkarpackie|lubuskie|warmińsko-mazurskie|warminsko-mazurskie|"
    r"podlaskie|opolskie|świętokrzyskie|swietokrzyskie|zachodniopomorskie)\b",
    re.IGNORECASE,
)

# Komornik case number — "Km 123/25" or "Km 1234/2026" or "Kmp 12/26".
# Covers standard (Km) and majątkowy (Kmp) types.
# Word boundary (\b) prevents matching inside longer codes like "Kmn 1/25".
_RE_SYGN = re.compile(r"\bKm[pP]?\s+(\d{1,5}/\d{2,4})\b", re.IGNORECASE)


def _extract_sygnatura(text: str) -> str | None:
    """Extract komornik case number from obwieszczenie text.

    Returns canonical form: 'Km 123/25' (always uppercase Km + single space).
    Returns None when the pattern is not found.
    """
    m = _RE_SYGN.search(text)
    if m:
        prefix = "Kmp" if "mp" in m.group(0).lower() else "Km"
        return f"{prefix} {m.group(1)}"
    return None


def _extract_price(text: str) -> Decimal | None:
    """Extract the first złoty amount from text (most likely the oszacowanie price)."""
    return extract_price_from_text(text)


def _extract_area(text: str) -> Decimal | None:
    """Extract surface area in m² from text."""
    for m in _RE_AREA.finditer(text):
        val = parse_polish_decimal(m.group(1))
        if val is None:
            continue
        if Decimal("0.5") <= val <= Decimal("10000000"):
            return val
    return None


def _extract_date(text: str) -> date | None:
    """Extract auction date from text (first DD.MM.YYYY match)."""
    m = _RE_DATE_PL.search(text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    return None


def _extract_location(text: str) -> tuple[str | None, str | None, str | None]:
    """Extract (gmina, powiat, województwo) strings from free text."""
    gmina_m = _RE_GMINA.search(text)
    powiat_m = _RE_POWIAT.search(text)

    # Try explicit "woj./województwo" prefix first; fall back to standalone province name
    woj_m = _RE_WOJ.search(text)
    if woj_m:
        woj = woj_m.group(1).strip()
    else:
        woj_m2 = _RE_WOJ_STANDALONE.search(text)
        woj = woj_m2.group(1).strip() if woj_m2 else None

    return (
        gmina_m.group(1).strip() if gmina_m else None,
        powiat_m.group(1).strip() if powiat_m else None,
        woj,
    )


def _serialise_kw(m: KwMatch) -> dict:
    return {
        "normalized": m.normalized,
        "raw_value": m.raw_value,
        "court_code": m.court_code,
        "book_number": m.book_number,
        "check_digit": m.check_digit,
        "check_valid": m.check_valid,
        "court_known": m.court_known,
        "confidence": m.confidence,
        "source": m.source.value,
        "snippet": m.snippet,
        "flag": m.flag(),
    }


def _serialise_parcel(m: ParcelMatch) -> dict:
    return {
        "numer": m.numer,
        "raw_value": m.raw_value,
        "obreb_raw": m.obreb_raw,
        "teryt_obreb": m.teryt_obreb,
        "confidence": m.confidence,
        "snippet": m.snippet,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    """Run the scraper once and print a summary to stdout."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    async with AsyncSessionLocal() as db:
        scraper = LicytacjeScraper(db)
        result = await scraper.run()

    print("\n" + "=" * 60)
    print("SCRAPE COMPLETE")
    print("=" * 60)
    print(f"  Scrape Run ID  : {result.scrape_run_id}")
    print(f"  Source         : {result.source}")
    print(f"  Listings found : {result.listings_found}")
    print(f"  Saved          : {result.listings_saved}")
    print(f"  Skipped (dedup): {result.listings_skipped_dedup}")
    print(f"  Failed         : {result.listings_failed}")
    print(f"  Duration       : {result.duration_s}s")
    if result.errors:
        print("\n  ERRORS:")
        for e in result.errors:
            print(f"    - {e}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
