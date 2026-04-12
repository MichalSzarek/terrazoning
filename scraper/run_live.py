"""run_live.py — Live-Fire Test: crawl licytacje.komornik.pl → PostGIS.

Usage:
    # From scraper/ directory, with the backend venv active and DB running:
    cd scraper
    python run_live.py

    # Single province, one page (safest first run):
    python run_live.py --provinces slaskie --max-pages 1

    # Both target provinces, two pages each:
    python run_live.py --provinces slaskie malopolskie --max-pages 2

    # Dry-run: fetch + parse only, no DB writes (good for verifying selectors):
    python run_live.py --dry-run --provinces slaskie --max-pages 1 --verbose

    # Skip province post-filter (get all-Poland nieruchomości):
    python run_live.py --no-province-filter --max-pages 1

Environment:
    DATABASE_URL must be set (inherits from backend/.env via docker-compose).
    Explicit override:
        export DATABASE_URL="postgresql+asyncpg://terrazoning:terrazoning@localhost:5432/terrazoning"

Portal entry points (as of April 2026):
    /Notice/Filter/28    — grunty (land/plots) — primary target
    /Notice/Filter/-3    — wszystkie nieruchomości — fallback sweep
    Detail pages support both:
        /Notice/Details/{id}                         (legacy format)
        /wyszukiwarka/obwieszczenia-o-licytacji/{id}/...  (new format)

After a successful run:
    cd ../backend && python -m app.services.geo_resolver
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime

# ---------------------------------------------------------------------------
# Logging — configure BEFORE any imports that use logger at module level
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

logger = logging.getLogger("run_live")

# ---------------------------------------------------------------------------
# Application imports (after logging is set up)
# ---------------------------------------------------------------------------

from app.core.database import AsyncSessionLocal  # noqa: E402
from scraper.komornik_crawler import (  # noqa: E402
    _BASE_URL,
    _CATEGORIES,
    _LISTING_URL,
    _HEADERS,
    _PAGE_SIZE,
    _PROVINCE_IDS,
    _RE_NEW_DETAIL,
    _RE_OLD_DETAIL,
    KomornikCrawler,
    _extract_listing_links,
    _fetch_and_parse_detail_standalone,
)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

_PROVINCE_MAP: dict[str, str] = {
    "malopolskie": "małopolskie",
    "małopolskie": "małopolskie",
    "slaskie":     "śląskie",
    "śląskie":     "śląskie",
}


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="TerraZoning live scraper — licytacje.komornik.pl",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--provinces",
        nargs="+",
        default=list(_PROVINCE_IDS.keys()),
        metavar="PROVINCE",
        help="Provinces to crawl. Choices: malopolskie, slaskie  (default: both)",
    )
    p.add_argument(
        "--max-pages",
        type=int,
        default=2,
        dest="max_pages",
        help="Max filter-result pages per category×province pass (default: 2)",
    )
    p.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Seconds between detail-page requests (default: 1.5)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Fetch and parse, but do NOT write to database",
    )
    p.add_argument(
        "--no-province-filter",
        action="store_true",
        dest="skip_province_filter",
        help="Save all listings regardless of extracted province (default: filter)",
    )
    p.add_argument(
        "--categories",
        nargs="+",
        default=list(_CATEGORIES.keys()),
        metavar="CAT",
        help=f"Filter categories to crawl. Available: {list(_CATEGORIES.keys())}",
    )
    p.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable DEBUG logging",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main async entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    args = _parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger("httpx").setLevel(logging.INFO)

    # Resolve province aliases
    resolved_provinces: dict[str, int] = {}
    for name in args.provinces:
        canonical = _PROVINCE_MAP.get(name.lower().strip())
        if canonical is None:
            logger.error(
                "Unknown province %r. Valid choices: %s",
                name, list(_PROVINCE_MAP.keys()),
            )
            sys.exit(1)
        resolved_provinces[canonical] = _PROVINCE_IDS[canonical]

    # Resolve category aliases
    resolved_categories: dict[str, int] = {}
    for cat in args.categories:
        if cat not in _CATEGORIES:
            logger.error(
                "Unknown category %r. Valid choices: %s", cat, list(_CATEGORIES.keys()),
            )
            sys.exit(1)
        resolved_categories[cat] = _CATEGORIES[cat]

    logger.info("=" * 60)
    logger.info("TerraZoning Live-Fire Test — licytacje.komornik.pl")
    logger.info("=" * 60)
    logger.info("Categories  : %s", list(resolved_categories.keys()))
    logger.info("Provinces   : %s", list(resolved_provinces.keys()))
    logger.info("Max pages   : %d per category×province", args.max_pages)
    logger.info("Delay       : %.1f s between details", args.delay)
    logger.info("Dry run     : %s", args.dry_run)
    logger.info("Prov.filter : %s", not args.skip_province_filter)
    logger.info("Started at  : %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[DRY RUN] No DB writes — showing parsed extraction output")
        await _run_dry(resolved_categories, args.max_pages, args.delay)
        return

    async with AsyncSessionLocal() as db:
        crawler = KomornikCrawler(
            db=db,
            categories=resolved_categories,
            provinces=resolved_provinces,
            max_pages=args.max_pages,
            detail_delay_s=args.delay,
            skip_province_filter=args.skip_province_filter,
        )
        result = await crawler.run()

    print()
    print("=" * 60)
    print("LIVE SCRAPE COMPLETE")
    print("=" * 60)
    print(f"  Scrape Run ID  : {result.scrape_run_id}")
    print(f"  Source         : {result.source}")
    print(f"  Listings found : {result.listings_found}")
    print(f"  Saved          : {result.listings_saved}")
    print(f"  Skipped (dedup): {result.listings_skipped_dedup}")
    print(f"  Failed         : {result.listings_failed}")
    print(f"  Duration       : {result.duration_s}s")

    if result.errors:
        print(f"\n  ERRORS ({len(result.errors)}):")
        for e in result.errors[:10]:
            print(f"    - {e}")
        if len(result.errors) > 10:
            print(f"    ... and {len(result.errors) - 10} more (check --verbose logs)")
    print("=" * 60)

    if result.listings_saved > 0:
        print()
        print("Next step — run the Geo-Resolver:")
        print("  cd ../backend && python -m app.services.geo_resolver")
    elif result.listings_found == 0:
        print()
        print("WARNING: 0 listings found. Diagnostic steps:")
        print("  1. Verify filter page is reachable:")
        print("     curl -L 'https://licytacje.komornik.pl/Notice/Filter/28'")
        print("  2. Re-run with --verbose to see diagnostics (SPA detection, link counts)")
        print("  3. If SPA shell detected: consider adding Playwright as fallback")
        print("  4. Check if new portal URL structure changed again:")
        print("     curl -L 'https://licytacje.komornik.pl/Notice/Filter/-3' | grep -o 'href=\"[^\"]*\"'")


# ---------------------------------------------------------------------------
# Dry-run mode
# ---------------------------------------------------------------------------

async def _run_dry(
    categories: dict[str, int],
    max_pages: int,
    delay_s: float,
) -> None:
    """Fetch + parse listings without any DB writes.

    Crawls up to 3 detail pages per category×page for quick validation
    of selectors and extraction quality. No rate-limit adjustment needed
    (we still respect the delay to avoid hammering the portal).
    """
    import httpx
    from bs4 import BeautifulSoup

    _DRY_DETAILS_PER_PAGE = 3   # cap detail fetches per search page

    async with httpx.AsyncClient(
        headers=_HEADERS,
        timeout=httpx.Timeout(30.0),
        follow_redirects=True,
    ) as http:
        # Warm up session
        warmup = await http.get(_BASE_URL)
        logger.debug("[DRY] Warm-up → HTTP %d (final URL: %s)", warmup.status_code, warmup.url)

        total_fetched = 0

        for cat_name, cat_sub in categories.items():
            logger.info("[DRY] Category: %s (subCategory=%s)", cat_name, cat_sub or "all")

            for page_idx in range(max_pages):
                params = {
                    "mainCategory": "REAL_ESTATE",
                    "province":     "śląskie",
                    "offset":       str(page_idx * _PAGE_SIZE),
                }
                if cat_sub:
                    params["subCategory"] = cat_sub
                logger.info("[DRY] GET %s (page %d)", _LISTING_URL, page_idx + 1)

                resp = await http.get(_LISTING_URL, params=params)
                logger.info(
                    "[DRY] Response: HTTP %d, final URL: %s, content-length: %d chars",
                    resp.status_code, resp.url, len(resp.text),
                )

                soup = BeautifulSoup(resp.text, "lxml")
                links = _extract_listing_links(soup)

                logger.info("[DRY] Extracted %d listing links from page %d", len(links), page_idx + 1)

                if not links:
                    # Show what we got back for debugging
                    new_count = len(soup.find_all("a", href=_RE_NEW_DETAIL))
                    old_count = len(soup.find_all("a", href=_RE_OLD_DETAIL))
                    script_count = len(soup.find_all("script"))
                    body_text_len = len(soup.get_text())
                    logger.warning(
                        "[DRY] Zero links: new_format_links=%d old_format_links=%d "
                        "scripts=%d body_text_len=%d",
                        new_count, old_count, script_count, body_text_len,
                    )
                    print(f"\n  [Page {page_idx+1}] Body snippet (first 400 chars):")
                    print(" ", soup.get_text()[:400].replace("\n", " ").strip())
                    break

                # Fetch up to N detail pages from this result page
                for idx, (url, notice_id) in enumerate(
                    links[:_DRY_DETAILS_PER_PAGE], start=1
                ):
                    logger.info(
                        "[DRY] Detail %d/%d (id=%s): %s",
                        idx, min(_DRY_DETAILS_PER_PAGE, len(links)), notice_id, url,
                    )
                    await asyncio.sleep(delay_s)

                    payload = await _fetch_and_parse_detail_standalone(http, url)
                    total_fetched += 1

                    if payload:
                        print()
                        print(f"  {'─'*54}")
                        print(f"  URL    : {url}")
                        print(f"  Title  : {payload.title or '—'}")
                        print(f"  KW     : {payload.raw_kw or '—'}"
                              f"  [valid={payload.kw_check_valid}]")
                        print(f"  Dz.    : {payload.raw_numer_dzialki or '—'}")
                        print(f"  Area   : {payload.area_m2 or '—'} m²")
                        print(f"  Price  : {payload.price_zl or '—'} zł")
                        print(f"  Sygn.  : {payload.sygnatura_akt or '—'}")
                        print(f"  Gmina  : {payload.raw_gmina or '—'}")
                        print(f"  Woj.   : {payload.raw_wojewodztwo or '—'}")
                        print(f"  Conf.  : {payload.extraction_confidence:.2f}")
                        if payload.all_kw_matches:
                            print(f"  KW all : {[m['normalized'] for m in payload.all_kw_matches]}")
                    else:
                        print(f"\n  [PARSE FAILED] {url}")

        print()
        print(f"  Dry-run complete: {total_fetched} detail pages parsed")
        print("  To write to DB: remove --dry-run flag")


if __name__ == "__main__":
    asyncio.run(main())
