"""Operational focus tool for the Gliwice city + powiat gliwicki cluster.

This script helps us stop treating Poland as one big queue when the highest-value
work is now highly local:
  - city Gliwice (powiat grodzki, TERYT powiat 2466 / gmina 2466011)
  - powiat gliwicki (TERYT powiat 2405)

What it does:
  1. reports the cluster state across Bronze / Silver / Gold,
  2. lists remaining DLQ cases in the cluster,
  3. estimates whether LLM fallback could still help on those cases,
  4. optionally syncs MPZP only for cluster gminy already present in WFS_REGISTRY,
  5. optionally replays GeoResolver only for cluster DLQ listings and then
     recalculates DeltaEngine only for cluster parcels.

Usage:
    uv run python run_gliwice_cluster.py
    uv run python run_gliwice_cluster.py --show-dlq
    uv run python run_gliwice_cluster.py --sync-mpzp
    uv run python run_gliwice_cluster.py --replay
    uv run python run_gliwice_cluster.py --sync-mpzp --replay --show-dlq
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable
from uuid import UUID

from sqlalchemy import distinct, func, or_, select, update

from app.core.database import AsyncSessionLocal
from app.models.bronze import RawListing
from app.models.gold import InvestmentLead, PlanningZone
from app.models.silver import DlqParcel, Dzialka, ListingParcel
from app.services.delta_engine import DeltaReport, run_delta_engine
from app.services.geo_resolver import (
    GeoResolver,
    ResolutionReport,
    _ULDK_INTER_REQUEST_DELAY_S,
)
from app.services.uldk import ULDKClient
from run_wfs_sync import WFS_REGISTRY, WFSSyncReport, run_wfs_sync

logger = logging.getLogger("run_gliwice_cluster")

GLIWICE_CLUSTER_POWIAT_CODES: frozenset[str] = frozenset({"2466", "2405"})
GLIWICE_CLUSTER_GMINA_LABELS: dict[str, str] = {
    "2466011": "Gliwice",
    "2405011": "Knurów",
    "2405021": "Pyskowice",
    "2405032": "Gierałtowice",
    "2405042": "Pilchowice",
    "2405053": "Rudziniec",
    "2405063": "Sośnicowice",
    "2405073": "Toszek",
    "2405082": "Wielowieś",
}
GLIWICE_CLUSTER_KEYWORDS: tuple[str, ...] = (
    "gliwic",
    "knurow",
    "pyskowic",
    "gieraltowic",
    "pilchowic",
    "rudzin",
    "sosnicowic",
    "toszek",
    "wielowies",
)

_RE_MULTI_SPACE = re.compile(r"\s+")
_RE_EXPLICIT_REGION = re.compile(r"(?i)\bobr(?:ę|e)?b(?:ie)?\.?\s*(?:nr\s*)?(?P<code>\d{1,4})\b")
_RE_ADDRESS_LOCALITY = re.compile(
    r"(?is)(?:adres nieruchomości|miejsce oględzin:\s*pod adresem)\s+"
    r"(?:\d{2}-\d{3}\s+)?(?P<name>[A-ZĄĆĘŁŃÓŚŹŻ][A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż .-]{1,80}?)"
    r"(?=,\s*poczta\b|\s+poczta\b|$)"
)
_STREET_TOKENS = ("ul.", "ulica", "aleja", "osiedle", "plac", "rondo")


@dataclass
class ClusterDLQItem:
    listing_id: UUID
    source_url: str
    title: str | None
    raw_numer_dzialki: str | None
    raw_obreb: str | None
    raw_gmina: str | None
    raw_powiat: str | None
    raw_kw: str | None
    raw_text: str | None
    attempt_count: int
    last_error: str | None
    next_retry_at: object | None
    llm_help: str
    llm_reason: str


@dataclass
class ClusterSummary:
    silver_dzialki: int
    linked_bronze: int
    leads: int
    cluster_gminy: list[tuple[str, int]]
    planning_by_gmina: list[tuple[str, int]]
    dlq_items: list[ClusterDLQItem]


def _normalize(value: str | None) -> str:
    if not value:
        return ""
    text = value.strip().replace("-", " ")
    text = text.replace("ł", "l").replace("Ł", "L")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return _RE_MULTI_SPACE.sub(" ", text).lower()


def _like_any(column) -> object:
    return or_(
        *[
            func.lower(func.coalesce(column, "")).like(f"%{keyword}%")
            for keyword in GLIWICE_CLUSTER_KEYWORDS
        ]
    )


def _cluster_listing_filter() -> object:
    return or_(
        func.lower(func.coalesce(RawListing.raw_powiat, "")).like("%gliwick%"),
        _like_any(RawListing.raw_gmina),
        _like_any(RawListing.raw_obreb),
        _like_any(RawListing.title),
        _like_any(RawListing.raw_text),
        func.coalesce(RawListing.raw_kw, "").like("GL1G/%"),
    )


def _extract_address_locality(text: str | None) -> str | None:
    if not text:
        return None
    match = _RE_ADDRESS_LOCALITY.search(text)
    if not match:
        return None
    name = match.group("name").strip(" ,.-")
    if any(token in _normalize(name) for token in _STREET_TOKENS):
        return None
    return name or None


def _assess_llm_help(
    *,
    raw_text: str | None,
    raw_numer_dzialki: str | None,
    raw_obreb: str | None,
    raw_gmina: str | None,
    raw_kw: str | None,
) -> tuple[str, str]:
    """Estimate whether LLM fallback can still improve the cluster case.

    We intentionally stay conservative here:
      - "high" means extractor data is visibly incomplete or polluted,
      - "medium" means there is locality ambiguity and text may contain clues,
      - "low" means extraction is already good and the blocker is downstream,
      - "unlikely" means the listing simply does not contain the missing signal.
    """
    text = raw_text or ""
    address_locality = _extract_address_locality(text)
    explicit_region = _RE_EXPLICIT_REGION.search(text)
    obreb_norm = _normalize(raw_obreb)
    gmina_norm = _normalize(raw_gmina)
    address_norm = _normalize(address_locality)

    if not raw_numer_dzialki:
        return "high", "No parcel number is currently extracted. LLM may still recover it from the body text."

    if not raw_obreb and address_locality:
        return "medium", "Parcel exists and the address block exposes a locality that LLM could surface."

    if not raw_obreb:
        return "high", "Parcel exists but locality is missing. LLM may help only if the text states the locality explicitly."

    if any(token in obreb_norm for token in ("ul ", "ul.", "aleja", "osiedle", "plac", "rondo")):
        return "medium", "Current locality looks like a street/address artifact. LLM may recover the true locality."

    if explicit_region:
        return "low", (
            f"Text already contains explicit obręb code {explicit_region.group('code')}. "
            "This is a resolver/integration problem, not an extraction problem."
        )

    if raw_kw and raw_numer_dzialki and raw_obreb:
        if address_locality and address_norm and address_norm != obreb_norm and address_norm != gmina_norm:
            return "medium", "Address block names a different locality than the current extraction. LLM might help surface that alternate locality."
        return "unlikely", (
            "Parcel number, city/locality and KW are already extracted. "
            "The missing signal is the cadastral precinct, and the text does not expose it."
        )

    return "low", "Extractor data already looks structurally complete. The remaining blocker is likely ULDK ambiguity."


async def _fetch_cluster_summary() -> ClusterSummary:
    async with AsyncSessionLocal() as db:
        silver_dzialki = int(
            (
                await db.execute(
                    select(func.count())
                    .select_from(Dzialka)
                    .where(Dzialka.teryt_powiat.in_(GLIWICE_CLUSTER_POWIAT_CODES))
                )
            ).scalar_one()
        )

        linked_bronze = int(
            (
                await db.execute(
                    select(func.count(distinct(RawListing.id)))
                    .select_from(RawListing)
                    .join(ListingParcel, ListingParcel.listing_id == RawListing.id)
                    .join(Dzialka, Dzialka.id == ListingParcel.dzialka_id)
                    .where(Dzialka.teryt_powiat.in_(GLIWICE_CLUSTER_POWIAT_CODES))
                )
            ).scalar_one()
        )

        leads = int(
            (
                await db.execute(
                    select(func.count())
                    .select_from(InvestmentLead)
                    .join(Dzialka, InvestmentLead.dzialka_id == Dzialka.id)
                    .where(Dzialka.teryt_powiat.in_(GLIWICE_CLUSTER_POWIAT_CODES))
                )
            ).scalar_one()
        )

        cluster_gminy = list(
            (
                await db.execute(
                    select(Dzialka.teryt_gmina, func.count())
                    .where(Dzialka.teryt_powiat.in_(GLIWICE_CLUSTER_POWIAT_CODES))
                    .group_by(Dzialka.teryt_gmina)
                    .order_by(func.count().desc(), Dzialka.teryt_gmina.asc())
                )
            ).all()
        )

        gmina_codes = [code for code, _ in cluster_gminy]
        planning_by_gmina = list(
            (
                await db.execute(
                    select(PlanningZone.teryt_gmina, func.count())
                    .where(PlanningZone.teryt_gmina.in_(gmina_codes))
                    .group_by(PlanningZone.teryt_gmina)
                    .order_by(PlanningZone.teryt_gmina.asc())
                )
            ).all()
        ) if gmina_codes else []

        rows = (
            await db.execute(
                select(
                    RawListing.id,
                    RawListing.source_url,
                    RawListing.title,
                    RawListing.raw_numer_dzialki,
                    RawListing.raw_obreb,
                    RawListing.raw_gmina,
                    RawListing.raw_powiat,
                    RawListing.raw_kw,
                    RawListing.raw_text,
                    DlqParcel.attempt_count,
                    DlqParcel.last_error,
                    DlqParcel.next_retry_at,
                )
                .join(DlqParcel, DlqParcel.listing_id == RawListing.id)
                .where(_cluster_listing_filter())
                .order_by(DlqParcel.attempt_count.desc(), RawListing.created_at.desc())
            )
        ).all()

    dlq_items = [
        ClusterDLQItem(
            listing_id=row.id,
            source_url=row.source_url,
            title=row.title,
            raw_numer_dzialki=row.raw_numer_dzialki,
            raw_obreb=row.raw_obreb,
            raw_gmina=row.raw_gmina,
            raw_powiat=row.raw_powiat,
            raw_kw=row.raw_kw,
            raw_text=row.raw_text,
            attempt_count=row.attempt_count,
            last_error=row.last_error,
            next_retry_at=row.next_retry_at,
            llm_help=_assess_llm_help(
                raw_text=row.raw_text,
                raw_numer_dzialki=row.raw_numer_dzialki,
                raw_obreb=row.raw_obreb,
                raw_gmina=row.raw_gmina,
                raw_kw=row.raw_kw,
            )[0],
            llm_reason=_assess_llm_help(
                raw_text=row.raw_text,
                raw_numer_dzialki=row.raw_numer_dzialki,
                raw_obreb=row.raw_obreb,
                raw_gmina=row.raw_gmina,
                raw_kw=row.raw_kw,
            )[1],
        )
        for row in rows
    ]

    return ClusterSummary(
        silver_dzialki=silver_dzialki,
        linked_bronze=linked_bronze,
        leads=leads,
        cluster_gminy=cluster_gminy,
        planning_by_gmina=planning_by_gmina,
        dlq_items=dlq_items,
    )


async def _cluster_dzialka_ids() -> list[UUID]:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Dzialka.id)
            .where(Dzialka.teryt_powiat.in_(GLIWICE_CLUSTER_POWIAT_CODES))
            .order_by(Dzialka.created_at.asc())
        )
        return list(result.scalars().all())


async def _sync_cluster_mpzp(gmina_codes: Iterable[str]) -> list[WFSSyncReport]:
    reports: list[WFSSyncReport] = []
    for gmina_code in gmina_codes:
        if gmina_code not in WFS_REGISTRY:
            logger.info(
                "[GliwiceCluster] Skipping MPZP sync for %s (%s) — no registry entry",
                gmina_code,
                GLIWICE_CLUSTER_GMINA_LABELS.get(gmina_code, "unknown"),
            )
            continue
        reports.append(await run_wfs_sync(teryt_filter=gmina_code))
    return reports


async def _requeue_cluster_dlq(listing_ids: list[UUID]) -> int:
    if not listing_ids:
        return 0
    async with AsyncSessionLocal() as db:
        await db.execute(
            update(DlqParcel)
            .where(DlqParcel.listing_id.in_(listing_ids))
            .values(next_retry_at=func.now())
        )
        bronze_update = await db.execute(
            update(RawListing)
            .where(RawListing.id.in_(listing_ids))
            .values(is_processed=False)
        )
        await db.commit()
        return bronze_update.rowcount or 0


async def _run_cluster_replay(listing_ids: list[UUID]) -> ResolutionReport:
    report = ResolutionReport()
    if not listing_ids:
        return report

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(RawListing)
            .where(RawListing.id.in_(listing_ids))
            .order_by(RawListing.created_at.asc())
        )
        listings = list(result.scalars().all())

        async with ULDKClient() as uldk:
            resolver = GeoResolver(db, uldk)
            for listing in listings:
                outcome = await resolver._resolve_listing(listing)
                if outcome == "resolved":
                    report.resolved += 1
                elif outcome == "dlq":
                    report.sent_to_dlq += 1
                else:
                    report.already_resolved += 1
                report.total_processed += 1
                await asyncio.sleep(_ULDK_INTER_REQUEST_DELAY_S)

    return report


def _print_summary(summary: ClusterSummary, show_dlq: bool) -> None:
    print(f"\n{'=' * 60}")
    print("GLIWICE CLUSTER REPORT")
    print(f"{'=' * 60}")
    print("  Scope                : Gliwice city + powiat gliwicki")
    print(f"  Silver dzialki       : {summary.silver_dzialki}")
    print(f"  Bronze linked        : {summary.linked_bronze}")
    print(f"  Gold leads           : {summary.leads}")
    print(f"  DLQ rows             : {len(summary.dlq_items)}")
    print(f"{'=' * 60}")

    print("\nResolved gminy:")
    if not summary.cluster_gminy:
        print("  - none")
    for code, count in summary.cluster_gminy:
        label = GLIWICE_CLUSTER_GMINA_LABELS.get(code, "unknown")
        print(f"  - {code} {label}: {count} działka(i)")

    print("\nPlanning coverage:")
    if not summary.planning_by_gmina:
        print("  - none")
    for code, count in summary.planning_by_gmina:
        label = GLIWICE_CLUSTER_GMINA_LABELS.get(code, "unknown")
        print(f"  - {code} {label}: {count} stref")

    llm_buckets: dict[str, int] = {}
    for item in summary.dlq_items:
        llm_buckets[item.llm_help] = llm_buckets.get(item.llm_help, 0) + 1

    print("\nLLM usefulness estimate:")
    if not llm_buckets:
        print("  - no cluster DLQ rows")
    else:
        for label in ("high", "medium", "low", "unlikely"):
            if label in llm_buckets:
                print(f"  - {label}: {llm_buckets[label]}")

    if not show_dlq:
        return

    print("\nDLQ details:")
    if not summary.dlq_items:
        print("  - none")
        return
    for item in summary.dlq_items:
        print(
            f"  - listing={item.listing_id} parcel={item.raw_numer_dzialki or '—'} "
            f"obreb={item.raw_obreb or '—'} kw={item.raw_kw or '—'} "
            f"attempt={item.attempt_count} llm={item.llm_help}"
        )
        print(f"    reason: {item.llm_reason}")
        if item.last_error:
            print(f"    last_error: {item.last_error}")
        print(f"    url: {item.source_url}")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze and optionally replay the Gliwice cluster.")
    parser.add_argument("--show-dlq", action="store_true", help="Print detailed DLQ rows for the cluster.")
    parser.add_argument("--sync-mpzp", action="store_true", help="Sync MPZP only for cluster gminy already covered by WFS_REGISTRY.")
    parser.add_argument("--replay", action="store_true", help="Replay GeoResolver only for cluster DLQ listings, then rerun DeltaEngine for cluster parcels.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    summary = await _fetch_cluster_summary()
    _print_summary(summary, show_dlq=args.show_dlq)

    if args.sync_mpzp:
        print("\nSyncing MPZP for cluster gminy present in WFS_REGISTRY...")
        sync_reports = await _sync_cluster_mpzp(code for code, _ in summary.cluster_gminy)
        for report in sync_reports:
            print(
                f"  - matched={report.matched_gminy} completed={report.completed_gminy} "
                f"failed={report.failed_gminy} fetched={report.total_features_fetched}"
            )

    if args.replay:
        listing_ids = [item.listing_id for item in summary.dlq_items]
        requeued = await _requeue_cluster_dlq(listing_ids)
        print(f"\nRequeued cluster Bronze rows: {requeued}")
        geo_report = await _run_cluster_replay(listing_ids)
        cluster_dzialka_ids = await _cluster_dzialka_ids()
        delta_report = await run_delta_engine(
            batch_size=max(len(cluster_dzialka_ids), 1),
            dzialka_ids=cluster_dzialka_ids,
        )

        print(f"\n{'=' * 60}")
        print("GLIWICE CLUSTER REPLAY COMPLETE")
        print(f"{'=' * 60}")
        print(f"  Geo processed         : {geo_report.total_processed}")
        print(f"  Geo resolved          : {geo_report.resolved}")
        print(f"  Geo sent to DLQ       : {geo_report.sent_to_dlq}")
        print(f"  Delta analyzed        : {delta_report.dzialki_analyzed}")
        print(f"  Delta results created : {delta_report.delta_results_created}")
        print(f"  Leads created         : {delta_report.leads_created}")
        print(f"  Leads updated         : {delta_report.leads_updated}")
        print(f"{'=' * 60}")


if __name__ == "__main__":
    asyncio.run(main())
