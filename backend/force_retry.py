"""Force one full retry cycle for GeoResolver and DeltaEngine.

This script is operational glue for situations where queue state got stuck after
extractor / resolver / delta logic changes.

It performs five reset actions:
  1. Moves every DLQ row to `next_retry_at = now()`
  2. Marks the corresponding Bronze listings as `is_processed = false`
  3. Clears stale Silver listing → parcel links for the retried listings
  4. Deletes orphaned Silver parcels left behind by those stale links
  5. Deletes all Gold delta_results and investment_leads for a clean replay

Then it runs:
  - run_geo_resolver()
  - run_delta_engine()
"""

from __future__ import annotations

import asyncio
import argparse
import logging
from dataclasses import dataclass, field
from uuid import UUID

from sqlalchemy import delete, distinct, exists, func, select, update

from app.core.database import AsyncSessionLocal
from app.models.bronze import RawListing
from app.models.gold import DeltaResult, InvestmentLead
from app.models.silver import DlqParcel, Dzialka, ListingParcel
from app.services.manual_backlog_store import ManualBacklogRecord, upsert_manual_backlog
from app.services.operations_scope import (
    classify_dlq_error,
    normalize_province,
    province_display_name,
    sql_listing_province_filter,
    sql_teryt_prefix_filter,
)
from app.services.delta_engine import DeltaReport, run_delta_engine
from app.services.geo_resolver import ResolutionReport, run_geo_resolver

logger = logging.getLogger("force_retry")


@dataclass
class ResetReport:
    stale_dlq_removed: int = 0
    manual_backlog_archived: int = 0
    dlq_rows_reset: int = 0
    bronze_rows_requeued: int = 0
    listing_links_deleted: int = 0
    orphaned_dzialki_deleted: int = 0
    leads_deleted: int = 0
    delta_results_deleted: int = 0
    dzialki_available: int = 0
    target_listing_ids: list[UUID] = field(default_factory=list)
    affected_dzialka_ids: list[UUID] = field(default_factory=list)
    province: str | None = None


async def sweep_stale_dlq_rows(
    *,
    listing_ids: list[UUID] | None = None,
    province: str | None = None,
) -> int:
    """Remove DLQ rows for listings that already have silver links."""
    province = normalize_province(province)
    async with AsyncSessionLocal() as db:
        stmt = (
            delete(DlqParcel)
            .where(
                exists(
                    select(1)
                    .select_from(ListingParcel)
                    .where(ListingParcel.listing_id == DlqParcel.listing_id)
                )
            )
        )
        if listing_ids:
            stmt = stmt.where(DlqParcel.listing_id.in_(listing_ids))
        elif province:
            stmt = stmt.where(
                exists(
                    select(1)
                    .select_from(RawListing)
                    .where(RawListing.id == DlqParcel.listing_id)
                    .where(sql_listing_province_filter(RawListing.raw_wojewodztwo, province))
                )
            )

        result = await db.execute(stmt)
        await db.commit()
        return result.rowcount or 0


async def archive_manual_only_dlq_rows(
    *,
    listing_ids: list[UUID] | None = None,
    province: str | None = None,
) -> int:
    """Move exhausted/manual-only DLQ rows out of the active retry queue."""
    province = normalize_province(province)
    async with AsyncSessionLocal() as db:
        query = (
            select(
                DlqParcel.id,
                DlqParcel.listing_id,
                DlqParcel.raw_teryt_input,
                DlqParcel.attempt_count,
                DlqParcel.last_error,
                RawListing.title,
                RawListing.source_url,
                RawListing.raw_wojewodztwo,
                RawListing.raw_obreb,
                RawListing.raw_numer_dzialki,
            )
            .join(RawListing, RawListing.id == DlqParcel.listing_id)
        )
        if listing_ids:
            query = query.where(DlqParcel.listing_id.in_(listing_ids))
        elif province:
            query = query.where(sql_listing_province_filter(RawListing.raw_wojewodztwo, province))

        rows = (await db.execute(query)).all()
        archive_records: list[ManualBacklogRecord] = []
        dlq_ids_to_delete: list[UUID] = []
        for (
            dlq_id,
            listing_id,
            raw_teryt_input,
            attempt_count,
            last_error,
            title,
            source_url,
            raw_wojewodztwo,
            raw_obreb,
            raw_numer_dzialki,
        ) in rows:
            category, next_action = classify_dlq_error(
                last_error=last_error,
                attempt_count=int(attempt_count or 1),
                raw_obreb=raw_obreb,
                raw_numer_dzialki=raw_numer_dzialki,
            )
            should_archive = category == "manual_only_case" or (
                category == "parser_issue"
                and any(ch.isalpha() for ch in (raw_numer_dzialki or ""))
            )
            if not should_archive:
                continue
            dlq_ids_to_delete.append(dlq_id)
            archive_records.append(
                ManualBacklogRecord(
                    listing_id=str(listing_id),
                    province=normalize_province(raw_wojewodztwo),
                    title=title,
                    source_url=source_url,
                    raw_teryt_input=raw_teryt_input,
                    last_error=last_error,
                    category=category,
                    next_action=next_action,
                    attempt_count=int(attempt_count or 1),
                )
            )

        if not dlq_ids_to_delete:
            return 0

        upsert_manual_backlog(archive_records)
        await db.execute(delete(DlqParcel).where(DlqParcel.id.in_(dlq_ids_to_delete)))
        await db.commit()
        return len(dlq_ids_to_delete)


async def _resolve_target_listing_ids(
    *,
    db,
    listing_ids: list[UUID] | None = None,
    province: str | None = None,
) -> list[UUID]:
    if listing_ids:
        return list(dict.fromkeys(listing_ids))

    if province:
        rows = await db.execute(
            select(distinct(RawListing.id))
            .outerjoin(DlqParcel, DlqParcel.listing_id == RawListing.id)
            .where(sql_listing_province_filter(RawListing.raw_wojewodztwo, province))
            .where(
                (RawListing.is_processed == False)  # noqa: E712
                | DlqParcel.id.is_not(None)
            )
        )
        return [row[0] for row in rows.fetchall()]

    rows = await db.execute(select(distinct(DlqParcel.listing_id)))
    return [row[0] for row in rows.fetchall()]


async def clear_gold_for_dzialki(dzialka_ids: list[UUID]) -> tuple[int, int]:
    """Delete scoped Gold rows for the provided działki IDs."""
    if not dzialka_ids:
        return 0, 0

    async with AsyncSessionLocal() as db:
        leads_delete = await db.execute(
            delete(InvestmentLead).where(InvestmentLead.dzialka_id.in_(dzialka_ids))
        )
        delta_delete = await db.execute(
            delete(DeltaResult).where(DeltaResult.dzialka_id.in_(dzialka_ids))
        )
        await db.commit()
        return leads_delete.rowcount or 0, delta_delete.rowcount or 0


async def reset_queues(
    *,
    listing_ids: list[UUID] | None = None,
    province: str | None = None,
    destructive_gold_reset: bool = True,
) -> ResetReport:
    """Reset retry/analyzed state so the pipelines can run from scratch."""
    province = normalize_province(province)
    stale_dlq_removed = await sweep_stale_dlq_rows(
        listing_ids=listing_ids,
        province=province,
    )
    manual_backlog_archived = await archive_manual_only_dlq_rows(
        listing_ids=listing_ids,
        province=province,
    )
    async with AsyncSessionLocal() as db:
        report = ResetReport(
            province=province,
            stale_dlq_removed=stale_dlq_removed,
            manual_backlog_archived=manual_backlog_archived,
        )
        target_listing_ids = await _resolve_target_listing_ids(
            db=db,
            listing_ids=listing_ids,
            province=province,
        )
        report.target_listing_ids = target_listing_ids

        affected_dzialka_ids = []
        if target_listing_ids:
            dzialka_ids_result = await db.execute(
                select(distinct(ListingParcel.dzialka_id)).where(
                    ListingParcel.listing_id.in_(target_listing_ids)
                )
            )
            affected_dzialka_ids = [row[0] for row in dzialka_ids_result.fetchall()]
        report.affected_dzialka_ids = affected_dzialka_ids

        dlq_stmt = update(DlqParcel).values(next_retry_at=func.now())
        if target_listing_ids:
            dlq_stmt = dlq_stmt.where(DlqParcel.listing_id.in_(target_listing_ids))
        elif province:
            dlq_stmt = dlq_stmt.where(
                exists(
                    select(1)
                    .select_from(RawListing)
                    .where(RawListing.id == DlqParcel.listing_id)
                    .where(sql_listing_province_filter(RawListing.raw_wojewodztwo, province))
                )
            )
        dlq_update = await db.execute(dlq_stmt)
        report.dlq_rows_reset = dlq_update.rowcount or 0

        if target_listing_ids:
            bronze_update = await db.execute(
                update(RawListing)
                .where(RawListing.id.in_(target_listing_ids))
                .values(is_processed=False)
            )
            report.bronze_rows_requeued = bronze_update.rowcount or 0

            listing_link_delete = await db.execute(
                delete(ListingParcel).where(ListingParcel.listing_id.in_(target_listing_ids))
            )
            report.listing_links_deleted = listing_link_delete.rowcount or 0

        # Live DB constraints currently do not cascade dzialka deletes, so clear
        # dependent Gold rows explicitly before removing orphaned Silver parcels.
        if destructive_gold_reset and not target_listing_ids and not province:
            leads_delete = await db.execute(delete(InvestmentLead))
            report.leads_deleted = leads_delete.rowcount or 0

            delta_delete = await db.execute(delete(DeltaResult))
            report.delta_results_deleted = delta_delete.rowcount or 0
        elif affected_dzialka_ids:
            leads_delete = await db.execute(
                delete(InvestmentLead).where(InvestmentLead.dzialka_id.in_(affected_dzialka_ids))
            )
            report.leads_deleted = leads_delete.rowcount or 0

            delta_delete = await db.execute(
                delete(DeltaResult).where(DeltaResult.dzialka_id.in_(affected_dzialka_ids))
            )
            report.delta_results_deleted = delta_delete.rowcount or 0

        if affected_dzialka_ids:
            orphaned_delete = await db.execute(
                delete(Dzialka)
                .where(Dzialka.id.in_(affected_dzialka_ids))
                .where(
                    ~exists(
                        select(1)
                        .select_from(ListingParcel)
                        .where(ListingParcel.dzialka_id == Dzialka.id)
                    )
                )
            )
            report.orphaned_dzialki_deleted = orphaned_delete.rowcount or 0

        dzialki_result = await db.execute(
            select(func.count()).select_from(Dzialka)
        )
        report.dzialki_available = int(dzialki_result.scalar_one())

        await db.commit()
        return report


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reset DLQ/replay queues for TerraZoning")
    parser.add_argument("--province", choices=["slaskie", "malopolskie"], default=None)
    parser.add_argument(
        "--listing-id",
        action="append",
        dest="listing_ids",
        default=None,
        help="Scope the reset to one or more listing UUIDs",
    )
    parser.add_argument(
        "--scoped",
        action="store_true",
        help="Do not wipe all Gold rows; delete only rows tied to the affected dzialki",
    )
    return parser.parse_args()


async def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    listing_ids = [UUID(value) for value in (args.listing_ids or [])]
    reset_report = await reset_queues(
        listing_ids=listing_ids or None,
        province=args.province,
        destructive_gold_reset=not args.scoped and not args.province and not listing_ids,
    )
    logger.info(
        "[force_retry] Reset complete: dlq=%d bronze_requeued=%d links_deleted=%d "
        "orphaned_dzialki=%d leads_deleted=%d delta_deleted=%d dzialki=%d stale_dlq=%d "
        "manual_backlog=%d",
        reset_report.dlq_rows_reset,
        reset_report.bronze_rows_requeued,
        reset_report.listing_links_deleted,
        reset_report.orphaned_dzialki_deleted,
        reset_report.leads_deleted,
        reset_report.delta_results_deleted,
        reset_report.dzialki_available,
        reset_report.stale_dlq_removed,
        reset_report.manual_backlog_archived,
    )
    print("Kolejki zresetowane. Uruchamiam procesy...")

    geo_batch_size = max(100, len(reset_report.target_listing_ids), reset_report.bronze_rows_requeued)
    geo_report: ResolutionReport = await run_geo_resolver(
        batch_size=geo_batch_size,
        listing_ids=reset_report.target_listing_ids or None,
    )

    if args.province:
        async with AsyncSessionLocal() as db:
            province_dzialki = (
                await db.execute(
                    select(Dzialka.id).where(sql_teryt_prefix_filter(Dzialka.teryt_gmina, args.province))
                )
            ).scalars().all()
        delta_batch_ids = list(province_dzialki)
        if delta_batch_ids:
            deleted_leads, deleted_deltas = await clear_gold_for_dzialki(delta_batch_ids)
            reset_report.leads_deleted += deleted_leads
            reset_report.delta_results_deleted += deleted_deltas
        delta_report: DeltaReport = await run_delta_engine(
            batch_size=max(100, len(delta_batch_ids)),
            dzialka_ids=delta_batch_ids or None,
        )
    elif reset_report.affected_dzialka_ids and (args.scoped or listing_ids):
        delta_report = await run_delta_engine(
            batch_size=max(100, len(reset_report.affected_dzialka_ids)),
            dzialka_ids=reset_report.affected_dzialka_ids,
        )
    else:
        delta_report = await run_delta_engine(batch_size=max(100, reset_report.dzialki_available))

    print(f"\n{'=' * 60}")
    print("FORCE RETRY COMPLETE")
    print(f"{'=' * 60}")
    print(f"  DLQ rows reset        : {reset_report.dlq_rows_reset}")
    print(f"  Bronze requeued       : {reset_report.bronze_rows_requeued}")
    print(f"  Listing links deleted : {reset_report.listing_links_deleted}")
    print(f"  Orphaned dzialki del. : {reset_report.orphaned_dzialki_deleted}")
    print(f"  Stale DLQ removed     : {reset_report.stale_dlq_removed}")
    print(f"  Manual backlog arch.  : {reset_report.manual_backlog_archived}")
    print(f"  Leads deleted         : {reset_report.leads_deleted}")
    print(f"  Delta rows deleted    : {reset_report.delta_results_deleted}")
    print(f"  Geo processed         : {geo_report.total_processed}")
    print(f"  Geo resolved          : {geo_report.resolved}")
    print(f"  Geo sent to DLQ       : {geo_report.sent_to_dlq}")
    print(f"  Delta analyzed        : {delta_report.dzialki_analyzed}")
    print(f"  Delta results created : {delta_report.delta_results_created}")
    print(f"  Leads created         : {delta_report.leads_created}")
    print(f"  Leads updated         : {delta_report.leads_updated}")
    if args.province:
        print(f"  Province scope        : {province_display_name(args.province)}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    asyncio.run(main())
