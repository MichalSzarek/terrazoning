"""Server-side persistence for investor watchlist criteria."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

from app.schemas.watchlist import InvestorWatchlistPayload, InvestorWatchlistResponse
from app.services.watchlist_store import (
    InvestorWatchlistRecord,
    load_watchlist,
    save_watchlist,
)

router = APIRouter(tags=["watchlist"])


def _to_response(record: InvestorWatchlistRecord) -> InvestorWatchlistResponse:
    return InvestorWatchlistResponse(
        max_price_per_m2=record.max_price_per_m2,
        min_coverage_pct=record.min_coverage_pct,
        min_confidence_pct=record.min_confidence_pct,
        required_designation=record.required_designation,
        only_reliable_price=record.only_reliable_price,
        acknowledged_at=datetime.fromisoformat(record.acknowledged_at) if record.acknowledged_at else None,
        updated_at=datetime.fromisoformat(record.updated_at),
    )


@router.get(
    "/watchlist",
    response_model=InvestorWatchlistResponse,
    summary="Get persisted investor watchlist criteria",
)
async def get_watchlist() -> InvestorWatchlistResponse:
    return _to_response(load_watchlist())


@router.put(
    "/watchlist",
    response_model=InvestorWatchlistResponse,
    summary="Persist investor watchlist criteria on the server",
)
async def update_watchlist(payload: InvestorWatchlistPayload) -> InvestorWatchlistResponse:
    current = load_watchlist()
    current.max_price_per_m2 = payload.max_price_per_m2
    current.min_coverage_pct = payload.min_coverage_pct
    current.min_confidence_pct = payload.min_confidence_pct
    current.required_designation = payload.required_designation
    current.only_reliable_price = payload.only_reliable_price
    current.acknowledged_at = payload.acknowledged_at.isoformat() if payload.acknowledged_at else None
    return _to_response(save_watchlist(current))


@router.post(
    "/watchlist/acknowledge",
    response_model=InvestorWatchlistResponse,
    summary="Mark current watchlist matches as reviewed",
)
async def acknowledge_watchlist() -> InvestorWatchlistResponse:
    current = load_watchlist()
    current.acknowledged_at = datetime.now(timezone.utc).isoformat()
    return _to_response(save_watchlist(current))

