"""Schemas for investor watchlist persistence."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class InvestorWatchlistPayload(BaseModel):
    max_price_per_m2: float | None = Field(default=None, ge=0)
    min_coverage_pct: float = Field(default=60, ge=0, le=100)
    min_confidence_pct: float = Field(default=80, ge=0, le=100)
    required_designation: str = Field(default="")
    only_reliable_price: bool = Field(default=True)
    acknowledged_at: datetime | None = Field(default=None)


class InvestorWatchlistResponse(InvestorWatchlistPayload):
    updated_at: datetime

