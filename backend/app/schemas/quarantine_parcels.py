"""Schemas for parcel quarantine and human-in-the-loop overrides."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field


class QuarantineParcelProperties(BaseModel):
    """GeoJSON properties for a parcel that still needs analyst review."""

    dzialka_id: UUID
    identyfikator: str
    teryt_gmina: str | None = None
    area_m2: float | None = None
    source_url: str | None = None
    reason: str | None = None
    status: str | None = None
    current_use: str | None = None
    dominant_przeznaczenie: str | None = None
    manual_przeznaczenie: str | None = None
    created_at: datetime | None = None


class QuarantineParcelFeature(BaseModel):
    """GeoJSON Feature wrapper."""

    type: Literal["Feature"] = "Feature"
    geometry: dict[str, Any]
    properties: QuarantineParcelProperties


class QuarantineParcelFeatureCollection(BaseModel):
    """GeoJSON FeatureCollection response."""

    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: list[QuarantineParcelFeature] = Field(default_factory=list)
    count: int = 0


class ManualOverrideRequest(BaseModel):
    """Manual planning designation entered by the analyst."""

    manual_przeznaczenie: str = Field(
        min_length=1,
        description="Raw planning symbol copied from the PDF / operat, e.g. MN, U, MN/U",
    )


class ManualOverrideResponse(BaseModel):
    """Result of a manual override that produced or updated a lead."""

    dzialka_id: UUID
    lead_id: UUID
    delta_result_id: UUID
    planning_zone_id: UUID
    manual_przeznaczenie: str
    source_url: str | None = None
    lead_created: bool
    lead_updated: bool
