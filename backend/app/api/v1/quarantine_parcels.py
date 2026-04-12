"""API endpoints for human-in-the-loop parcel quarantine workflows."""

from __future__ import annotations

import logging
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.quarantine_parcels import (
    ManualOverrideRequest,
    ManualOverrideResponse,
    QuarantineParcelFeatureCollection,
)
from app.services.quarantine_parcels import apply_manual_override, list_quarantine_parcels

logger = logging.getLogger(__name__)

router = APIRouter(tags=["quarantine"])


@router.get(
    "/quarantine_parcels",
    response_model=QuarantineParcelFeatureCollection,
    summary="GeoJSON of resolved parcels that still require manual review",
)
async def get_quarantine_parcels(
    limit: int = Query(
        default=250,
        ge=1,
        le=1000,
        description="Maximum number of quarantined parcels to return",
    ),
    only_uncovered_gmina: bool = Query(
        default=False,
        description="When true, return only parcels from gminy with zero planning zones",
    ),
    db: AsyncSession = Depends(get_db),
) -> QuarantineParcelFeatureCollection:
    logger.info(
        "[quarantine] GET /quarantine_parcels limit=%d only_uncovered_gmina=%s",
        limit,
        only_uncovered_gmina,
    )
    return await list_quarantine_parcels(
        db,
        limit=limit,
        only_uncovered_gmina=only_uncovered_gmina,
    )


@router.post(
    "/quarantine_parcels/{dzialka_id}/manual_override",
    response_model=ManualOverrideResponse,
    summary="Create a manual delta override and promote the parcel to an investment lead",
)
async def post_manual_override(
    payload: ManualOverrideRequest,
    dzialka_id: UUID = Path(description="Target silver.dzialki UUID"),
    db: AsyncSession = Depends(get_db),
) -> ManualOverrideResponse:
    logger.info(
        "[quarantine] POST /quarantine_parcels/%s/manual_override przeznaczenie=%s",
        dzialka_id,
        payload.manual_przeznaczenie,
    )
    result = await apply_manual_override(
        db,
        dzialka_id=dzialka_id,
        manual_przeznaczenie=payload.manual_przeznaczenie,
    )
    return ManualOverrideResponse(
        dzialka_id=result.dzialka_id,
        lead_id=result.lead_id,
        delta_result_id=result.delta_result_id,
        planning_zone_id=result.planning_zone_id,
        manual_przeznaczenie=result.manual_przeznaczenie,
        source_url=result.source_url,
        lead_created=result.lead_created,
        lead_updated=result.lead_updated,
    )
