"""Health endpoint — liveness + readiness check.

GET /api/v1/health  →  checks DB connectivity (SELECT 1 via async session).
This endpoint is intentionally unauthenticated — it must be reachable by
load balancers and Cloud Run health probes without a JWT.

Latency budget: p50 < 10ms, p99 < 50ms (this must NEVER be the slow endpoint).
"""

from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_db
from app.schemas.health import HealthErrorResponse, HealthResponse

router = APIRouter(tags=["health"])


@router.get(
    "/health",
    response_model=HealthResponse,
    responses={503: {"model": HealthErrorResponse}},
    summary="Liveness + DB readiness check",
)
async def health_check(db: AsyncSession = Depends(get_db)) -> JSONResponse:
    """Returns 200 if the API is alive and can reach PostGIS.
    Returns 503 if the database is unreachable.
    """
    try:
        await db.execute(text("SELECT 1"))
        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "database": "connected",
                "version": settings.app_version,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
    except Exception as exc:
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "database": "unreachable",
                "detail": str(exc),
            },
        )
