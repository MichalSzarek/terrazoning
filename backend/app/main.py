"""TerraZoning FastAPI application factory.

Start locally:
    uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

OpenAPI docs available at:
    http://localhost:8000/docs   (Swagger UI)
    http://localhost:8000/redoc  (ReDoc)
"""

from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.health import router as health_router
from app.api.v1.leads import router as leads_router
from app.api.v1.quarantine_parcels import router as quarantine_parcels_router
from app.api.v1.watchlist import router as watchlist_router
from app.core.config import settings
from app.core.database import engine


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup / shutdown lifecycle."""
    # Startup: verify DB connectivity on boot
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import sessionmaker

    AsyncSession_ = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with AsyncSession_() as session:
            await session.execute(text("SELECT 1"))
    except Exception as exc:
        # Log but don't crash — Cloud Run may start before DB is ready
        import logging
        logging.getLogger(__name__).warning(
            "DB connectivity check failed at startup: %s", exc
        )

    yield  # application is running

    # Shutdown: dispose connection pool gracefully
    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description=(
            "TerraZoning — Polish Land Arbitrage Platform. "
            "Correlates public auction data with spatial planning documents "
            "to surface undervalued land opportunities."
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # CORS — tighten allowed origins before production
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000"],  # Frontend dev server
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Routers
    app.include_router(health_router, prefix="/api/v1")
    app.include_router(leads_router, prefix="/api/v1")
    app.include_router(quarantine_parcels_router, prefix="/api/v1")
    app.include_router(watchlist_router, prefix="/api/v1")

    return app


app = create_app()
