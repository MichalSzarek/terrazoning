"""Async SQLAlchemy engine and session factory.

Usage in FastAPI endpoints:
    from app.core.database import get_db
    from sqlalchemy.ext.asyncio import AsyncSession

    @router.get("/example")
    async def my_endpoint(db: AsyncSession = Depends(get_db)):
        result = await db.execute(select(MyModel))
        ...
"""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import settings


def _create_engine() -> AsyncEngine:
    return create_async_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout,
        pool_pre_ping=True,   # reconnect silently after idle timeout
        echo=settings.debug,  # log SQL only in debug mode
    )


engine: AsyncEngine = _create_engine()

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,  # avoid lazy-load errors after commit in async context
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency: yields a scoped AsyncSession, auto-closed on exit."""
    async with AsyncSessionLocal() as session:
        yield session
