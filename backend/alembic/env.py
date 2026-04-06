"""Alembic migration environment — async SQLAlchemy configuration.

This file configures Alembic to:
  1. Read the DATABASE_URL from pydantic-settings (never from alembic.ini)
  2. Use asyncpg driver for online migrations
  3. Use psycopg2 driver for offline (--sql) mode
  4. Target ALL three schemas: bronze, silver, gold

Run migrations:
    cd backend/
    uv run alembic upgrade head        # apply all pending
    uv run alembic downgrade -1        # roll back one step
    uv run alembic revision --autogenerate -m "describe_change"
"""

import asyncio
import logging
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

# Import all models so Base.metadata is populated.
# This is the single import that makes autogenerate work.
import app.models  # noqa: F401 — registers all ORM models
from app.core.config import settings
from app.models.base import Base

# ---------------------------------------------------------------------------
# Alembic Config object
# ---------------------------------------------------------------------------
config = context.config

# Configure logging from alembic.ini [loggers] section
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

logger = logging.getLogger("alembic.env")

# ---------------------------------------------------------------------------
# Inject DATABASE_URL from pydantic-settings — NEVER hardcode here
# ---------------------------------------------------------------------------
config.set_main_option("sqlalchemy.url", settings.database_url)

# Tell Alembic which metadata to inspect for autogenerate
target_metadata = Base.metadata

# Include all three custom schemas in autogenerate scope
MANAGED_SCHEMAS = ("bronze", "silver", "gold")


def include_object(object, name, type_, reflected, compare_to):  # noqa: A002
    """Filter: only autogenerate for our managed schemas."""
    if type_ == "table":
        return getattr(object, "schema", None) in MANAGED_SCHEMAS
    return True


# ---------------------------------------------------------------------------
# Offline mode — generates raw SQL without connecting to DB
# ---------------------------------------------------------------------------
def run_migrations_offline() -> None:
    url = settings.database_url_sync  # psycopg2 for offline SQL generation
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        include_object=include_object,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


# ---------------------------------------------------------------------------
# Online mode — runs migrations against live DB via asyncpg
# ---------------------------------------------------------------------------
def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        include_object=include_object,
        compare_type=True,
        # Ensure PostGIS extensions and schemas exist before migration runs
        # (handled by init SQL; Alembic should not attempt to create them)
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,  # no pooling in migration context
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if context.is_offline_mode():
    logger.info("Running migrations in OFFLINE mode")
    run_migrations_offline()
else:
    logger.info("Running migrations in ONLINE mode (async)")
    run_migrations_online()
