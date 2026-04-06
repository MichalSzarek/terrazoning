"""Declarative base shared by all SQLAlchemy ORM models."""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Single source of truth for SQLAlchemy metadata.

    All Bronze / Silver / Gold models inherit from this Base.
    Alembic's env.py imports Base.metadata to detect schema changes.
    """
    pass
