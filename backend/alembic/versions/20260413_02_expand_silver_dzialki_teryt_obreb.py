"""Expand silver.dzialki.teryt_obreb to handle 10-digit obręb identifiers."""

from __future__ import annotations

from alembic import op


revision = "20260413_02"
down_revision = "20260413_01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE silver.dzialki
        ALTER COLUMN teryt_obreb TYPE TEXT
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE silver.dzialki
        ALTER COLUMN teryt_obreb TYPE CHAR(9)
        USING substring(teryt_obreb from 1 for 9)
        """
    )
