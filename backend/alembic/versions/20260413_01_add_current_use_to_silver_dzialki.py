"""add current_use to silver.dzialki

Revision ID: 20260413_01
Revises: 20260409_01
Create Date: 2026-04-13
"""

from __future__ import annotations

from alembic import op


# revision identifiers, used by Alembic.
revision = "20260413_01"
down_revision = "20260409_01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE silver.dzialki
            ADD COLUMN IF NOT EXISTS current_use TEXT;
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN silver.dzialki.current_use IS
            'EGiB land-use code used by DeltaEngine and FutureBuildabilityEngine. '
            'Examples: R, Ł, Ps, Ls, B, Bp. NULL means not yet available.';
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE silver.dzialki
            DROP COLUMN IF EXISTS current_use;
        """
    )
