"""future buildability foundation

Revision ID: 20260409_01
Revises:
Create Date: 2026-04-09
"""

from __future__ import annotations

from alembic import op


# revision identifiers, used by Alembic.
revision = "20260409_01"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.planning_signals (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            dzialka_id UUID REFERENCES silver.dzialki(id) ON DELETE CASCADE,
            teryt_gmina TEXT NOT NULL,
            signal_kind TEXT NOT NULL
                CHECK (signal_kind IN ('pog_zone', 'pog_ouz', 'studium_zone', 'mpzp_project', 'planning_resolution', 'coverage_only')),
            signal_status TEXT NOT NULL
                CHECK (signal_status IN ('formal_binding', 'formal_directional', 'formal_preparatory', 'heuristic')),
            designation_raw TEXT,
            designation_normalized TEXT,
            description TEXT,
            plan_name TEXT,
            uchwala_nr TEXT,
            effective_date DATE,
            source_url TEXT,
            source_type TEXT NOT NULL DEFAULT 'manual_registry'
                CHECK (source_type IN ('wfs', 'wms_grid', 'gison_popup', 'pdf', 'html_index', 'planning_zone_passthrough', 'manual_registry')),
            source_confidence NUMERIC(3,2) NOT NULL DEFAULT 1.00
                CHECK (source_confidence >= 0.00 AND source_confidence <= 1.00),
            legal_weight NUMERIC(6,2) NOT NULL DEFAULT 0.00,
            geom geometry(MultiPolygon, 2180),
            evidence_chain JSONB NOT NULL DEFAULT '[]'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_planning_signals_geom ON gold.planning_signals USING GIST (geom)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_planning_signals_gmina ON gold.planning_signals (teryt_gmina)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_planning_signals_kind ON gold.planning_signals (signal_kind)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_planning_signals_status ON gold.planning_signals (signal_status)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_planning_signals_dzialka ON gold.planning_signals (dzialka_id) WHERE dzialka_id IS NOT NULL")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.future_buildability_assessments (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            dzialka_id UUID NOT NULL REFERENCES silver.dzialki(id) ON DELETE CASCADE,
            listing_id UUID REFERENCES bronze.raw_listings(id) ON DELETE SET NULL,
            strategy_type TEXT NOT NULL DEFAULT 'future_buildable'
                CHECK (strategy_type = 'future_buildable'),
            current_use TEXT,
            current_buildable_status TEXT NOT NULL
                CHECK (current_buildable_status IN ('non_buildable', 'mixed', 'already_buildable')),
            future_signal_score NUMERIC(6,2) NOT NULL
                CHECK (future_signal_score >= 0.00 AND future_signal_score <= 100.00),
            cheapness_score NUMERIC(6,2) NOT NULL DEFAULT 0.00
                CHECK (cheapness_score >= 0.00 AND cheapness_score <= 100.00),
            overall_score NUMERIC(6,2) NOT NULL
                CHECK (overall_score >= 0.00 AND overall_score <= 100.00),
            confidence_band TEXT
                CHECK (confidence_band IN ('formal', 'supported', 'speculative') OR confidence_band IS NULL),
            dominant_future_signal TEXT,
            future_signal_count INTEGER NOT NULL DEFAULT 0,
            distance_to_nearest_buildable_m NUMERIC(12,2),
            adjacent_buildable_pct NUMERIC(5,2),
            price_per_m2_zl NUMERIC(10,2),
            status TEXT NOT NULL DEFAULT 'assessed',
            evidence_chain JSONB NOT NULL DEFAULT '[]'::jsonb,
            signal_breakdown JSONB NOT NULL DEFAULT '[]'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_future_assessments_dzialka UNIQUE (dzialka_id)
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_future_assessments_listing ON gold.future_buildability_assessments (listing_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_future_assessments_score ON gold.future_buildability_assessments (overall_score DESC)")

    op.execute(
        """
        ALTER TABLE gold.investment_leads
            ADD COLUMN IF NOT EXISTS strategy_type TEXT NOT NULL DEFAULT 'current_buildable',
            ADD COLUMN IF NOT EXISTS confidence_band TEXT,
            ADD COLUMN IF NOT EXISTS future_signal_score NUMERIC(6,2),
            ADD COLUMN IF NOT EXISTS cheapness_score NUMERIC(6,2),
            ADD COLUMN IF NOT EXISTS overall_score NUMERIC(6,2),
            ADD COLUMN IF NOT EXISTS dominant_future_signal TEXT,
            ADD COLUMN IF NOT EXISTS future_signal_count INTEGER,
            ADD COLUMN IF NOT EXISTS distance_to_nearest_buildable_m NUMERIC(12,2),
            ADD COLUMN IF NOT EXISTS adjacent_buildable_pct NUMERIC(5,2),
            ADD COLUMN IF NOT EXISTS signal_breakdown JSONB NOT NULL DEFAULT '[]'::jsonb
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'ck_leads_strategy_type'
                  AND connamespace = 'gold'::regnamespace
            ) THEN
                ALTER TABLE gold.investment_leads
                    ADD CONSTRAINT ck_leads_strategy_type
                    CHECK (strategy_type IN ('current_buildable', 'future_buildable'));
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'ck_leads_confidence_band'
                  AND connamespace = 'gold'::regnamespace
            ) THEN
                ALTER TABLE gold.investment_leads
                    ADD CONSTRAINT ck_leads_confidence_band
                    CHECK (confidence_band IN ('formal', 'supported', 'speculative') OR confidence_band IS NULL);
            END IF;
        END $$;
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_leads_strategy ON gold.investment_leads (strategy_type)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS gold.idx_leads_strategy;")
    op.execute(
        """
        ALTER TABLE gold.investment_leads
            DROP CONSTRAINT IF EXISTS ck_leads_confidence_band,
            DROP CONSTRAINT IF EXISTS ck_leads_strategy_type,
            DROP COLUMN IF EXISTS signal_breakdown,
            DROP COLUMN IF EXISTS adjacent_buildable_pct,
            DROP COLUMN IF EXISTS distance_to_nearest_buildable_m,
            DROP COLUMN IF EXISTS future_signal_count,
            DROP COLUMN IF EXISTS dominant_future_signal,
            DROP COLUMN IF EXISTS overall_score,
            DROP COLUMN IF EXISTS cheapness_score,
            DROP COLUMN IF EXISTS future_signal_score,
            DROP COLUMN IF EXISTS confidence_band,
            DROP COLUMN IF EXISTS strategy_type;
        """
    )
    op.execute("DROP TABLE IF EXISTS gold.future_buildability_assessments;")
    op.execute("DROP TABLE IF EXISTS gold.planning_signals;")
