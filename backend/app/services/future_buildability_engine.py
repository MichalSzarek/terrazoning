"""Rule-based future-buildability assessment engine."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.bronze import RawListing
from app.models.gold import (
    FUTURE_CONFIDENCE_BANDS,
    FutureBuildabilityAssessment,
    InvestmentLead,
    PlanningSignal,
)
from app.models.silver import Dzialka, ListingParcel
from app.services.delta_engine import (
    _AGRICULTURAL_EGIB_CODES,
    _BUILT_EGIB_CODES,
)
from app.services.operations_scope import coverage_alias_teryt
from app.services.planning_signal_utils import (
    HARD_NEGATIVE_DESIGNATIONS,
    POSITIVE_DESIGNATIONS,
    score_signal,
    signal_evidence_label,
)

logger = logging.getLogger(__name__)

_CURRENT_BUILDABLE_SIGNAL_QUERY = text(
    """
    SELECT
        EXISTS(
            SELECT 1
            FROM gold.investment_leads il
            WHERE il.dzialka_id = :dzialka_id
              AND COALESCE(il.strategy_type, 'current_buildable') = 'current_buildable'
        ) AS has_current_lead,
        EXISTS(
            SELECT 1
            FROM gold.delta_results dr
            WHERE dr.dzialka_id = :dzialka_id
              AND dr.is_upgrade = TRUE
        ) AS has_upgrade_delta
    """
)

_SIGNAL_QUERY = text(
    """
    SELECT
        ps.id,
        ps.signal_kind,
        ps.signal_status,
        ps.designation_raw,
        ps.designation_normalized,
        ps.description,
        ps.plan_name,
        ps.uchwala_nr,
        ps.source_url,
        ps.source_type,
        ps.source_confidence,
        ps.legal_weight,
        ps.evidence_chain,
        CASE
            WHEN ps.geom IS NOT NULL THEN ST_Area(ST_Intersection(d.geom, ps.geom))
            ELSE NULL
        END AS intersection_area_m2
    FROM gold.planning_signals ps
    JOIN silver.dzialki d ON d.id = :dzialka_id
    WHERE ps.teryt_gmina = :coverage_teryt
      AND (
          ps.dzialka_id = d.id
          OR (ps.geom IS NOT NULL AND ST_Intersects(ps.geom, d.geom))
          OR (ps.geom IS NULL AND ps.dzialka_id IS NULL)
      )
    ORDER BY ps.legal_weight DESC, ps.created_at DESC
    """
)

_HEURISTICS_QUERY = text(
    """
    WITH target AS (
        SELECT geom
        FROM silver.dzialki
        WHERE id = :dzialka_id
    ),
    buildable AS (
        SELECT pz.geom
        FROM gold.planning_zones pz, target t
        WHERE (
            pz.przeznaczenie IN ('MN', 'MW', 'ML', 'MR', 'U', 'UC', 'UK', 'MU', 'UM', 'MN/U', 'U/MN', 'MNU', 'U/MW', 'MW/U')
            OR pz.przeznaczenie LIKE '%.MN'
            OR pz.przeznaczenie LIKE 'MN.%'
            OR pz.przeznaczenie LIKE '%/MN%'
            OR pz.przeznaczenie LIKE '%MNU%'
        )
    ),
    mixed_service AS (
        SELECT pz.geom
        FROM gold.planning_zones pz, target t
        WHERE (
            pz.przeznaczenie IN ('MU', 'UM', 'MNU', 'U/MN', 'MN/U', 'U/MW', 'MW/U')
            OR pz.przeznaczenie LIKE '%/MN%'
            OR pz.przeznaczenie LIKE '%/U%'
            OR pz.przeznaczenie LIKE '%MN%'
            OR pz.przeznaczenie LIKE '%MW%'
        )
    ),
    road_hierarchy AS (
        SELECT pz.geom
        FROM gold.planning_zones pz, target t
        WHERE (
            pz.przeznaczenie LIKE 'KD%'
            OR pz.przeznaczenie IN ('KDG', 'KDP', 'KDL', 'KDD', 'KDR', 'KDA', 'KDS', 'KDW')
        )
    )
    SELECT
        (
            SELECT MIN(ST_Distance(t.geom, b.geom))
            FROM target t, buildable b
        ) AS distance_to_nearest_buildable_m,
        (
            SELECT MAX(
                CASE
                    WHEN NULLIF(ST_Perimeter(t.geom), 0) IS NULL THEN 0
                    ELSE ST_Length(
                        ST_Intersection(ST_Boundary(t.geom), ST_Boundary(b.geom))
                    ) / NULLIF(ST_Perimeter(t.geom), 0) * 100.0
                END
            )
            FROM target t, buildable b
            WHERE ST_DWithin(t.geom, b.geom, 5.0)
        ) AS adjacent_buildable_pct,
        (
            SELECT COALESCE(SUM(
                CASE
                    WHEN NULLIF(ST_Perimeter(t.geom), 0) IS NULL THEN 0
                    ELSE ST_Length(ST_Intersection(ST_Boundary(t.geom), ST_Boundary(b.geom)))
                END
            ), 0.00)
            FROM target t, buildable b
            WHERE ST_DWithin(t.geom, b.geom, 5.0)
        ) AS shared_boundary_m,
        (
            SELECT MIN(ST_Distance(t.geom, ms.geom))
            FROM target t, mixed_service ms
        ) AS distance_to_mixed_service_zone_m,
        (
            SELECT MIN(ST_Distance(t.geom, rh.geom))
            FROM target t, road_hierarchy rh
        ) AS distance_to_meaningful_road_m,
        EXISTS(
            SELECT 1
            FROM target t
            JOIN gold.planning_zones pz ON ST_DWithin(t.geom, pz.geom, 30.0)
            WHERE pz.przeznaczenie LIKE 'KD%'
        ) AS has_road_access_signal,
        EXISTS(
            SELECT 1
            FROM target t, buildable b
            WHERE ST_DWithin(t.geom, b.geom, 100.0)
        ) AS has_urban_cluster_signal
    """
)

_BENCHMARK_QUERY = text(
    """
    SELECT
        COUNT(*) AS sample_size,
        percentile_cont(0.25) WITHIN GROUP (ORDER BY rl.price_zl / NULLIF(d.area_m2, 0)) AS p25_price_per_m2_zl,
        percentile_cont(0.40) WITHIN GROUP (ORDER BY rl.price_zl / NULLIF(d.area_m2, 0)) AS p40_price_per_m2_zl,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY rl.price_zl / NULLIF(d.area_m2, 0)) AS median_price_per_m2_zl
    FROM silver.listing_parcels lp
    JOIN bronze.raw_listings rl ON rl.id = lp.listing_id
    JOIN silver.dzialki d ON d.id = lp.dzialka_id
    WHERE rl.price_zl IS NOT NULL
      AND d.area_m2 > 0
      AND substr(d.teryt_gmina, 1, :prefix_len) = :prefix_value
    """
)

_PENDING_DZIALKI_QUERY = text(
    """
    SELECT d.id
    FROM silver.dzialki d
    JOIN silver.listing_parcels lp ON lp.dzialka_id = d.id
    WHERE d.resolution_status = 'resolved'
      AND d.area_m2 > 0
      AND NOT EXISTS (
          SELECT 1
          FROM gold.future_buildability_assessments fa
          WHERE fa.dzialka_id = d.id
      )
    GROUP BY d.id, d.created_at
    ORDER BY d.created_at ASC
    LIMIT :batch_size
    """
)


@dataclass
class FutureBuildabilityReport:
    dzialki_analyzed: int = 0
    assessments_created: int = 0
    assessments_updated: int = 0
    leads_created: int = 0
    leads_updated: int = 0
    duration_s: float = 0.0
    errors: list[str] = field(default_factory=list)


@dataclass
class MarketBenchmark:
    scope: str
    sample_size: int
    p25_price_per_m2_zl: Decimal | None
    p40_price_per_m2_zl: Decimal | None
    median_price_per_m2_zl: Decimal | None


class FutureBuildabilityEngine:
    """Assess parcels for likely future buildability without touching DeltaEngine."""

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def calculate_assessments(
        self,
        *,
        dzialka_ids: Optional[list[UUID]] = None,
        batch_size: int = 100,
    ) -> FutureBuildabilityReport:
        started = asyncio.get_event_loop().time()
        report = FutureBuildabilityReport()

        target_ids = dzialka_ids or await self._fetch_pending_dzialki(batch_size)
        if not target_ids:
            return report

        for dzialka_id in target_ids:
            try:
                created_assessment, created_lead = await self._assess_one(dzialka_id)
                if created_assessment is True:
                    report.assessments_created += 1
                elif created_assessment is False:
                    report.assessments_updated += 1
                if created_lead is True:
                    report.leads_created += 1
                elif created_lead is False:
                    report.leads_updated += 1
                report.dzialki_analyzed += 1
            except Exception as exc:
                message = f"{dzialka_id}: {exc}"
                logger.exception("[FutureBuildabilityEngine] %s", message)
                report.errors.append(message)

        await self.db.commit()
        report.duration_s = round(asyncio.get_event_loop().time() - started, 2)
        logger.info(
            "[FutureBuildabilityEngine] analyzed=%d assessments_new=%d assessments_updated=%d "
            "leads_new=%d leads_updated=%d duration=%.2fs",
            report.dzialki_analyzed,
            report.assessments_created,
            report.assessments_updated,
            report.leads_created,
            report.leads_updated,
            report.duration_s,
        )
        return report

    async def _assess_one(self, dzialka_id: UUID) -> tuple[Optional[bool], Optional[bool]]:
        dzialka = await self.db.get(Dzialka, dzialka_id)
        if dzialka is None:
            return None, None

        listing_id, listing_price_zl, source_url = await self._fetch_listing_context(dzialka_id)
        price_per_m2_zl: Decimal | None = None
        if listing_price_zl is not None and dzialka.area_m2 and dzialka.area_m2 > 0:
            price_per_m2_zl = (listing_price_zl / Decimal(str(dzialka.area_m2))).quantize(Decimal("0.01"))

        current_buildable_status = await self._current_buildable_status(dzialka_id)
        signal_rows = await self._fetch_signal_rows(dzialka_id)
        heuristics = await self._fetch_heuristics(dzialka_id)
        benchmark = await self.market_benchmark(dzialka.teryt_gmina)

        signal_breakdown: list[dict[str, Any]] = []
        future_signal_score = Decimal("0.00")
        dominant_future_signal: str | None = None
        positive_signal_count = 0
        hard_negative = False
        has_formal_signal = False
        has_supporting_formal_signal = False
        has_corroborated_supporting_signal = False
        dominant_unknown_resolution = False

        for row in signal_rows:
            weight = score_signal(
                signal_kind=row["signal_kind"],
                designation_normalized=row["designation_normalized"],
                signal_status=row["signal_status"],
            )
            if weight == 0 and row["designation_normalized"] not in HARD_NEGATIVE_DESIGNATIONS:
                continue
            if weight > 0:
                positive_signal_count += 1
            if weight < 0 and (row["designation_normalized"] or "") in HARD_NEGATIVE_DESIGNATIONS:
                hard_negative = True
            if row["signal_kind"] in {"pog_zone", "pog_ouz", "studium_zone"} and weight > 0:
                has_formal_signal = True
            if row["signal_kind"] in {"planning_resolution", "mpzp_project"} and weight > 0:
                has_supporting_formal_signal = True
            future_signal_score += weight
            label = signal_evidence_label(
                row["signal_kind"],
                row["designation_normalized"],
                row["plan_name"],
            )
            if dominant_future_signal is None and weight > 0:
                dominant_future_signal = label
                if row["signal_kind"] == "planning_resolution" and (row["designation_normalized"] or "") == "unknown":
                    dominant_unknown_resolution = True
            signal_breakdown.append(
                {
                    "kind": row["signal_kind"],
                    "status": row["signal_status"],
                    "designation_raw": row["designation_raw"],
                    "designation_normalized": row["designation_normalized"],
                    "weight": float(weight),
                    "source_url": row["source_url"],
                    "evidence_label": label,
                }
            )

        corroboration_bonus, has_corroborated_supporting_signal, corroboration_breakdown = (
            _score_supporting_signal_corroboration(signal_rows)
        )
        future_signal_score += corroboration_bonus
        signal_breakdown.extend(corroboration_breakdown)

        heuristics_bonus, heuristic_hits, heuristic_breakdown = _score_spatial_heuristics(heuristics)
        signal_breakdown.extend(heuristic_breakdown)
        distance_to_nearest = _decimal_or_none(heuristics.get("distance_to_nearest_buildable_m"))
        adjacent_pct = _decimal_or_none(heuristics.get("adjacent_buildable_pct"))

        future_signal_score = min(
            Decimal("100.00"),
            max(Decimal("0.00"), future_signal_score + heuristics_bonus),
        )
        cheapness_score = self._cheapness_score(price_per_m2_zl, benchmark)
        overall_score = min(Decimal("100.00"), future_signal_score + cheapness_score)

        confidence_band = _derive_confidence_band(
            current_buildable_status=current_buildable_status,
            overall_score=overall_score,
            future_signal_score=future_signal_score,
            cheapness_score=cheapness_score,
            has_formal_signal=has_formal_signal,
            has_supporting_formal_signal=has_supporting_formal_signal,
            has_corroborated_supporting_signal=has_corroborated_supporting_signal,
            heuristic_hits=heuristic_hits,
            hard_negative=hard_negative,
            dominant_unknown_resolution=dominant_unknown_resolution,
        )

        evidence_chain: list[dict[str, Any]] = []
        if source_url:
            evidence_chain.append({"step": "source", "ref": str(listing_id), "url": source_url})
        evidence_chain.append({"step": "parcel", "ref": str(dzialka.id), "teryt": dzialka.identyfikator})
        evidence_chain.extend((row["evidence_chain"] or [])[:1] for row in signal_rows if row["evidence_chain"])
        evidence_chain.append(
            {
                "step": "future_assessment",
                "ref": str(dzialka.id),
                "future_signal_score": float(future_signal_score),
                "cheapness_score": float(cheapness_score),
                "overall_score": float(overall_score),
                "confidence_band": confidence_band,
            }
        )
        evidence_chain = _flatten_chain(evidence_chain)

        assessment_created = await self._upsert_assessment(
            dzialka=dzialka,
            listing_id=listing_id,
            current_buildable_status=current_buildable_status,
            future_signal_score=future_signal_score,
            cheapness_score=cheapness_score,
            overall_score=overall_score,
            confidence_band=confidence_band,
            dominant_future_signal=dominant_future_signal,
            future_signal_count=positive_signal_count,
            distance_to_nearest_buildable_m=distance_to_nearest,
            adjacent_buildable_pct=adjacent_pct,
            price_per_m2_zl=price_per_m2_zl,
            evidence_chain=evidence_chain,
            signal_breakdown=signal_breakdown,
        )

        lead_created: Optional[bool] = None
        if confidence_band is not None and current_buildable_status == "non_buildable":
            lead_created = await self._upsert_future_lead(
                dzialka=dzialka,
                listing_id=listing_id,
                confidence_band=confidence_band,
                overall_score=overall_score,
                price_per_m2_zl=price_per_m2_zl,
                dominant_future_signal=dominant_future_signal,
                future_signal_score=future_signal_score,
                cheapness_score=cheapness_score,
                future_signal_count=positive_signal_count,
                distance_to_nearest_buildable_m=distance_to_nearest,
                adjacent_buildable_pct=adjacent_pct,
                evidence_chain=evidence_chain,
                signal_breakdown=signal_breakdown,
            )
        else:
            await self.db.execute(
                delete(InvestmentLead).where(
                    InvestmentLead.dzialka_id == dzialka.id,
                    InvestmentLead.strategy_type == "future_buildable",
                )
            )

        return assessment_created, lead_created

    async def _fetch_pending_dzialki(self, batch_size: int) -> list[UUID]:
        result = await self.db.execute(_PENDING_DZIALKI_QUERY.bindparams(batch_size=batch_size))
        return [row[0] for row in result.fetchall()]

    async def _fetch_signal_rows(self, dzialka_id: UUID) -> list[dict[str, Any]]:
        coverage_teryt = await self._coverage_teryt_for_dzialka(dzialka_id)
        result = await self.db.execute(
            _SIGNAL_QUERY.bindparams(dzialka_id=dzialka_id, coverage_teryt=coverage_teryt)
        )
        return [dict(row) for row in result.mappings().all()]

    async def _coverage_teryt_for_dzialka(self, dzialka_id: UUID) -> str | None:
        result = await self.db.execute(
            select(Dzialka.teryt_gmina).where(Dzialka.id == dzialka_id)
        )
        teryt = result.scalar_one_or_none()
        return coverage_alias_teryt(teryt) if isinstance(teryt, str) else None

    async def _fetch_heuristics(self, dzialka_id: UUID) -> dict[str, Any]:
        result = await self.db.execute(_HEURISTICS_QUERY.bindparams(dzialka_id=dzialka_id))
        row = result.mappings().one()
        return dict(row)

    async def _current_buildable_status(self, dzialka_id: UUID) -> str:
        result = await self.db.execute(_CURRENT_BUILDABLE_SIGNAL_QUERY.bindparams(dzialka_id=dzialka_id))
        row = result.mappings().one()
        if row["has_current_lead"]:
            return "already_buildable"
        if row["has_upgrade_delta"]:
            return "mixed"
        dzialka = await self.db.get(Dzialka, dzialka_id)
        current_use = (dzialka.current_use or "").strip() if dzialka else ""
        if current_use in _BUILT_EGIB_CODES:
            return "already_buildable"
        if current_use in _AGRICULTURAL_EGIB_CODES or not current_use:
            return "non_buildable"
        return "mixed"

    async def _fetch_listing_context(
        self,
        dzialka_id: UUID,
    ) -> tuple[Optional[UUID], Decimal | None, str | None]:
        result = await self.db.execute(
            select(ListingParcel.listing_id, RawListing.price_zl, RawListing.source_url)
            .join(RawListing, RawListing.id == ListingParcel.listing_id)
            .where(ListingParcel.dzialka_id == dzialka_id)
            .order_by(ListingParcel.created_at.desc())
            .limit(1)
        )
        row = result.first()
        if row is None:
            return None, None, None
        listing_id, price_zl, source_url = row
        return listing_id, (_decimal_or_none(price_zl)), source_url

    async def market_benchmark(self, teryt_gmina: str) -> MarketBenchmark:
        scopes = (
            ("gmina", len(teryt_gmina), teryt_gmina),
            ("powiat", 4, teryt_gmina[:4]),
            ("wojewodztwo", 2, teryt_gmina[:2]),
        )
        for scope, prefix_len, prefix_value in scopes:
            result = await self.db.execute(
                _BENCHMARK_QUERY.bindparams(prefix_len=prefix_len, prefix_value=prefix_value)
            )
            row = result.mappings().one()
            sample_size = int(row["sample_size"] or 0)
            if sample_size == 0:
                continue
            return MarketBenchmark(
                scope=scope,
                sample_size=sample_size,
                p25_price_per_m2_zl=_decimal_or_none(row["p25_price_per_m2_zl"]),
                p40_price_per_m2_zl=_decimal_or_none(row["p40_price_per_m2_zl"]),
                median_price_per_m2_zl=_decimal_or_none(row["median_price_per_m2_zl"]),
            )
        return MarketBenchmark(
            scope="wojewodztwo",
            sample_size=0,
            p25_price_per_m2_zl=None,
            p40_price_per_m2_zl=None,
            median_price_per_m2_zl=None,
        )

    def _cheapness_score(
        self,
        price_per_m2_zl: Decimal | None,
        benchmark: MarketBenchmark,
    ) -> Decimal:
        if price_per_m2_zl is None:
            return Decimal("0.00")
        if benchmark.sample_size == 0:
            return Decimal("0.00")
        if benchmark.p25_price_per_m2_zl is not None and price_per_m2_zl <= benchmark.p25_price_per_m2_zl:
            return Decimal("20.00")
        if benchmark.p40_price_per_m2_zl is not None and price_per_m2_zl <= benchmark.p40_price_per_m2_zl:
            return Decimal("10.00")
        return Decimal("0.00")

    async def _upsert_assessment(
        self,
        *,
        dzialka: Dzialka,
        listing_id: Optional[UUID],
        current_buildable_status: str,
        future_signal_score: Decimal,
        cheapness_score: Decimal,
        overall_score: Decimal,
        confidence_band: Optional[str],
        dominant_future_signal: Optional[str],
        future_signal_count: int,
        distance_to_nearest_buildable_m: Decimal | None,
        adjacent_buildable_pct: Decimal | None,
        price_per_m2_zl: Decimal | None,
        evidence_chain: list[dict[str, Any]],
        signal_breakdown: list[dict[str, Any]],
    ) -> bool:
        existing = (
            await self.db.execute(
                select(FutureBuildabilityAssessment).where(
                    FutureBuildabilityAssessment.dzialka_id == dzialka.id
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            self.db.add(
                FutureBuildabilityAssessment(
                    dzialka_id=dzialka.id,
                    listing_id=listing_id,
                    current_use=dzialka.current_use,
                    current_buildable_status=current_buildable_status,
                    future_signal_score=future_signal_score,
                    cheapness_score=cheapness_score,
                    overall_score=overall_score,
                    confidence_band=confidence_band,
                    dominant_future_signal=dominant_future_signal,
                    future_signal_count=future_signal_count,
                    distance_to_nearest_buildable_m=distance_to_nearest_buildable_m,
                    adjacent_buildable_pct=adjacent_buildable_pct,
                    price_per_m2_zl=price_per_m2_zl,
                    evidence_chain=evidence_chain,
                    signal_breakdown=signal_breakdown,
                )
            )
            return True

        existing.listing_id = listing_id
        existing.current_use = dzialka.current_use
        existing.current_buildable_status = current_buildable_status
        existing.future_signal_score = future_signal_score
        existing.cheapness_score = cheapness_score
        existing.overall_score = overall_score
        existing.confidence_band = confidence_band
        existing.dominant_future_signal = dominant_future_signal
        existing.future_signal_count = future_signal_count
        existing.distance_to_nearest_buildable_m = distance_to_nearest_buildable_m
        existing.adjacent_buildable_pct = adjacent_buildable_pct
        existing.price_per_m2_zl = price_per_m2_zl
        existing.evidence_chain = evidence_chain
        existing.signal_breakdown = signal_breakdown
        existing.updated_at = datetime.now(timezone.utc)
        return False

    async def _upsert_future_lead(
        self,
        *,
        dzialka: Dzialka,
        listing_id: Optional[UUID],
        confidence_band: str,
        overall_score: Decimal,
        price_per_m2_zl: Decimal | None,
        dominant_future_signal: Optional[str],
        future_signal_score: Decimal,
        cheapness_score: Decimal,
        future_signal_count: int,
        distance_to_nearest_buildable_m: Decimal | None,
        adjacent_buildable_pct: Decimal | None,
        evidence_chain: list[dict[str, Any]],
        signal_breakdown: list[dict[str, Any]],
    ) -> bool:
        confidence_score = min(Decimal("1.00"), (overall_score / Decimal("100.00")).quantize(Decimal("0.01")))
        priority = "high" if confidence_score >= Decimal("0.90") else "medium" if confidence_score >= Decimal("0.75") else "low"

        existing = (
            await self.db.execute(
                select(InvestmentLead).where(
                    InvestmentLead.dzialka_id == dzialka.id,
                    InvestmentLead.strategy_type == "future_buildable",
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            self.db.add(
                InvestmentLead(
                    dzialka_id=dzialka.id,
                    listing_id=listing_id,
                    confidence_score=confidence_score,
                    priority=priority,
                    strategy_type="future_buildable",
                    confidence_band=confidence_band,
                    price_per_m2_zl=price_per_m2_zl,
                    future_signal_score=future_signal_score,
                    cheapness_score=cheapness_score,
                    overall_score=overall_score,
                    dominant_future_signal=dominant_future_signal,
                    future_signal_count=future_signal_count,
                    distance_to_nearest_buildable_m=distance_to_nearest_buildable_m,
                    adjacent_buildable_pct=adjacent_buildable_pct,
                    evidence_chain=evidence_chain,
                    signal_breakdown=signal_breakdown,
                    status="new",
                )
            )
            return True

        existing.listing_id = listing_id
        existing.confidence_score = confidence_score
        existing.priority = priority
        existing.confidence_band = confidence_band
        existing.price_per_m2_zl = price_per_m2_zl
        existing.future_signal_score = future_signal_score
        existing.cheapness_score = cheapness_score
        existing.overall_score = overall_score
        existing.dominant_future_signal = dominant_future_signal
        existing.future_signal_count = future_signal_count
        existing.distance_to_nearest_buildable_m = distance_to_nearest_buildable_m
        existing.adjacent_buildable_pct = adjacent_buildable_pct
        existing.evidence_chain = evidence_chain
        existing.signal_breakdown = signal_breakdown
        existing.updated_at = datetime.now(timezone.utc)
        return False


def _flatten_chain(entries: list[Any]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for entry in entries:
        if isinstance(entry, list):
            for child in entry:
                if isinstance(child, dict):
                    flattened.append(child)
        elif isinstance(entry, dict):
            flattened.append(entry)
    return flattened


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _score_spatial_heuristics(heuristics: dict[str, Any]) -> tuple[Decimal, int, list[dict[str, Any]]]:
    bonus = Decimal("0.00")
    hits = 0
    breakdown: list[dict[str, Any]] = []

    distance_to_nearest = _decimal_or_none(heuristics.get("distance_to_nearest_buildable_m"))
    adjacent_pct = _decimal_or_none(heuristics.get("adjacent_buildable_pct"))
    shared_boundary_m = _decimal_or_none(heuristics.get("shared_boundary_m"))
    distance_to_mixed_service = _decimal_or_none(heuristics.get("distance_to_mixed_service_zone_m"))
    distance_to_meaningful_road = _decimal_or_none(heuristics.get("distance_to_meaningful_road_m"))
    has_road_access_signal = bool(heuristics.get("has_road_access_signal"))
    has_urban_cluster_signal = bool(heuristics.get("has_urban_cluster_signal"))

    if distance_to_nearest is not None and distance_to_nearest <= Decimal("50.00"):
        bonus += Decimal("10.00")
        hits += 1
        breakdown.append(
            {
                "kind": "heuristic_near_buildable",
                "status": "heuristic",
                "designation_raw": None,
                "designation_normalized": "near_buildable",
                "weight": 10.0,
                "source_url": None,
                "evidence_label": f"distance_to_nearest_buildable <= 50m ({distance_to_nearest}m)",
            }
        )

    if adjacent_pct is not None and adjacent_pct >= Decimal("25.00"):
        bonus += Decimal("10.00")
        hits += 1
        breakdown.append(
            {
                "kind": "heuristic_adjacent_buildable",
                "status": "heuristic",
                "designation_raw": None,
                "designation_normalized": "adjacent_buildable",
                "weight": 10.0,
                "source_url": None,
                "evidence_label": f"adjacent_buildable_pct >= 25% ({adjacent_pct}%)",
            }
        )

    if shared_boundary_m is not None and shared_boundary_m >= Decimal("8.00"):
        bonus += Decimal("12.00")
        hits += 1
        breakdown.append(
            {
                "kind": "heuristic_shared_boundary",
                "status": "heuristic",
                "designation_raw": None,
                "designation_normalized": "shared_boundary",
                "weight": 12.0,
                "source_url": None,
                "evidence_label": f"shared_boundary_m >= 8m ({shared_boundary_m}m)",
            }
        )

    if distance_to_mixed_service is not None and distance_to_mixed_service <= Decimal("100.00"):
        bonus += Decimal("8.00")
        hits += 1
        breakdown.append(
            {
                "kind": "heuristic_mixed_service_proximity",
                "status": "heuristic",
                "designation_raw": None,
                "designation_normalized": "mixed_service",
                "weight": 8.0,
                "source_url": None,
                "evidence_label": (
                    f"distance_to_mixed_service_zone <= 100m ({distance_to_mixed_service}m)"
                ),
            }
        )

    if distance_to_meaningful_road is not None and distance_to_meaningful_road <= Decimal("60.00"):
        bonus += Decimal("6.00")
        hits += 1
        breakdown.append(
            {
                "kind": "heuristic_meaningful_road",
                "status": "heuristic",
                "designation_raw": "KD*",
                "designation_normalized": "road_hierarchy",
                "weight": 6.0,
                "source_url": None,
                "evidence_label": (
                    f"distance_to_meaningful_road <= 60m ({distance_to_meaningful_road}m)"
                ),
            }
        )

    if has_road_access_signal:
        bonus += Decimal("5.00")
        hits += 1
        breakdown.append(
            {
                "kind": "heuristic_road_access",
                "status": "heuristic",
                "designation_raw": "KD",
                "designation_normalized": "road",
                "weight": 5.0,
                "source_url": None,
                "evidence_label": "KD / road signal within 30m",
            }
        )

    if has_urban_cluster_signal:
        bonus += Decimal("10.00")
        hits += 1
        breakdown.append(
            {
                "kind": "heuristic_urban_cluster",
                "status": "heuristic",
                "designation_raw": None,
                "designation_normalized": "urban_cluster",
                "weight": 10.0,
                "source_url": None,
                "evidence_label": "Current buildable zone within 100m",
            }
        )

    if (
        shared_boundary_m is not None
        and shared_boundary_m > Decimal("0.00")
        and distance_to_nearest is not None
        and distance_to_nearest <= Decimal("25.00")
        and has_urban_cluster_signal
    ):
        bonus += Decimal("8.00")
        hits += 1
        breakdown.append(
            {
                "kind": "heuristic_expansion_edge",
                "status": "heuristic",
                "designation_raw": None,
                "designation_normalized": "expansion_edge",
                "weight": 8.0,
                "source_url": None,
                "evidence_label": (
                    "Expansion-edge parcel: shared boundary with buildable area and urban cluster proximity"
                ),
            }
        )

    return bonus, hits, breakdown


def _score_supporting_signal_corroboration(
    signal_rows: list[dict[str, Any]],
) -> tuple[Decimal, bool, list[dict[str, Any]]]:
    grouped_sources: dict[str, set[str]] = defaultdict(set)
    grouped_family_sources: dict[str, set[str]] = defaultdict(set)

    for row in signal_rows:
        signal_kind = row["signal_kind"]
        designation = row["designation_normalized"] or ""
        if signal_kind not in {"planning_resolution", "mpzp_project"}:
            continue
        if designation not in POSITIVE_DESIGNATIONS:
            continue
        if score_signal(
            signal_kind=signal_kind,
            designation_normalized=designation,
            signal_status=row["signal_status"],
        ) <= 0:
            continue
        source_key = row["source_url"] or row["plan_name"] or str(row["id"])
        grouped_sources[designation].add(source_key)
        grouped_family_sources["urbanizable"].add(source_key)

    best_designation = next(
        (
            designation
            for designation, sources in sorted(
                grouped_sources.items(),
                key=lambda item: (-len(item[1]), item[0]),
            )
            if len(sources) >= 3
        ),
        None,
    )
    if best_designation is not None:
        corroboration_count = len(grouped_sources[best_designation])
        return (
            Decimal("10.00"),
            True,
            [
                {
                    "kind": "supporting_signal_corroboration",
                    "status": "formal_preparatory",
                    "designation_raw": None,
                    "designation_normalized": best_designation,
                    "weight": 10.0,
                    "source_url": None,
                    "evidence_label": (
                        f"{corroboration_count} corroborating supporting sources for {best_designation}"
                    ),
                }
            ],
        )

    urbanizable_count = len(grouped_family_sources["urbanizable"])
    if urbanizable_count < 3:
        return Decimal("0.00"), False, []

    return (
        Decimal("10.00"),
        True,
        [
            {
                "kind": "supporting_signal_corroboration",
                "status": "formal_preparatory",
                "designation_raw": None,
                "designation_normalized": "urbanizable",
                "weight": 10.0,
                "source_url": None,
                "evidence_label": (
                    f"{urbanizable_count} corroborating supporting sources for urbanizable uses"
                ),
            }
        ],
    )


def _derive_confidence_band(
    *,
    current_buildable_status: str,
    overall_score: Decimal,
    future_signal_score: Decimal,
    cheapness_score: Decimal = Decimal("0.00"),
    has_formal_signal: bool,
    has_supporting_formal_signal: bool,
    has_corroborated_supporting_signal: bool,
    heuristic_hits: int,
    hard_negative: bool,
    dominant_unknown_resolution: bool,
) -> str | None:
    if current_buildable_status != "non_buildable":
        return None
    if dominant_unknown_resolution:
        return None
    if overall_score >= Decimal("60.00"):
        if has_formal_signal:
            return "formal"
        if (
            has_supporting_formal_signal
            and future_signal_score >= Decimal("40.00")
            and heuristic_hits >= 2
            and not hard_negative
        ):
            return "supported"
        if (
            has_supporting_formal_signal
            and has_corroborated_supporting_signal
            and future_signal_score >= Decimal("40.00")
            and not hard_negative
        ):
            return "supported"
        if heuristic_hits >= 2 and not hard_negative:
            return "speculative"
        return None
    if (
        overall_score >= Decimal("55.00")
        and future_signal_score >= Decimal("58.00")
        and has_formal_signal
        and heuristic_hits >= 5
        and not hard_negative
    ):
        # Geometry-backed directional signals can still be investor-useful just
        # below the formal cut-off when several independent spatial heuristics
        # point in the same direction.
        return "supported"
    if (
        overall_score >= Decimal("30.00")
        and cheapness_score >= Decimal("20.00")
        and future_signal_score >= Decimal("10.00")
        and has_supporting_formal_signal
        and not hard_negative
    ):
        # Conservative signal-only fallback: a positive formal planning
        # resolution plus a clearly cheap entry price is enough to surface a
        # low-confidence investor lead even before geometry-backed coverage is
        # available. This keeps the lead in the automatic pipeline without
        # pretending that we already have parcel-safe zoning polygons.
        return "supported"
    return None


def _derive_signal_quality_tier(
    *,
    strategy_type: str | None,
    confidence_band: str | None,
    signal_breakdown: list[dict[str, Any]] | None,
) -> str | None:
    """Return a compact investor-facing interpretation of a future-buildable lead."""

    if strategy_type != "future_buildable":
        return None
    if confidence_band == "formal":
        return "formal"
    if confidence_band == "supported":
        return "supported"

    breakdown = signal_breakdown or []
    has_positive_signal = any(float(item.get("weight") or 0.0) > 0 for item in breakdown)
    has_hard_negative = any(
        float(item.get("weight") or 0.0) < 0
        and (item.get("designation_normalized") or "") in HARD_NEGATIVE_DESIGNATIONS
        for item in breakdown
    )

    if confidence_band == "speculative" or has_positive_signal:
        return "below_threshold"
    if has_hard_negative:
        return "blocked"
    return "below_threshold"


def _derive_next_best_action(
    *,
    strategy_type: str | None,
    confidence_band: str | None,
    signal_breakdown: list[dict[str, Any]] | None,
    dominant_future_signal: str | None,
) -> str | None:
    """Return a short operator action that explains the next step."""

    if strategy_type != "future_buildable":
        return None

    tier = _derive_signal_quality_tier(
        strategy_type=strategy_type,
        confidence_band=confidence_band,
        signal_breakdown=signal_breakdown,
    )
    breakdown = signal_breakdown or []
    has_formal_geometry_signal = any(
        item.get("kind") in {"pog_zone", "pog_ouz", "studium_zone"}
        and float(item.get("weight") or 0.0) > 0
        for item in breakdown
    )
    has_supporting_formal_signal = any(
        item.get("kind") in {"planning_resolution", "mpzp_project"}
        and float(item.get("weight") or 0.0) > 0
        for item in breakdown
    )
    has_positive_signal = any(float(item.get("weight") or 0.0) > 0 for item in breakdown)
    has_hard_negative = any(
        float(item.get("weight") or 0.0) < 0
        and (item.get("designation_normalized") or "") in HARD_NEGATIVE_DESIGNATIONS
        for item in breakdown
    )

    if tier == "formal":
        return "Shortlist and verify access, price, and source docs."
    if tier == "supported":
        return "Confirm the planning source and validate the parcel boundary."
    if has_hard_negative and not has_positive_signal:
        return "Skip or re-scope this parcel; hard-negative planning signals dominate."
    if has_supporting_formal_signal and not has_formal_geometry_signal:
        return "Look for geometry-backed POG/Studium coverage to lift it above threshold."
    if dominant_future_signal:
        return "Add stronger heuristics or a formal planning source to lift the dominant signal."
    return "Research missing planning coverage or find a better planning source."


async def run_future_buildability_engine(
    *,
    batch_size: int = 100,
    dzialka_ids: Optional[list[UUID]] = None,
) -> FutureBuildabilityReport:
    from app.core.database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        engine = FutureBuildabilityEngine(db)
        return await engine.calculate_assessments(
            dzialka_ids=dzialka_ids,
            batch_size=batch_size,
        )
