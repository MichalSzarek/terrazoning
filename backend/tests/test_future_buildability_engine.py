from decimal import Decimal

from app.services.future_buildability_engine import (
    MarketBenchmark,
    FutureBuildabilityEngine,
    _derive_confidence_band,
    _derive_next_best_action,
    _derive_signal_quality_tier,
    _score_supporting_signal_corroboration,
    _score_spatial_heuristics,
)
from app.services.planning_signal_utils import (
    normalize_designation_class,
    score_signal,
)


def test_normalize_designation_class_maps_future_buildability_taxonomy() -> None:
    assert normalize_designation_class("19.MN", None) == "residential"
    assert normalize_designation_class("U/MN-3", None) == "mixed_residential"
    assert normalize_designation_class("CU1", "centrum usług") == "service"
    assert normalize_designation_class("UP", "tereny usługowo-produkcyjne") == "service"
    assert normalize_designation_class("KD-L", "droga lokalna") == "road"
    assert normalize_designation_class("ZL", "lasy ochronne") == "forest"


def test_score_signal_respects_positive_and_negative_weights() -> None:
    assert score_signal(
        signal_kind="pog_zone",
        designation_normalized="residential",
        signal_status="formal_directional",
    ) == Decimal("55.00")
    assert score_signal(
        signal_kind="studium_zone",
        designation_normalized="service",
        signal_status="formal_directional",
    ) == Decimal("20.00")
    assert score_signal(
        signal_kind="pog_zone",
        designation_normalized="forest",
        signal_status="formal_directional",
    ) == Decimal("-40.00")
    assert score_signal(
        signal_kind="planning_resolution",
        designation_normalized="unknown",
        signal_status="formal_directional",
    ) == Decimal("0.00")
    assert score_signal(
        signal_kind="planning_resolution",
        designation_normalized="residential",
        signal_status="formal_directional",
    ) == Decimal("10.00")


def test_cheapness_score_uses_market_percentiles() -> None:
    engine = FutureBuildabilityEngine.__new__(FutureBuildabilityEngine)
    benchmark = MarketBenchmark(
        scope="gmina",
        sample_size=12,
        p25_price_per_m2_zl=Decimal("120.00"),
        p40_price_per_m2_zl=Decimal("160.00"),
        median_price_per_m2_zl=Decimal("210.00"),
    )

    assert engine._cheapness_score(Decimal("110.00"), benchmark) == Decimal("20.00")
    assert engine._cheapness_score(Decimal("150.00"), benchmark) == Decimal("10.00")
    assert engine._cheapness_score(Decimal("220.00"), benchmark) == Decimal("0.00")


def test_confidence_band_accepts_supported_resolution_with_multiple_heuristics() -> None:
    assert _derive_confidence_band(
        current_buildable_status="non_buildable",
        overall_score=Decimal("50.00"),
        future_signal_score=Decimal("30.00"),
        has_formal_signal=False,
        has_supporting_formal_signal=True,
        has_corroborated_supporting_signal=False,
        heuristic_hits=2,
        hard_negative=False,
        dominant_unknown_resolution=False,
    ) is None


def test_confidence_band_promotes_strong_preparatory_signal_to_supported() -> None:
    assert _derive_confidence_band(
        current_buildable_status="non_buildable",
        overall_score=Decimal("61.00"),
        future_signal_score=Decimal("41.00"),
        has_formal_signal=False,
        has_supporting_formal_signal=True,
        has_corroborated_supporting_signal=False,
        heuristic_hits=2,
        hard_negative=False,
        dominant_unknown_resolution=False,
    ) == "supported"


def test_confidence_band_keeps_formal_threshold_for_geometry_backed_signals() -> None:
    assert _derive_confidence_band(
        current_buildable_status="non_buildable",
        overall_score=Decimal("50.00"),
        future_signal_score=Decimal("50.00"),
        has_formal_signal=True,
        has_supporting_formal_signal=False,
        has_corroborated_supporting_signal=False,
        heuristic_hits=1,
        hard_negative=False,
        dominant_unknown_resolution=False,
    ) is None


def test_confidence_band_accepts_near_threshold_geometry_backed_cases_as_supported() -> None:
    assert _derive_confidence_band(
        current_buildable_status="non_buildable",
        overall_score=Decimal("59.00"),
        future_signal_score=Decimal("59.00"),
        has_formal_signal=True,
        has_supporting_formal_signal=False,
        has_corroborated_supporting_signal=False,
        heuristic_hits=6,
        hard_negative=False,
        dominant_unknown_resolution=False,
    ) == "supported"


def test_confidence_band_does_not_promote_coverage_only_metadata_by_itself() -> None:
    assert _derive_confidence_band(
        current_buildable_status="non_buildable",
        overall_score=Decimal("65.00"),
        future_signal_score=Decimal("45.00"),
        has_formal_signal=False,
        has_supporting_formal_signal=False,
        has_corroborated_supporting_signal=False,
        heuristic_hits=0,
        hard_negative=False,
        dominant_unknown_resolution=False,
    ) is None


def test_confidence_band_blocks_unknown_planning_resolution_domination() -> None:
    assert _derive_confidence_band(
        current_buildable_status="non_buildable",
        overall_score=Decimal("70.00"),
        future_signal_score=Decimal("60.00"),
        has_formal_signal=False,
        has_supporting_formal_signal=True,
        has_corroborated_supporting_signal=False,
        heuristic_hits=5,
        hard_negative=False,
        dominant_unknown_resolution=True,
    ) is None


def test_supporting_signal_corroboration_rewards_three_consistent_sources() -> None:
    bonus, corroborated, breakdown = _score_supporting_signal_corroboration(
        [
            {
                "id": "a",
                "signal_kind": "mpzp_project",
                "signal_status": "formal_preparatory",
                "designation_normalized": "mixed_residential",
                "source_url": "https://example.test/a",
                "plan_name": "Plan A",
            },
            {
                "id": "b",
                "signal_kind": "mpzp_project",
                "signal_status": "formal_preparatory",
                "designation_normalized": "mixed_residential",
                "source_url": "https://example.test/b",
                "plan_name": "Plan B",
            },
            {
                "id": "c",
                "signal_kind": "planning_resolution",
                "signal_status": "formal_preparatory",
                "designation_normalized": "mixed_residential",
                "source_url": "https://example.test/c",
                "plan_name": "Plan C",
            },
        ]
    )

    assert bonus == Decimal("10.00")
    assert corroborated is True
    assert breakdown[0]["kind"] == "supporting_signal_corroboration"


def test_supporting_signal_corroboration_rewards_three_urbanizable_sources() -> None:
    bonus, corroborated, breakdown = _score_supporting_signal_corroboration(
        [
            {
                "id": "a",
                "signal_kind": "planning_resolution",
                "signal_status": "formal_preparatory",
                "designation_normalized": "mixed_residential",
                "source_url": "https://example.test/a",
                "plan_name": "Plan A",
            },
            {
                "id": "b",
                "signal_kind": "planning_resolution",
                "signal_status": "formal_preparatory",
                "designation_normalized": "residential",
                "source_url": "https://example.test/b",
                "plan_name": "Plan B",
            },
            {
                "id": "c",
                "signal_kind": "planning_resolution",
                "signal_status": "formal_directional",
                "designation_normalized": "mixed_residential",
                "source_url": "https://example.test/c",
                "plan_name": "Plan C",
            },
        ]
    )

    assert bonus == Decimal("10.00")
    assert corroborated is True
    assert breakdown[0]["designation_normalized"] == "urbanizable"


def test_confidence_band_accepts_corroborated_supporting_signal_without_spatial_heuristics() -> None:
    assert _derive_confidence_band(
        current_buildable_status="non_buildable",
        overall_score=Decimal("60.00"),
        future_signal_score=Decimal("40.00"),
        has_formal_signal=False,
        has_supporting_formal_signal=True,
        has_corroborated_supporting_signal=True,
        heuristic_hits=0,
        hard_negative=False,
        dominant_unknown_resolution=False,
    ) == "supported"


def test_signal_quality_tier_collapses_speculative_into_below_threshold() -> None:
    breakdown = [
        {
            "kind": "planning_resolution",
            "designation_normalized": "unknown",
            "weight": 10.0,
        }
    ]

    assert _derive_signal_quality_tier(
        strategy_type="future_buildable",
        confidence_band="speculative",
        signal_breakdown=breakdown,
    ) == "below_threshold"


def test_signal_quality_tier_marks_hard_negative_parcels_as_blocked() -> None:
    breakdown = [
        {
            "kind": "planning_resolution",
            "designation_normalized": "forest",
            "weight": -40.0,
        }
    ]

    assert _derive_signal_quality_tier(
        strategy_type="future_buildable",
        confidence_band=None,
        signal_breakdown=breakdown,
    ) == "blocked"


def test_next_best_action_reflects_tier_and_signal_mix() -> None:
    supported_breakdown = [
        {
            "kind": "planning_resolution",
            "designation_normalized": "unknown",
            "weight": 10.0,
        },
        {
            "kind": "heuristic_adjacent_buildable",
            "designation_normalized": "adjacent_buildable",
            "weight": 10.0,
        },
    ]
    blocked_breakdown = [
        {
            "kind": "planning_resolution",
            "designation_normalized": "forest",
            "weight": -40.0,
        }
    ]

    assert _derive_next_best_action(
        strategy_type="future_buildable",
        confidence_band="supported",
        signal_breakdown=supported_breakdown,
        dominant_future_signal="planning_resolution: local plan",
    ) == "Confirm the planning source and validate the parcel boundary."
    assert _derive_next_best_action(
        strategy_type="future_buildable",
        confidence_band=None,
        signal_breakdown=blocked_breakdown,
        dominant_future_signal=None,
    ) == "Skip or re-scope this parcel; hard-negative planning signals dominate."


def test_spatial_heuristics_include_shared_boundary_and_expansion_edge_signals() -> None:
    bonus, hits, breakdown = _score_spatial_heuristics(
        {
            "distance_to_nearest_buildable_m": Decimal("18.00"),
            "adjacent_buildable_pct": Decimal("31.00"),
            "shared_boundary_m": Decimal("12.50"),
            "distance_to_mixed_service_zone_m": Decimal("60.00"),
            "distance_to_meaningful_road_m": Decimal("45.00"),
            "has_road_access_signal": True,
            "has_urban_cluster_signal": True,
        }
    )

    kinds = {item["kind"] for item in breakdown}
    assert bonus >= Decimal("49.00")
    assert hits >= 6
    assert "heuristic_shared_boundary" in kinds
    assert "heuristic_mixed_service_proximity" in kinds
    assert "heuristic_meaningful_road" in kinds
    assert "heuristic_expansion_edge" in kinds
