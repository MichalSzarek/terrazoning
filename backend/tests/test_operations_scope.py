from decimal import Decimal

from app.services.operations_scope import (
    classify_backlog_status,
    classify_dlq_error,
    classify_lead_quality,
    classify_price_signal,
    compute_investment_score,
    coverage_alias_teryt,
    normalize_province,
    province_db_label,
    province_display_name,
    province_teryt_prefix,
)


def test_normalize_province_handles_ascii_and_diacritics() -> None:
    assert normalize_province("slaskie") == "slaskie"
    assert normalize_province("ŚLĄSKIE") == "slaskie"
    assert normalize_province("małopolskie") == "malopolskie"
    assert normalize_province("Podkarpackie") == "podkarpackie"


def test_province_labels_and_prefixes_match_expected_runtime_scope() -> None:
    assert province_db_label("podkarpackie") == "podkarpackie"
    assert province_db_label("slaskie") == "śląskie"
    assert province_db_label("malopolskie") == "małopolskie"
    assert province_display_name("podkarpackie") == "Podkarpackie"
    assert province_display_name("slaskie") == "Śląskie"
    assert province_display_name("malopolskie") == "Małopolskie"
    assert province_teryt_prefix("podkarpackie") == "18"
    assert province_teryt_prefix("slaskie") == "24"
    assert province_teryt_prefix("malopolskie") == "12"


def test_classify_backlog_status_distinguishes_source_gap_from_covered_gap() -> None:
    status, hint = classify_backlog_status(
        in_registry=False,
        has_planning_zones=False,
        has_delta_rows=False,
    )
    assert status == "no_source_configured"
    assert "No confirmed MPZP source" in hint

    status, hint = classify_backlog_status(
        in_registry=True,
        has_planning_zones=False,
        has_delta_rows=False,
    )
    assert status == "source_configured_but_not_loaded"
    assert "source is already configured" in hint

    status, hint = classify_backlog_status(
        in_registry=True,
        has_planning_zones=True,
        has_delta_rows=False,
    )
    assert status == "covered_but_no_delta"
    assert "Planning coverage already exists" in hint

    status, hint = classify_backlog_status(
        in_registry=True,
        has_planning_zones=True,
        has_delta_rows=True,
    )
    assert status == "covered_but_no_buildable_delta"
    assert "delta intersections exist" in hint


def test_coverage_alias_teryt_maps_krakow_egib_units_to_city_code() -> None:
    assert coverage_alias_teryt("1261049") == "1261011"
    assert coverage_alias_teryt("1261039") == "1261011"
    assert coverage_alias_teryt("1213062") == "1213062"


def test_classify_price_signal_marks_missing_and_suspicious_cases() -> None:
    assert classify_price_signal(price_zl=None, price_per_m2_zl=None) == "missing"
    assert classify_price_signal(price_zl=Decimal("98.00"), price_per_m2_zl=Decimal("12.00")) == "suspicious"
    assert classify_price_signal(price_zl=Decimal("10000.00"), price_per_m2_zl=Decimal("3.00")) == "suspicious"
    assert classify_price_signal(price_zl=Decimal("10000.00"), price_per_m2_zl=Decimal("125.00")) == "reliable"


def test_classify_lead_quality_exposes_missing_financials_and_review_required() -> None:
    quality, missing = classify_lead_quality(
        price_zl=None,
        price_per_m2_zl=None,
        area_m2=Decimal("800"),
        max_buildable_area_m2=Decimal("600"),
        max_coverage_pct=Decimal("75"),
        dominant_przeznaczenie="MN",
    )
    assert quality == "missing_financials"
    assert "price_zl" in missing
    assert "price_per_m2_zl" in missing

    quality, missing = classify_lead_quality(
        price_zl=Decimal("10000"),
        price_per_m2_zl=Decimal("2.50"),
        area_m2=Decimal("800"),
        max_buildable_area_m2=Decimal("600"),
        max_coverage_pct=Decimal("75"),
        dominant_przeznaczenie="MN",
    )
    assert quality == "review_required"
    assert missing == ()

    quality, missing = classify_lead_quality(
        price_zl=Decimal("120000"),
        price_per_m2_zl=Decimal("180"),
        area_m2=Decimal("800"),
        max_buildable_area_m2=Decimal("700"),
        max_coverage_pct=Decimal("8.5"),
        dominant_przeznaczenie="U",
    )
    assert quality == "review_required"
    assert missing == ()


def test_compute_investment_score_penalizes_suspicious_price_and_low_coverage() -> None:
    strong = compute_investment_score(
        confidence_score=Decimal("0.92"),
        price_zl=Decimal("120000"),
        price_per_m2_zl=Decimal("150"),
        max_buildable_area_m2=Decimal("1200"),
        max_coverage_pct=Decimal("78"),
        price_signal="reliable",
        quality_signal="complete",
    )
    weak = compute_investment_score(
        confidence_score=Decimal("0.92"),
        price_zl=Decimal("98"),
        price_per_m2_zl=Decimal("4"),
        max_buildable_area_m2=Decimal("1200"),
        max_coverage_pct=Decimal("8"),
        price_signal="suspicious",
        quality_signal="review_required",
    )
    assert strong > weak


def test_classify_dlq_error_routes_manual_and_parser_cases() -> None:
    category, _ = classify_dlq_error(
        last_error="GEOMETRY_MISSING: ULDK returned status=-1",
        attempt_count=5,
    )
    assert category == "manual_only_case"

    category, _ = classify_dlq_error(
        last_error="PARSE_ERROR: missing parcel number in source text",
        attempt_count=1,
    )
    assert category == "parser_issue"

    category, _ = classify_dlq_error(
        last_error="ULDK_NOT_FOUND: raw_obreb='Zwonowice' raw_numer_dzialki='Stodolska'",
        attempt_count=2,
        raw_obreb="Zwonowice",
        raw_numer_dzialki="Stodolska",
    )
    assert category == "parser_issue"

    category, _ = classify_dlq_error(
        last_error="ULDK_NOT_FOUND: raw_obreb='Użytkowanie Wieczyste' raw_numer_dzialki='418'",
        attempt_count=3,
        raw_obreb="Użytkowanie Wieczyste",
        raw_numer_dzialki="418",
    )
    assert category == "parser_issue"

    category, _ = classify_dlq_error(
        last_error="urban fallback: ambiguous parcel number across 18 region(s)",
        attempt_count=2,
    )
    assert category == "resolver_ambiguity"
