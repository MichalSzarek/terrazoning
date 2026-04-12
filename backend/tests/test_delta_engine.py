from decimal import Decimal
from uuid import uuid4

from app.services.delta_engine import DeltaRow, is_buildable_symbol, normalize_symbol


def _sample_delta_row(przeznaczenie: str) -> DeltaRow:
    return DeltaRow(
        dzialka_id=uuid4(),
        identyfikator="126101_1.0001.123/4",
        match_confidence=Decimal("0.95"),
        dzialka_area_m2=Decimal("1000"),
        teryt_gmina="1261011",
        current_use="R",
        planning_zone_id=uuid4(),
        przeznaczenie=przeznaczenie,
        plan_type="mpzp",
        plan_name="Test plan",
        coverage_pct=Decimal("55.00"),
        intersection_area_m2=Decimal("550.0"),
        intersection_geom=None,
    )


def test_normalize_symbol_handles_local_mpzp_variants() -> None:
    assert normalize_symbol("19.MN") == "MN"
    assert normalize_symbol("U/MN-3") == "U/MN"
    assert normalize_symbol("MN.1") == "MN"
    assert normalize_symbol("1MNU") == "MNU"
    assert normalize_symbol("MW/U.1") == "MW/U"


def test_is_buildable_symbol_uses_normalized_typology() -> None:
    assert is_buildable_symbol("19.MN") is True
    assert is_buildable_symbol("U/MW-3") is True
    assert is_buildable_symbol("1MNU") is True
    assert is_buildable_symbol("1.ZP") is False


def test_delta_row_qualifies_for_lead_with_normalized_symbol() -> None:
    row = _sample_delta_row("U/MN-3")

    assert row.is_buildable is True
    assert row.is_genuine_delta is True
    assert row.qualifies_for_lead is True


def test_delta_row_qualifies_when_buildable_area_exceeds_absolute_threshold() -> None:
    row = _sample_delta_row("MN")
    row.coverage_pct = Decimal("12.00")
    row.intersection_area_m2 = Decimal("650.00")

    assert row.qualifies_for_lead is True


def test_delta_row_rejects_small_buildable_sliver_noise() -> None:
    row = _sample_delta_row("MN")
    row.coverage_pct = Decimal("8.00")
    row.intersection_area_m2 = Decimal("120.00")

    assert row.qualifies_for_lead is False
