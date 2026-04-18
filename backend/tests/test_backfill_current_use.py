from __future__ import annotations

from pathlib import Path

from backfill_current_use import (
    _infer_current_use_from_text,
    _load_rows_from_csv,
    _normalize_current_use,
)


def test_normalize_current_use_uppercases_and_keeps_egib_style_codes() -> None:
    assert _normalize_current_use("  ł/ps-iv ") == "Ł/PS-IV"


def test_load_rows_from_csv_reads_required_columns(tmp_path: Path) -> None:
    path = tmp_path / "current_use.csv"
    path.write_text(
        "identyfikator,current_use\n"
        "181614502.267/3,R\n"
        "181614502.267/4,Ps\n",
        encoding="utf-8",
    )

    rows = _load_rows_from_csv(path)

    assert len(rows) == 2
    assert rows[0].identyfikator == "181614502.267/3"
    assert rows[0].current_use == "R"
    assert rows[1].current_use == "PS"


def test_load_rows_from_csv_rejects_duplicates(tmp_path: Path) -> None:
    path = tmp_path / "current_use.csv"
    path.write_text(
        "identyfikator,current_use\n"
        "181614502.267/3,R\n"
        "181614502.267/3,Ps\n",
        encoding="utf-8",
    )

    try:
        _load_rows_from_csv(path)
    except ValueError as exc:
        assert "duplicate identyfikator" in str(exc)
    else:
        raise AssertionError("expected duplicate-identyfikator validation error")


def test_infer_current_use_from_text_marks_vacant_land_as_r() -> None:
    result = _infer_current_use_from_text(
        title="Licytacja działki niezabudowanej",
        raw_text="Nieruchomość gruntowa niezabudowana o przeznaczeniu rolnym.",
    )

    assert result == "R"


def test_infer_current_use_from_text_marks_built_property_as_b() -> None:
    result = _infer_current_use_from_text(
        title="Licytacja domu mieszkalnego",
        raw_text="Nieruchomość zabudowana budynkiem mieszkalnym jednorodzinnym.",
    )

    assert result == "B"
