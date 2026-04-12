from run_wfs_sync import _classify_uncovered_gmina


def test_classify_uncovered_gmina_known_no_source_from_failed_knurow_probe() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="2405011",
        in_registry=False,
    )
    assert category == "no_source_available"
    assert "connection errors" in next_action.lower()


def test_classify_uncovered_gmina_known_no_source() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1206105",
        in_registry=False,
    )
    assert category == "no_source_available"
    assert "source discovery" in next_action.lower()


def test_classify_uncovered_gmina_registry_gison_falls_back_to_candidate() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="2417042",
        in_registry=True,
    )
    assert category == "gison_raster_candidate"
    assert "raster source" in next_action.lower()


def test_classify_uncovered_gmina_default_no_source() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1299999",
        in_registry=False,
    )
    assert category == "no_source_available"
    assert "no confirmed planning source" in next_action.lower()
