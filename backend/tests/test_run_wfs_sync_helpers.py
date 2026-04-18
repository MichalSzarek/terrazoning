from run_wfs_sync import WFS_REGISTRY, _classify_uncovered_gmina


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
    assert category == "manual_backlog"
    assert "wms service" in next_action.lower()


def test_classify_uncovered_gmina_chrzanow_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1203034",
        in_registry=False,
    )
    assert category == "manual_backlog"
    assert "chrzanów" in next_action.lower()


def test_classify_uncovered_gmina_iwanowice_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1206032",
        in_registry=False,
    )
    assert category == "manual_backlog"
    assert "iwanowice" in next_action.lower()


def test_classify_uncovered_gmina_ksiaz_wielki_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1208045",
        in_registry=False,
    )
    assert category == "manual_backlog"
    assert "książ wielki" in next_action.lower()


def test_classify_uncovered_gmina_sekowa_has_no_parcel_match() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1205092",
        in_registry=False,
    )
    assert category == "source_discovered_no_parcel_match"
    assert "ropica górna" in next_action.lower()


def test_classify_uncovered_gmina_brzesko_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1202024",
        in_registry=False,
    )
    assert category == "manual_backlog"
    assert "brzesko" in next_action.lower()


def test_classify_uncovered_gmina_nowy_targ_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1211092",
        in_registry=False,
    )
    assert category == "manual_backlog"
    assert "nowy targ" in next_action.lower()


def test_classify_uncovered_gmina_korzenna_has_no_parcel_match() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1210062",
        in_registry=False,
    )
    assert category == "source_discovered_no_parcel_match"
    assert "janczowa" in next_action.lower()


def test_classify_uncovered_gmina_dukla_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1807025",
        in_registry=False,
    )
    assert category == "manual_backlog"
    assert "login-gated" in next_action.lower()


def test_classify_uncovered_gmina_boguchwala_registry_requires_manual_probe() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1816035",
        in_registry=True,
    )
    assert category == "manual_backlog"
    assert "source is configured" in next_action.lower()


def test_classify_uncovered_gmina_registry_gison_falls_back_to_candidate() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="2417042",
        in_registry=True,
    )
    assert category == "gison_raster_candidate"
    assert "raster source" in next_action.lower()


def test_classify_uncovered_gmina_hyzne_has_gison_candidate() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1816072",
        in_registry=False,
    )
    assert category == "gison_raster_candidate"
    assert "manual legend override" in next_action.lower()


def test_classify_uncovered_gmina_tyczyn_has_gison_candidate() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1816145",
        in_registry=False,
    )
    assert category == "gison_raster_candidate"
    assert "plan 038" in next_action.lower()


def test_classify_uncovered_gmina_narol_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1809054",
        in_registry=False,
    )
    assert category == "manual_backlog"
    assert "formal binding plan metadata" in next_action.lower()


def test_classify_uncovered_gmina_tarnowiec_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1805112",
        in_registry=False,
    )
    assert category == "manual_backlog"
    assert "403" in next_action.lower()


def test_classify_uncovered_gmina_czermin_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1811032",
        in_registry=False,
    )
    assert category == "manual_backlog"
    assert "403" in next_action.lower()


def test_classify_uncovered_gmina_pilzno_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1803065",
        in_registry=False,
    )
    assert category == "manual_backlog"
    assert "403" in next_action.lower()


def test_classify_uncovered_gmina_debica_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1803042",
        in_registry=False,
    )
    assert category == "upstream_blocker"
    assert "błąd połączenia em_" in next_action.lower()


def test_classify_uncovered_gmina_jodlowa_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1803052",
        in_registry=False,
    )
    assert category == "upstream_blocker"
    assert "błąd połączenia em_" in next_action.lower()


def test_classify_uncovered_gmina_glogow_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1816065",
        in_registry=False,
    )
    assert category == "manual_backlog"
    assert "głogów" in next_action.lower()


def test_classify_uncovered_gmina_iwierzyce_has_discovered_sources_without_match() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1815012",
        in_registry=False,
    )
    assert category == "source_discovered_no_parcel_match"
    assert "do not intersect any discovered plan bbox" in next_action.lower()


def test_classify_uncovered_gmina_niebylec_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1819032",
        in_registry=False,
    )
    assert category == "manual_backlog"
    assert "403" in next_action.lower()


def test_classify_uncovered_gmina_strzyzow_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1819045",
        in_registry=False,
    )
    assert category == "manual_backlog"
    assert "403" in next_action.lower()


def test_classify_uncovered_gmina_grebow_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1820032",
        in_registry=False,
    )
    assert category == "manual_backlog"
    assert "403" in next_action.lower()


def test_classify_uncovered_gmina_szklary_mismatch_has_manual_backlog() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1807102",
        in_registry=False,
    )
    assert category == "manual_backlog"
    assert "non-matching teryt" in next_action.lower()


def test_classify_uncovered_gmina_sokolow_malopolski_has_discovered_sources_without_match() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1816115",
        in_registry=False,
    )
    assert category == "source_discovered_no_parcel_match"
    assert "do not intersect any discovered plan bbox" in next_action.lower()


def test_classify_uncovered_gmina_default_no_source() -> None:
    category, next_action = _classify_uncovered_gmina(
        teryt="1299999",
        in_registry=False,
    )
    assert category == "no_source_available"
    assert "no confirmed planning source" in next_action.lower()


def test_wfs_registry_contains_verified_podkarpackie_sources() -> None:
    for teryt in (
        "1810042",
        "1821035",
        "1810011",
        "1816072",
        "1816145",
        "1804082",
        "1805042",
        "1805112",
        "1807102",
        "1808042",
        "1809054",
        "1815012",
        "1816115",
        "1819032",
        "1819045",
        "1820032",
        "1206022",
        "1206032",
        "1206162",
        "1208045",
        "1205062",
        "1206115",
        "1210142",
        "1212042",
        "1216092",
        "1216145",
        "1210062",
    ):
        assert teryt in WFS_REGISTRY


def test_wfs_registry_tyczyn_uses_conservative_app_fallback() -> None:
    entry = WFS_REGISTRY["1816145"]

    assert entry.source_kind == "wms_grid"
    assert entry.query_url_template == "https://tyczyn.e-mapa.net/wykazplanow/view_gml.php?plan=038"
    assert entry.fallback_designation == "ZL"
    assert "forests and afforestation" in (entry.fallback_description or "").lower()


def test_wfs_registry_gmina_jaslo_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1805042"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "jaslogmina" in entry.wfs_url.lower()


def test_wfs_registry_giedlarowa_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1808042"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is True
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "giedlarowa" in entry.wfs_url.lower()


def test_wfs_registry_radymno_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1804082"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "radymnogmina" in entry.wfs_url.lower()


def test_wfs_registry_tarnowiec_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1805112"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "tarnowiec" in entry.wfs_url.lower()


def test_wfs_registry_jasliska_uses_vector_igeomap_extent() -> None:
    entry = WFS_REGISTRY["1807102"]

    assert entry.source_kind == "wfs"
    assert entry.plan_type == "mpzp"
    assert entry.wfs_version == "2.0.0"
    assert entry.layer_name == "ms:zasiegi"
    assert entry.wfs_url.endswith("/180710")


def test_wfs_registry_narol_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1809054"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "narol" in entry.wfs_url.lower()


def test_wfs_registry_iwierzyce_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1815012"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is True
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "iwierzyce" in entry.wfs_url.lower()
    assert "wiercany" in (entry.fallback_description or "").lower()


def test_wfs_registry_niebylec_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1819032"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "niebylec" in entry.wfs_url.lower()


def test_wfs_registry_strzyzow_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1819045"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "strzyzow" in entry.wfs_url.lower()


def test_wfs_registry_grebow_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1820032"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "grebow" in entry.wfs_url.lower()


def test_wfs_registry_sokolow_malopolski_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1816115"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is True
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "sokolowmalopolski" in entry.wfs_url.lower()
    assert "wólka niedźwiedzka" in (entry.fallback_description or "").lower()


def test_wfs_registry_igolomia_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1206022"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "igolomiawawrzenczyce" in entry.wfs_url.lower()


def test_wfs_registry_iwanowice_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1206032"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "iwanowice" in entry.wfs_url.lower()


def test_wfs_registry_zabierzow_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1206162"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "zabierzow" in entry.wfs_url.lower()


def test_wfs_registry_ksiaz_wielki_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1208045"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "ksiazwielki" in entry.wfs_url.lower()


def test_wfs_registry_luzna_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1205062"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "luzna" in entry.wfs_url.lower()


def test_wfs_registry_skawina_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1206115"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "skawina" in entry.wfs_url.lower()


def test_wfs_registry_podegrodzie_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1210142"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "podegrodzie" in entry.wfs_url.lower()


def test_wfs_registry_klucze_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1212042"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "klucze" in entry.wfs_url.lower()


def test_wfs_registry_gmina_tarnow_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1216092"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "tarnowgmina" in entry.wfs_url.lower()


def test_wfs_registry_zakliczyn_uses_app_gml_project_extent() -> None:
    entry = WFS_REGISTRY["1216145"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "zakliczyn" in entry.wfs_url.lower()


def test_wfs_registry_korzenna_uses_app_gml_drawing_extent() -> None:
    entry = WFS_REGISTRY["1210062"]

    assert entry.source_kind == "app_gml"
    assert entry.plan_type == "mpzp"
    assert entry.swap_xy is False
    assert entry.fallback_designation == "MPZP_PROJ"
    assert "korzenna" in entry.wfs_url.lower()
    assert "rysunkiaktuplanowania.mpzp" in entry.wfs_url.lower()
