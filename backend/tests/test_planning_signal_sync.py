from app.services.planning_signal_sync import (
    HTML_INDEX_SIGNAL_REGISTRY,
    HtmlIndexSignalSource,
    _clean_html_fragment,
    _detect_source_type,
    _detect_html_index_error,
    _parse_generic_planning_page_signal,
    _parse_studium_page_signal,
    _resolve_relative_href,
)


def test_clean_html_fragment_strips_tags_and_entities() -> None:
    raw = "<a href='gml.php?plan=000'>SUiKZP</a>&nbsp;<b>Zakopane</b>"
    assert _clean_html_fragment(raw) == "SUiKZP Zakopane"


def test_resolve_relative_href_joins_against_base_url() -> None:
    assert _resolve_relative_href(
        "https://mzakopane.e-mapa.net/wykazplanow/",
        "<a href='gml.php?plan=000'>APP</a>",
    ) == "https://mzakopane.e-mapa.net/wykazplanow/gml.php?plan=000"


def test_detect_html_index_error_flags_operator_error_pages() -> None:
    assert _detect_html_index_error("Błąd połączenia em_") is not None


def test_detect_source_type_handles_pdf_urls() -> None:
    assert (
        _detect_source_type(
            "https://rastry.gison.pl/mpzp-public/zarki/uchwaly/U_2016_112_XVII_studium_tekst.pdf",
            "application/pdf",
        )
        == "pdf"
    )


def test_parse_generic_planning_page_signal_detects_plan_ogolny_article() -> None:
    page = (
        "<html><body><h1>Plan ogólny gminy</h1>"
        "<p>Obecnie trwa sporządzenie projektu planu.</p>"
        "<p>Dnia 27 czerwca 2024 r. Rada Miasta Zakopane podjęła uchwałę w sprawie "
        "przystąpienia do sporządzenia planu ogólnego miasta Zakopane.</p>"
        "</body></html>"
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="1217011",
        source_url="https://www.zakopane.pl/zagospodarowanie-przestrzenne/plan-ogolny-gminy/",
        label="Zakopane Plan ogólny gminy",
    )
    signal = _parse_generic_planning_page_signal(page, source)
    assert signal is not None
    assert signal.signal_kind == "planning_resolution"
    assert signal.signal_status == "formal_preparatory"
    assert signal.plan_name == "POG 1217011"
    assert signal.source_url == source.source_url
    assert signal.legal_weight > 0


def test_html_index_registry_includes_plan_ogolny_sources() -> None:
    urls = {source.source_url for source in HTML_INDEX_SIGNAL_REGISTRY}
    assert "https://www.zakopane.pl/zagospodarowanie-przestrzenne/plan-ogolny-gminy/" in urls
    assert (
        "https://gminaoswiecim.pl/pl/1960/24294/sporzadzenie-planu-ogolnego-gminy-oswiecim-ogloszenie-wojta-gminy-oswiecim.html"
        in urls
    )
    assert "https://nowywisnicz.pl/aktualnosci/planowanie-przestrzenne/" in urls
    assert "https://wadowice.pl/urzad/wydzialy/wydzial-planowania-przestrzennego/plan-ogolny-gminy-wadowice/" in urls
    assert "https://wadowice.pl/urzad/wydzialy/wydzial-planowania-przestrzennego/system-informacji-przestrzennej/" in urls
    assert "https://bip.kamienicapolska.pl/artykul/plan-ogolny" in urls
    assert "https://www.kruszyna.pl/plan-ogolny-gminy-kruszyna/" in urls
    assert "https://www.kozieglowy.pl/aktualnosci/4985" in urls
    assert "https://www.bip.wreczyca-wielka.akcessnet.net/index.php?a=0&id=587&idg=3&x=65&y=10" in urls
    assert "https://wreczyca-wielka.pl/aktualnosc-1167-ogloszenie.html" in urls
    assert "https://www.imielin.pl/pl/205/7551/plan-ogolny-miasta.html" in urls
    assert "https://szczekociny.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://bip.czerwionka-leszczyny.pl/informacje_urzedu/plan-ogolny-gminy-i-miasta-czerwionka-leszczyny-pog" in urls
    assert "https://bip.bojszowy.pl/pl/3144/0/plan-ogolny.html" in urls
    assert "https://bip.bojszowy.pl/pl/3145/26261/projekt-planu-ogolnego-etap-opiniowania-i-uzgadniania.html" in urls
    assert "https://bip.kamienicapolska.pl/artykul/studium-uwarunkowan" in urls
    assert "https://bip.kamienicapolska.pl/artykul/opiniowanie-z-gminna-komisja-urbanistyczno-architektoniczna" in urls
    assert "https://www.zarki.bip.jur.pl/artykuly/6686" in urls
    assert "https://www.zarki.bip.jur.pl/kategorie/projekt_zmiany_studium_uwarunkowan_i_kierunkow" in urls
    assert "https://rastry.gison.pl/mpzp-public/zarki/uchwaly/U_2016_112_XVII_studium_tekst.pdf" in urls


def test_parse_generic_planning_page_signal_detects_wnioski_do_planu_ogolnego() -> None:
    page = (
        "<html><body><article>"
        "<h1>WAŻNE !!! Jeszcze do piątku można składać wnioski do planu ogólnego Gminy Nowy Wiśnicz</h1>"
        "<p>Burmistrz Nowego Wiśnicza przypomina, że do dnia 12.07.2024r. można składać wnioski do planu ogólnego.</p>"
        "</article></body></html>"
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="1201065",
        source_url="https://nowywisnicz.pl/aktualnosci/planowanie-przestrzenne/",
        label="Nowy Wiśnicz Planowanie przestrzenne",
    )
    signal = _parse_generic_planning_page_signal(page, source)
    assert signal is not None
    assert signal.signal_kind == "planning_resolution"
    assert signal.signal_status == "formal_preparatory"
    assert signal.plan_name == "POG 1201065"
    assert signal.source_url == source.source_url
    assert signal.legal_weight > 0


def test_parse_studium_page_signal_detects_current_studium_page() -> None:
    page = (
        "<html><body><section>"
        "<h1>Obowiązujące studium uwarunkowań i kierunków zagospodarowania przestrzennego gminy Wadowice</h1>"
        "<p>System Informacji Przestrzennej obejmuje m.in. miejscowe plany zagospodarowania przestrzennego i studium.</p>"
        "</section></body></html>"
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="1218095",
        source_url="https://wadowice.pl/urzad/wydzialy/wydzial-planowania-przestrzennego/system-informacji-przestrzennej/",
        label="Wadowice Studium uwarunkowań",
    )
    signal = _parse_studium_page_signal(page, source)
    assert signal is not None
    assert signal.signal_kind == "planning_resolution"
    assert signal.signal_status == "formal_directional"
    assert signal.plan_name == "SUiKZP 1218095"
    assert signal.designation_raw == "SUiKZP"
    assert signal.source_url == source.source_url
    assert signal.legal_weight > 0


def test_parse_generic_planning_page_signal_detects_bip_preparatory_phrases() -> None:
    page = (
        "<html><body><article>"
        "<h1>Plan ogólny gminy Kamienica Polska</h1>"
        "<p>Etap opiniowania i uzgadniania. Opiniowanie z Gminną Komisją Urbanistyczno-Architektoniczną.</p>"
        "</article></body></html>"
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="2404042",
        source_url="https://bip.kamienicapolska.pl/artykul/plan-ogolny",
        label="Kamienica Polska Plan ogólny - BIP",
    )
    signal = _parse_generic_planning_page_signal(page, source)
    assert signal is not None
    assert signal.signal_status == "formal_preparatory"
    assert signal.source_url == source.source_url
    assert signal.legal_weight > 0


def test_parse_generic_planning_page_signal_classifies_mpzp_project() -> None:
    page = (
        "<html><body><article>"
        "<h1>Przystąpienie do sporządzenia miejscowego planu zagospodarowania przestrzennego</h1>"
        "<p>Trwa sporządzenie projektu miejscowego planu zagospodarowania przestrzennego dla wybranego obszaru.</p>"
        "</article></body></html>"
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="2414042",
        source_url="https://example.org/mpzp-project",
        label="Bojszowy MPZP project",
    )
    signal = _parse_generic_planning_page_signal(page, source)
    assert signal is not None
    assert signal.signal_kind == "mpzp_project"
    assert signal.signal_status == "formal_preparatory"
    assert signal.designation_raw == "MPZP"
    assert signal.plan_name == "MPZP 2414042"
    assert signal.legal_weight > 0


def test_parse_generic_planning_page_signal_classifies_studium_change_context() -> None:
    page = (
        "<html><body><article>"
        "<h1>Zmiana studium uwarunkowań i kierunków zagospodarowania przestrzennego gminy</h1>"
        "<p>Trwa sporządzenie projektu dokumentu oraz etap uzgadniania.</p>"
        "</article></body></html>"
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="1218095",
        source_url="https://example.org/studium-change",
        label="Wadowice Studium change",
    )
    signal = _parse_generic_planning_page_signal(page, source)
    assert signal is not None
    assert signal.signal_kind == "planning_resolution"
    assert signal.designation_raw == "SUiKZP"
    assert signal.plan_name == "SUiKZP 1218095"
    assert signal.signal_status == "formal_preparatory"


def test_parse_generic_planning_page_signal_detects_zbieranie_uwag_and_rejestr_phrase() -> None:
    page = (
        "<html><body><article>"
        "<h1>Plan ogólny Miasta Imielin</h1>"
        "<p>Zakończyliśmy etap zbierania uwag do Planu Ogólnego.</p>"
        "<p>Informacje publikujemy w rejestrze planów ogólnych.</p>"
        "</article></body></html>"
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="2414021",
        source_url="https://www.imielin.pl/pl/205/7551/plan-ogolny-miasta.html",
        label="Imielin Plan Ogólny Miasta - Aktualności",
    )
    signal = _parse_generic_planning_page_signal(page, source)
    assert signal is not None
    assert signal.signal_status == "formal_preparatory"
    assert signal.source_url == source.source_url
    assert signal.legal_weight > 0
