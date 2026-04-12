from app.services.wms_grid_ingestor import (
    _parse_semicolon_feature_info,
    parse_gison_portal_html_metadata,
    parse_gison_portal_html_feature_info,
    parse_ruda_plaintext_feature_info,
)


def test_parse_semicolon_feature_info_extracts_layer_and_payload() -> None:
    body = (
        "@MPZP - przeznaczenia terenów "
        "OBJECTID;numerplanu;symbolzrastra;oznaczenieterenuzrastra;statusplanu; "
        "367273;RS_85;MNU;16MNU;obowiązujący;"
    )
    parsed = _parse_semicolon_feature_info(body)

    assert parsed is not None
    layer_name, payload = parsed
    assert layer_name == "MPZP - przeznaczenia terenów"
    assert payload["numerplanu"] == "RS_85"
    assert payload["symbolzrastra"] == "MNU"
    assert payload["oznaczenieterenuzrastra"] == "16MNU"


def test_parse_ruda_plaintext_feature_info_maps_fields() -> None:
    body = (
        "@MPZP - przeznaczenia terenów "
        "OBJECTID;numerplanu;symbolzrastra;numerterenuzrastra;oznaczenieterenuzrastra;"
        "kategoriaprzeznaczeniaterenu;numeruchwałyouchwaleniuplanu;statusplanu;wersjaplanu; "
        "361403;RS_84;U;13;13U;tereny zabudowy usługowej;PR.0007.59.2018;obowiązujący;RS_84.01;"
    )

    hit = parse_ruda_plaintext_feature_info(body)

    assert hit is not None
    assert hit.designation == "U"
    assert hit.plan_name == "RS_84"
    assert hit.description == "tereny zabudowy usługowej"
    assert hit.uchwala_nr == "PR.0007.59.2018"


def test_parse_gison_portal_html_feature_info_extracts_designation_and_links() -> None:
    body = (
        "<br/><u><b>MPZP</b></u><br/>"
        "<b>Nazwa:</b> <strong>Miejscowy plan zagospodarowania przestrzennego gminy Gołcza</strong>"
        "<br/><b>Uchwała:</b> <a href='https://example.test/uchwala.pdf' target='_blank'>"
        "<strong>MPZP Nr: V/24/2015</strong></a>"
        "<br/><b>Legenda:</b> <a href='https://example.test/legenda.jpg' target='_blank'>"
        "<strong>link</strong></a>"
        "<br/><b>Przeznaczenie:</b><br/>&nbsp<b>G-2R2</b> - teren rolny"
    )

    hit = parse_gison_portal_html_feature_info(body)

    assert hit is not None
    assert hit.designation == "G-2R2"
    assert hit.description == "teren rolny"
    assert hit.plan_name == "Miejscowy plan zagospodarowania przestrzennego gminy Gołcza"
    assert hit.uchwala_nr == "V/24/2015"
    assert hit.raw_payload["legend_url"] == "https://example.test/legenda.jpg"


def test_parse_gison_portal_html_feature_info_returns_none_for_blank_payload() -> None:
    assert parse_gison_portal_html_feature_info("") is None


def test_parse_gison_portal_html_feature_info_handles_preceding_bullet_lines() -> None:
    body = (
        "<br/><u><b>MPZP</b></u><br/>"
        "<b>Nazwa:</b> <strong>Miejscowy plan zagospodarowania przestrzennego miasta Knurów</strong>"
        "<br/><b>Uchwała:</b> <a href='https://example.test/uchwala.pdf' target='_blank'>"
        "<strong>MPZP Nr: LXIII/770/2022</strong></a>"
        "<br/><b>Przeznaczenie:</b><br/>&nbsp- teren i obszar górniczy"
        "<br/>&nbsp<b> Z27.1KS</b> - tereny obsługi komunikacji"
        "<br/><br/><b>Dokument zmieniający:</b>"
    )

    hit = parse_gison_portal_html_feature_info(body)

    assert hit is not None
    assert hit.designation == "Z27.1KS"
    assert hit.description == "tereny obsługi komunikacji"
    assert hit.plan_name == "Miejscowy plan zagospodarowania przestrzennego miasta Knurów"
    assert hit.uchwala_nr == "LXIII/770/2022"


def test_parse_gison_portal_html_metadata_extracts_plan_without_designation() -> None:
    body = (
        "<br/><u><b>MPZP</b></u><br/>"
        "<b>Nazwa:</b> <strong>Miejscowy Plan Zagospodarowania Przestrzennego Gminy i Miasta Czerwionka - Leszczyny</strong>"
        "<br/><b>Uchwała:</b> <a href='https://example.test/uchwala.pdf' target='_blank'>"
        "<strong>MPZP Nr: IX/78/2002</strong></a>"
        "<br/><b>Legenda:</b> <a href='https://example.test/legenda.png' target='_blank'>"
        "<strong>link</strong></a>"
    )

    metadata = parse_gison_portal_html_metadata(body)

    assert metadata is not None
    assert metadata["plan_name"] == "Miejscowy Plan Zagospodarowania Przestrzennego Gminy i Miasta Czerwionka - Leszczyny"
    assert metadata["uchwala_nr"] == "IX/78/2002"
    assert metadata["legend_url"] == "https://example.test/legenda.png"
