from scraper.extractors.parcel import extract_obreb, extract_parcel_ids


def _primary_match(text: str):
    matches = extract_parcel_ids(text)
    assert matches, "expected at least one parcel match"
    return matches[0]


def test_extracts_locality_from_pol_w_phrase() -> None:
    text = "dzialka nr 1443/4 o pow. 0,8841 HA, pol. w Starym Wisniczu"

    match = _primary_match(text)

    assert match.numer == "1443/4"
    assert match.obreb_raw == "Starym Wisniczu"
    assert extract_obreb(text)[0] == "Starym Wisniczu"


def test_extracts_locality_from_postal_code_context() -> None:
    text = "dz. nr 492/2 32-125 Stregoborzyce Komornik Sadowy"

    match = _primary_match(text)

    assert match.numer == "492/2"
    assert match.obreb_raw == "Stregoborzyce"


def test_extracts_marker_locality_before_postal_code() -> None:
    text = "dzialka nr 1155/17 map_marker Kroczymiech, 32-500 Chrzanow"

    match = _primary_match(text)

    assert match.numer == "1155/17"
    assert match.obreb_raw == "Kroczymiech"


def test_prefers_postal_locality_when_map_marker_prefix_is_street_name() -> None:
    text = (
        "Licytacja nieruchomości prawo użytkowania wieczystego "
        "map_marker Ofiar Września, 41-400 Mysłowice Komornik Sądowy"
    )

    assert extract_obreb(text)[0] == "Mysłowice"


def test_prefers_postal_locality_over_street_name_in_map_marker() -> None:
    text = (
        "Licytacja nieruchomości "
        "map_marker Bartniczej, 41-407 Imielinie Komornik Sądowy"
    )

    assert extract_obreb(text)[0] == "Imielinie"


def test_repairs_ocr_split_locality_tokens() -> None:
    text = "Nieruchomosc gruntowa 32-620 Brze zcze Komornik Sadowy"

    assert extract_obreb(text)[0] == "Brzeszcze"


def test_extracts_plain_w_locality_after_property_phrase() -> None:
    text = "Nieruchomosc gruntowa w GRZEGORZOWICACH WIELKICH, objeta KW KR1S/00018865/7"

    assert extract_obreb(text)[0] == "Grzegorzowicach Wielkich"


def test_rejects_share_numbers_as_parcels() -> None:
    text = "udzial nr 4 map_marker Marianow 3, 32-043 Cianowice"

    assert extract_parcel_ids(text) == []


def test_rejects_property_count_as_parcel_number() -> None:
    text = (
        "Licytacja nieruchomości 3 działki gruntu o pow. 3053 m2 "
        "map_marker Tysiąclecia, 43-241 Łąka Komornik Sądowy"
    )

    assert extract_parcel_ids(text) == []
    assert extract_obreb(text)[0] == "Łąka"


def test_prefers_real_parcel_over_postal_code_noise() -> None:
    text = "Chocznia gm. Wadowice-dzialki o nr: 3827/1, 3829/1 map_marker 34-123 Chocznia"

    match = _primary_match(text)

    assert match.numer == "3827/1"
    assert match.obreb_raw == "Chocznia"


def test_cuts_locality_before_rejonie_tail() -> None:
    text = (
        "nieruchomość położona w Frelichowie w rejonie ulicy Polnej, "
        "składająca się z 2 działek ewidencyjnych nr 25/1, 26/3"
    )

    match = _primary_match(text)

    assert match.numer == "25/1"
    assert match.obreb_raw == "Frelichowie"
    assert extract_obreb(text)[0] == "Frelichowie"


def test_rejects_postal_code_prefix_as_parcel_number() -> None:
    text = "Adres nieruchomości 44-190 Knurów, poczta Knurów"

    assert extract_parcel_ids(text) == []


def test_rejects_keyword_match_that_only_captures_postal_prefix() -> None:
    text = (
        "udział 20/1320 w prawie własności nieruchomości gruntowej "
        "Adres nieruchomości 44-105 Gliwice, poczta Gliwice "
        "Dla nieruchomości prowadzona jest księga wieczysta GL1G/00023264/2"
    )

    assert extract_parcel_ids(text) == []


def test_cuts_locality_before_o_numerze_tail() -> None:
    text = "nieruchomość gruntowa położona w Knurowie o numerze KW: GL1G/00038126/1"

    assert extract_obreb(text)[0] == "Knurowie"


def test_extracts_obreb_from_ob_abbreviation() -> None:
    text = "działkę gruntu oznaczoną numerem ewidencyjnym 1890/18 (ob. 0006, Szopienice; KW: KA1K/00098017/7)"

    match = _primary_match(text)

    assert match.numer == "1890/18"
    assert match.obreb_raw == "Szopienice"
    assert extract_obreb(text)[0] == "Szopienice"


def test_ignores_obreb_number_and_keeps_real_parcel() -> None:
    text = (
        "Nieruchomość gruntowa położona w Zakopanem przy ul. H. Sienkiewicza, "
        "obręb nr 5, gm. Zakopane, składająca się z działek ewidencyjnych nr 344/1 i nr 344/2"
    )

    match = _primary_match(text)

    assert match.numer == "344/1"


def test_does_not_glue_locality_with_bezposrednio_noise() -> None:
    text = (
        "działka ewidencyjna nr 3740/2 zlokalizowana jest w miejscowości Lasek "
        "bezpośrednio przy torach kolejowych"
    )

    match = _primary_match(text)

    assert match.numer == "3740/2"
    assert extract_obreb(text)[0] == "Lasek"


def test_does_not_glue_locality_with_ocr_bezpsrednio_noise() -> None:
    text = (
        "działka ewidencyjna nr 3740/2 zlokalizowana jest w miejscowości Lasek "
        "bezpśrednio przy torach kolejowych"
    )

    match = _primary_match(text)

    assert match.numer == "3740/2"
    assert match.obreb_raw == "Lasek"


def test_extracts_address_locality_from_adres_nieruchomosci_block() -> None:
    text = (
        "udział 1/3 części w działce nr 528/1 o pow. 0,26 ha "
        "Adres nieruchomości 33-250 Kłyż, poczta Otfinów"
    )

    match = _primary_match(text)

    assert match.numer == "528/1"
    assert match.obreb_raw == "Kłyż"
    assert extract_obreb(text)[0] == "Kłyż"


def test_extracts_address_locality_when_street_precedes_postal_code() -> None:
    text = (
        "Nieruchomość stanowi działkę nr 418 o powierzchni 82137 m2. "
        "Adres nieruchomości Polna, 43-520 FRELICHÓW, poczta Chybie"
    )

    match = _primary_match(text)

    assert match.numer == "418"
    assert match.obreb_raw == "Frelichów"
    assert extract_obreb(text)[0] == "Frelichów"


def test_keeps_locality_when_address_has_only_street_and_no_parcel_number() -> None:
    text = (
        "map_marker Stodolska, 44-292 Zwonowice Komornik Sądowy "
        "Adres nieruchomości Stodolska, 44-292 Zwonowice, poczta Lyski"
    )

    assert extract_parcel_ids(text) == []
    assert extract_obreb(text)[0] == "Zwonowice"


def test_prefers_jedn_ewid_candidate_over_city_context() -> None:
    text = (
        "działki nr 256/11 oraz działki nr 247 o łącznej powierzchni 1ha22a89m2, "
        "położonych w rejonie ul. Geologów w Krakowie, jedn. ewid. Podgórze, obręb 94"
    )

    match = _primary_match(text)

    assert match.numer == "256/11"
    assert match.obreb_raw == "Podgórze"
    assert extract_obreb(text)[0] == "Podgórze"
