from app.services.krakow_msip_resolver import (
    _derive_region_name,
    extract_contextual_parcel_numbers,
    extract_krakow_notice_context,
)


def test_extract_krakow_notice_context_from_official_notice_text() -> None:
    text = (
        "nieruchomość składa się z działek nr 256/11 i 247, "
        "położonych w jedn. ewid. Podgórze, obręb 94 w Krakowie"
    )
    assert extract_krakow_notice_context(text) == ("Podgórze", "0094")


def test_extract_krakow_notice_context_accepts_short_jedn_form() -> None:
    text = "działka nr 247, jedn. ewid. Nowa Huta obręb nr 11"
    assert extract_krakow_notice_context(text) == ("Nowa Huta", "0011")


def test_derive_region_name_for_known_krakow_units() -> None:
    assert _derive_region_name("Podgórze", "0094") == "P-94"
    assert _derive_region_name("Nowa Huta", "0011") == "NH-11"


def test_extract_contextual_parcel_numbers_prefers_notice_sentence() -> None:
    text = (
        "Działki nr 247 i 256/11, obr 94, jedn. ewid. Podgórze są objęte planem. "
        "Wcześniej w notice występuje też sygnatura 1/2."
    )
    assert extract_contextual_parcel_numbers(text) == ("247", "256/11")
