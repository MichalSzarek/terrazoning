from decimal import Decimal

from shapely.geometry import MultiPolygon, Polygon

from app.services.geo_resolver import (
    _city_commune_codes_in_text,
    _filter_parcels_by_area_hint,
    _infer_commune_code_for_listing,
)
from app.services.uldk import ULDKParcel


def _sample_parcel(area_m2: str) -> ULDKParcel:
    geom = MultiPolygon([Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])])
    return ULDKParcel(
        identifier=f"1261011.0001.{area_m2}",
        voivodeship="12",
        county="1261",
        commune="1261011",
        region="0001",
        parcel="1/1",
        teryt_wojewodztwo="12",
        teryt_powiat="1261",
        teryt_gmina="1261011",
        teryt_obreb="126101101",
        numer_dzialki="1/1",
        identyfikator=f"126101101.1/1.{area_m2}",
        geom_shape=geom,
        geom_wkb_hex="00",
        area_m2=Decimal(area_m2),
    )


def test_infer_commune_code_from_text_for_krakow_precinct_case() -> None:
    raw_text = (
        "Licytacja nieruchomości działka nr 256/11 oraz nr 247 położone w Krakowie "
        "map_marker Geologów, 30-698 Kraków Komornik Sądowy przy Sądzie Rejonowym "
        "dla Krakowa - Podgórza w Krakowie"
    )
    assert _infer_commune_code_for_listing(
        raw_gmina=None,
        obreb_name="Podgórze",
        raw_text=raw_text,
        title="Licytacja nieruchomości działka nr 256/11 oraz nr 247 położone w Krakowie",
        raw_woj="małopolskie",
    ) == "1261011"


def test_infer_commune_code_keeps_direct_city_mapping() -> None:
    assert _infer_commune_code_for_listing(
        raw_gmina=None,
        obreb_name="Nowym Sączu",
        raw_text="udział w nieruchomości położonej w obrębie 32 w Nowym Sączu",
        title=None,
        raw_woj="małopolskie",
    ) == "1262011"


def test_infer_commune_code_uses_existing_city_fallback_without_region() -> None:
    assert _infer_commune_code_for_listing(
        raw_gmina=None,
        obreb_name="Knurów",
        raw_text="nieruchomość gruntowa położona w Knurowie",
        title=None,
        raw_woj="śląskie",
    ) == "2405011"


def test_city_mentions_prefer_province_compatible_match() -> None:
    text = "nieruchomość położona w Krakowie i wzmianka o Gliwicach w kancelarii"
    assert _city_commune_codes_in_text(text, "małopolskie") == ["1261011"]


def test_filter_parcels_by_area_hint_uses_plus_minus_five_percent() -> None:
    parcels = [
        _sample_parcel("980.00"),
        _sample_parcel("1030.00"),
        _sample_parcel("1200.00"),
    ]

    matched = _filter_parcels_by_area_hint(parcels, Decimal("1000.00"))

    assert [parcel.area_m2 for parcel in matched] == [Decimal("980.00"), Decimal("1030.00")]
