from scraper.extractors.llm_extractor import (
    LLMParcelExtraction,
    _schema_for_gemini,
    extract_with_fallback,
    llm_to_parcel_match,
    should_use_llm_fallback,
)
from scraper.extractors.parcel import ParcelMatch


class _StubLLMExtractor:
    model = "gemini-2.5-flash"

    def __init__(self, extraction: LLMParcelExtraction | None) -> None:
        self._extraction = extraction

    async def extract(self, *_args, **_kwargs) -> LLMParcelExtraction | None:
        return self._extraction


def test_should_use_llm_fallback_when_regex_missing() -> None:
    assert should_use_llm_fallback(None, None) == "missing_parcel"


def test_should_use_llm_fallback_for_low_confidence_regex() -> None:
    parcel = ParcelMatch(
        raw_value="12/4",
        numer="12/4",
        obreb_raw=None,
        teryt_obreb=None,
        confidence=0.55,
        snippet="12/4",
        char_offset=0,
    )

    assert should_use_llm_fallback(parcel, None) == "low_regex_confidence"


def test_should_use_llm_fallback_for_missing_locality() -> None:
    parcel = ParcelMatch(
        raw_value="działka nr 344/1",
        numer="344/1",
        obreb_raw=None,
        teryt_obreb=None,
        confidence=0.80,
        snippet="działka nr 344/1",
        char_offset=0,
    )

    assert should_use_llm_fallback(parcel, None) == "missing_locality"


def test_schema_for_gemini_strips_nullable_anyof() -> None:
    schema = _schema_for_gemini()

    assert schema["type"] == "object"
    assert "anyOf" not in schema["properties"]["kw_number"]
    assert schema["properties"]["kw_number"]["type"] == "string"
    assert schema["properties"]["area_text"]["type"] == "string"


def test_llm_to_parcel_match_rejects_hallucinated_parcel() -> None:
    extraction = LLMParcelExtraction(
        parcel_number="999/9",
        precinct_or_city="Kozia Wólka",
        municipality="Pcim",
    )

    assert llm_to_parcel_match(extraction, "nieruchomość w Koziej Wólce") is None


async def test_extract_with_fallback_uses_stubbed_llm_for_low_confidence_text() -> None:
    raw_text = "Przedmiot licytacji: 12/4 poł. w msc. Kozia Wólka, gm. Pcim."
    stub = _StubLLMExtractor(
        LLMParcelExtraction(
            parcel_number="12/4",
            precinct_or_city="Kozia Wólka",
            municipality="Pcim",
            area_text="0,18 ha",
        )
    )

    result = await extract_with_fallback(
        raw_text,
        llm_extractor=stub,
    )

    assert result.llm_used is True
    assert result.primary_parcel is not None
    assert result.primary_parcel.numer == "12/4"
    assert result.obreb_name == "Kozia Wólka"
    assert result.municipality == "Pcim"
    assert result.area_text == "0,18 ha"
    assert result.llm_extraction is not None
    assert result.llm_extraction["accepted"] is True


async def test_extract_with_fallback_prefers_address_locality_over_nearest_city() -> None:
    raw_text = (
        "Licytacja nieruchomości udział 1/3 części w działce nr 528/1 o pow. 0,26 ha "
        "map_marker 33-250 Kłyż Komornik Sądowy przy Sądzie Rejonowym w Dąbrowie Tarnowskiej "
        "Adres nieruchomości 33-250 Kłyż, poczta Otfinów"
    )

    result = await extract_with_fallback(raw_text, llm_extractor=None)

    assert result.llm_used is False
    assert result.primary_parcel is not None
    assert result.primary_parcel.numer == "528/1"
    assert result.obreb_name == "Kłyż"


async def test_extract_with_fallback_prefers_ewid_unit_over_city_context() -> None:
    raw_text = (
        "działka nr 256/11 oraz nr 247 położone w Krakowie "
        "jedn. ewid. Podgórze, obręb 94"
    )

    result = await extract_with_fallback(raw_text, llm_extractor=None)

    assert result.llm_used is False
    assert result.primary_parcel is not None
    assert result.primary_parcel.numer == "256/11"
    assert result.obreb_name == "Podgórze"
