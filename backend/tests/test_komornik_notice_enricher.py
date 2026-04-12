import pytest

from app.services.komornik_notice_enricher import (
    KomornikNoticeEnricher,
    _parcel_numbers_from_text,
    extract_notice_id_from_source_url,
    normalize_kw_number,
)


def test_extract_notice_id_from_current_portal_url() -> None:
    url = (
        "https://licytacje.komornik.pl/wyszukiwarka/obwieszczenia-o-licytacji/"
        "32027/licytacja-nieruchomosci-prawo-wlasnosci-nieruchomosci-gruntowej"
    )
    assert extract_notice_id_from_source_url(url) == 32027


def test_normalize_kw_number_strips_spacing_noise() -> None:
    assert normalize_kw_number("gl1g / 00023264 / 2") == "GL1G/00023264/2"


def test_extracts_all_slash_parcels_from_notice_text() -> None:
    text = (
        "Obręb Żerniki, numery działek: 15/36, 15/45, 15/58, 15/59, 15/60, 15/62, 15/63, 15/64"
    )
    assert _parcel_numbers_from_text(text) == (
        "15/36",
        "15/45",
        "15/58",
        "15/59",
        "15/60",
        "15/62",
        "15/63",
        "15/64",
    )


@pytest.mark.asyncio
async def test_archived_kw_override_recovers_gliwice_notice_parcels() -> None:
    enricher = KomornikNoticeEnricher()
    hint = await enricher.fetch_notice_hint(
        source_url=(
            "https://licytacje.komornik.pl/wyszukiwarka/obwieszczenia-o-licytacji/"
            "32027/licytacja-nieruchomosci-prawo-wlasnosci-nieruchomosci-gruntowej"
        ),
        raw_kw="GL1G / 00023264 / 2",
    )

    assert hint is not None
    assert hint.obreb_name == "Żerniki"
    assert hint.parcel_numbers == (
        "15/36",
        "15/45",
        "15/58",
        "15/59",
        "15/60",
        "15/62",
        "15/63",
        "15/64",
    )
    assert hint.kw_number == "GL1G/00023264/2"
    assert hint.source.startswith("archived_kw_override:")
