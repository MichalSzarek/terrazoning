from urllib.parse import parse_qs, urlparse

from app.services.ekw_links import EKW_SEARCH_BASE_URL, build_ekw_search_url


def test_build_ekw_search_url_for_canonical_kw() -> None:
    url = build_ekw_search_url("KR1B/00079684/3")

    assert url is not None

    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    assert f"{parsed.scheme}://{parsed.netloc}{parsed.path}" == EKW_SEARCH_BASE_URL
    assert query["komunikaty"] == ["true"]
    assert query["kontakt"] == ["true"]
    assert query["okienkoSerwisowe"] == ["false"]
    assert query["kodEci"] == ["KR1B"]
    assert query["kodWydzialuInput"] == ["KR1B"]
    assert query["numerKW"] == ["00079684"]
    assert query["cyfraKontrolna"] == ["3"]


def test_build_ekw_search_url_returns_none_for_missing_or_invalid_kw() -> None:
    assert build_ekw_search_url(None) is None
    assert build_ekw_search_url("") is None
    assert build_ekw_search_url("KR1B/79684/3") is None
    assert build_ekw_search_url("not-a-kw") is None
