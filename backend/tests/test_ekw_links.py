from app.services.ekw_links import EKW_SEARCH_BASE_URL, build_ekw_search_url


def test_build_ekw_search_url_for_canonical_kw_returns_official_entrypoint() -> None:
    url = build_ekw_search_url("KR1B/00079684/3")

    assert url == EKW_SEARCH_BASE_URL


def test_build_ekw_search_url_returns_none_for_missing_or_invalid_kw() -> None:
    assert build_ekw_search_url(None) is None
    assert build_ekw_search_url("") is None
    assert build_ekw_search_url("KR1B/79684/3") is None
    assert build_ekw_search_url("not-a-kw") is None
