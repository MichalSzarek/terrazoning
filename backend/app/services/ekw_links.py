"""Helpers for building best-effort links to the official EKW search page."""

from __future__ import annotations

from re import Pattern, compile
from typing import Final
from urllib.parse import urlencode

EKW_SEARCH_BASE_URL: Final[str] = (
    "https://przegladarka-ekw.ms.gov.pl/eukw_prz/KsiegiWieczyste/wyszukiwanieKW"
)

_KW_PATTERN: Final[Pattern[str]] = compile(
    r"^(?P<court_code>[A-Z]{2}\d[A-Z])/(?P<book_number>\d{8})/(?P<check_digit>\d)$"
)


def build_ekw_search_url(kw_number: str | None) -> str | None:
    """Return a best-effort EKW search URL for a canonical KW number."""

    if not kw_number:
        return None

    match = _KW_PATTERN.fullmatch(kw_number.strip())
    if match is None:
        return None

    params = {
        "komunikaty": "true",
        "kontakt": "true",
        "okienkoSerwisowe": "false",
        "kodEci": match.group("court_code"),
        "kodWydzialuInput": match.group("court_code"),
        "numerKW": match.group("book_number"),
        "cyfraKontrolna": match.group("check_digit"),
    }
    return f"{EKW_SEARCH_BASE_URL}?{urlencode(params)}"
