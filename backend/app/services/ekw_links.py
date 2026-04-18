"""Helpers for linking to the official EKW search flow."""

from __future__ import annotations

from re import Pattern, compile
from typing import Final

EKW_SEARCH_BASE_URL: Final[str] = (
    "https://ekw.ms.gov.pl/eukw_ogol/KsiegiWieczyste"
)

_KW_PATTERN: Final[Pattern[str]] = compile(
    r"^[A-Z]{2}\d[A-Z]/\d{8}/\d$"
)


def build_ekw_search_url(kw_number: str | None) -> str | None:
    """Return the official EKW search entry URL for a canonical KW number.

    The public EKW portal currently does not expose a stable deep link that
    pre-fills the search form, so TerraZoning opens the official search entry
    point and keeps the canonical KW number ready for copy/paste in the UI.
    """

    if not kw_number:
        return None

    if _KW_PATTERN.fullmatch(kw_number.strip()) is None:
        return None

    return EKW_SEARCH_BASE_URL
