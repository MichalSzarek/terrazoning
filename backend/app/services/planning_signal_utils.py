"""Shared normalization and scoring helpers for future-buildability signals."""

from __future__ import annotations

import re
from decimal import Decimal

from app.services.delta_engine import is_buildable_symbol, normalize_symbol

POSITIVE_DESIGNATIONS = frozenset({"residential", "mixed_residential", "service"})
HARD_NEGATIVE_DESIGNATIONS = frozenset(
    {"forest", "water", "green", "cemetery", "infrastructure", "industrial"}
)
_RESIDENTIAL_TOKENS = frozenset({"MN", "MW", "ML", "MR"})
_SERVICE_TOKENS = frozenset({"U", "UC", "UK", "UT", "UH", "US", "UI", "UA", "UN", "UZ", "CU", "UP"})
_MIXED_TOKENS = frozenset({"MU", "UM", "MNU", "MN/U", "U/MN", "U/MW", "MW/U"})
_PLANNING_SYMBOLS = tuple(sorted(_RESIDENTIAL_TOKENS | _SERVICE_TOKENS | _MIXED_TOKENS, key=len, reverse=True))


def _extract_description_symbols(description: str) -> set[str]:
    """Extract planning symbols embedded in prose snippets like `Z14.1UP-UC`."""
    desc_upper = description.upper()
    matches: set[str] = set()
    for token in _PLANNING_SYMBOLS:
        pattern = re.compile(rf"(?<![A-ZĄĆĘŁŃÓŚŹŻ]){re.escape(token)}(?![A-ZĄĆĘŁŃÓŚŹŻ])")
        if pattern.search(desc_upper):
            matches.add(token)
    return matches


def normalize_designation_class(
    designation_raw: str | None,
    description: str | None = None,
) -> str:
    """Map local planning labels to a stable future-buildability taxonomy."""
    raw = (designation_raw or "").strip().upper()
    desc = (description or "").strip().lower()
    normalized_symbol = normalize_symbol(raw)
    tokens = {
        token
        for token in re.split(r"[/,;\\s]+", normalized_symbol.replace("-", "/"))
        if token
    }
    desc_symbols = _extract_description_symbols(desc)
    tokens |= {
        token
        for symbol in desc_symbols
        for token in re.split(r"[/,;\\s]+", symbol.replace("-", "/"))
        if token
    }
    document_designation = normalized_symbol in {"POG", "SUIKZP", "STUDIUM", "MPZP"}

    if is_buildable_symbol(normalized_symbol):
        if "/" in normalized_symbol or normalized_symbol in {"MU", "UM", "MNU"}:
            return "mixed_residential"
        if normalized_symbol.startswith("U"):
            return "service"
        return "residential"

    if document_designation and not desc_symbols and "mieszk" not in desc and "usług" not in desc:
        return "unknown"

    if normalized_symbol in _MIXED_TOKENS or ("mieszk" in desc and "usług" in desc):
        return "mixed_residential"
    if (tokens & _RESIDENTIAL_TOKENS) and (tokens & _SERVICE_TOKENS):
        return "mixed_residential"
    if tokens & _RESIDENTIAL_TOKENS or "mieszk" in desc:
        return "residential"
    if tokens & _SERVICE_TOKENS or "usług" in desc:
        return "service"
    if normalized_symbol.startswith("KD") or "droga" in desc:
        return "road"
    if normalized_symbol.startswith("ZL") or "las" in desc:
        return "forest"
    if normalized_symbol.startswith("WS") or "wod" in desc:
        return "water"
    if normalized_symbol.startswith("ZP") or "ziele" in desc:
        return "green"
    if normalized_symbol.startswith("P") or "produk" in desc or "przemys" in desc:
        return "industrial"
    if normalized_symbol.startswith("R") or "rol" in desc or "sad" in desc:
        return "agricultural"
    if normalized_symbol.startswith("C") or "cmentar" in desc:
        return "cemetery"
    if normalized_symbol.startswith("IT") or "infrastr" in desc:
        return "infrastructure"
    return "unknown"


def score_signal(
    *,
    signal_kind: str,
    designation_normalized: str | None,
    signal_status: str,
) -> Decimal:
    """Return the score contribution of a single planning signal."""
    designation = (designation_normalized or "unknown").strip().lower()

    if designation in HARD_NEGATIVE_DESIGNATIONS:
        return Decimal("-40.00")
    if designation == "agricultural" and signal_status != "heuristic":
        return Decimal("-20.00")

    if signal_kind in {"mpzp_project", "planning_resolution"} and signal_status != "heuristic":
        if designation in POSITIVE_DESIGNATIONS:
            return Decimal("10.00")
        return Decimal("0.00")

    if designation not in POSITIVE_DESIGNATIONS:
        return Decimal("0.00")

    if signal_kind == "pog_zone":
        return Decimal("55.00")
    if signal_kind == "pog_ouz":
        return Decimal("30.00")
    if signal_kind == "studium_zone":
        return Decimal("20.00")
    return Decimal("0.00")


def signal_evidence_label(
    signal_kind: str,
    designation_normalized: str | None,
    plan_name: str | None,
) -> str:
    designation = designation_normalized or "unknown"
    if plan_name:
        return f"{signal_kind}: {designation} ({plan_name})"
    return f"{signal_kind}: {designation}"
