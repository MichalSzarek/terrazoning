"""Helpers for parsing Polish monetary values and other decimal numbers."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

_SPACE_LIKE_CHARS_RE = re.compile(r"[\s\u00A0\u202F\u2007\u2009]+")
_NON_NUMERIC_RE = re.compile(r"[^0-9,.\-]")
_PRICE_RE = re.compile(
    r"(?<!\d)(\d[\d\s\u00A0\u202F\u2007\u2009.,]*\d|\d)\s*(?:z[łl]|pln)\b",
    re.IGNORECASE,
)
_PREFERRED_PRICE_CONTEXT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        r"(?:cena\s+wywo(?:ł|l)ania|cena\s+wywo(?:ł|l)awcza)"
        r"[^\d]{0,48}"
        r"(?:kwot(?:ę|e)\s*)?"
        r"(\d[\d\s\u00A0\u202F\u2007\u2009.,]*\d|\d)\s*(?:z[łl]|pln)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:suma\s+oszacowania|warto(?:ś|s)ć\s+oszacowania|warto(?:ś|s)ci\s+oszacowania|"
        r"oszacowan\w*\s+na\s+kwot(?:ę|e)|oszacowan\w*\s+na)"
        r"[^\d]{0,48}"
        r"(?:kwot(?:ę|e)\s*)?"
        r"(\d[\d\s\u00A0\u202F\u2007\u2009.,]*\d|\d)\s*(?:z[łl]|pln)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:za\s+cen(?:ę|e)\s+nie\s+ni(?:ż|z)sz(?:ą|a)\s+ni(?:ż|z)|"
        r"cena\s+nabycia|cena\s+sprzeda(?:ż|z)y)"
        r"[^\d]{0,48}"
        r"(\d[\d\s\u00A0\u202F\u2007\u2009.,]*\d|\d)\s*(?:z[łl]|pln)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:stanowi(?:ą|a)c\w*|wynosz\w*)"
        r"[^\d]{0,24}"
        r"(?:3/4|trzy\s+czwarte)"
        r"[^\d]{0,24}"
        r"(?:sumy\s+)?oszacowania"
        r"[^\d]{0,48}"
        r"(\d[\d\s\u00A0\u202F\u2007\u2009.,]*\d|\d)\s*(?:z[łl]|pln)\b",
        re.IGNORECASE,
    ),
)
_EXCLUDED_PRICE_CONTEXT_RE = re.compile(
    r"najni(?:ż|z)sze\s+post(?:ą|a)pienie|wadium|r(?:ę|e)kojmia|"
    r"udzia(?:ł|l)|u[łl]amek|udziale?|zaliczk\w*|koszt\w*",
    re.IGNORECASE,
)


def parse_polish_decimal(text: str) -> Decimal | None:
    """Parse a Polish-formatted number into Decimal.

    Examples:
      - "129 000,00"     -> Decimal("129000.00")
      - "129000"         -> Decimal("129000")
      - "1.234.567,89"   -> Decimal("1234567.89")
      - "1,234,567.89"   -> Decimal("1234567.89")
      - "603 m2"         -> Decimal("603")
    """
    if not text:
        return None

    compact = _SPACE_LIKE_CHARS_RE.sub("", text.strip())
    compact = _NON_NUMERIC_RE.sub("", compact)
    if not compact or compact in {".", ",", "-", "-.", "-,"}:
        return None

    last_comma = compact.rfind(",")
    last_dot = compact.rfind(".")

    if last_comma >= 0 and last_dot >= 0:
        decimal_sep = "," if last_comma > last_dot else "."
        thousands_sep = "." if decimal_sep == "," else ","
        compact = compact.replace(thousands_sep, "")
        compact = compact.replace(decimal_sep, ".")
    elif last_comma >= 0:
        fraction = compact[last_comma + 1:]
        if len(fraction) in {1, 2}:
            compact = compact.replace(".", "")
            compact = compact.replace(",", ".")
        else:
            compact = compact.replace(",", "")
    elif last_dot >= 0:
        fraction = compact[last_dot + 1:]
        if len(fraction) in {1, 2}:
            compact = compact.replace(",", "")
        else:
            compact = compact.replace(".", "")

    if compact.count(".") > 1:
        head, tail = compact.rsplit(".", 1)
        compact = head.replace(".", "") + "." + tail

    try:
        return Decimal(compact)
    except (InvalidOperation, ValueError):
        return None


def parse_price_value(text: str) -> Decimal | None:
    """Parse a single Polish price string to Decimal within sane range."""
    value = parse_polish_decimal(text)
    if value is None:
        return None
    if Decimal("1000") <= value <= Decimal("100000000"):
        return value
    return None


def _has_excluded_context(text: str, start: int, end: int) -> bool:
    window_start = max(0, start - 36)
    window_end = min(len(text), end + 20)
    return bool(_EXCLUDED_PRICE_CONTEXT_RE.search(text[window_start:window_end]))


def _extract_contextual_price_from_text(text: str) -> Decimal | None:
    for pattern in _PREFERRED_PRICE_CONTEXT_PATTERNS:
        for match in pattern.finditer(text):
            value = parse_price_value(match.group(1))
            if value is not None:
                return value
    return None


def extract_price_from_text(text: str) -> Decimal | None:
    """Extract the most plausible auction price from free text."""
    contextual = _extract_contextual_price_from_text(text)
    if contextual is not None:
        return contextual

    for match in _PRICE_RE.finditer(text):
        if _has_excluded_context(text, match.start(), match.end()):
            continue
        value = parse_price_value(match.group(1))
        if value is not None:
            return value
    return None
