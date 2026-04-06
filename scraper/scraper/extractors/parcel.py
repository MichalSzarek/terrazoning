"""Parcel ID (numer działki) and TERYT extractor.

Polish parcel ID formats:
  - Simple:      123, 45
  - Subdivision: 123/4, 7/2/1
  - Full TERYT:  141201.0001.123/4  (TERYT 6-digit . obręb 4-digit . działka)

Polish TERYT field lengths:
  - Województwo: 2 digits
  - Powiat:      4 digits (ww + pp)
  - Gmina:       7 digits (ww + pp + gg + r)
  - Obręb:       9 digits (gmina + obreb 2)

Confidence scoring (per persona rubric):
  - Full TERYT in canonical form → 0.90
  - Partial TERYT (obreb + numer) → 0.75
  - Bare numer działki from text  → 0.55
  - OCR/fuzzy match               → 0.35
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass


@dataclass(frozen=True)
class ParcelMatch:
    """Result of a single parcel ID extraction.

    raw_value:    Exact string as found in source
    numer:        Normalised działka number (e.g. '123/4')
    obreb_raw:    Raw obręb string (name or code) if found
    teryt_obreb:  9-digit TERYT obręb code if parseable
    confidence:   0.0–1.0
    snippet:      Surrounding text for Evidence Chain (max 60 chars)
    char_offset:  Position in source
    """

    raw_value: str
    numer: str
    obreb_raw: str | None
    teryt_obreb: str | None
    confidence: float
    snippet: str
    char_offset: int


# ---------------------------------------------------------------------------
# Regex patterns for numer działki
# ---------------------------------------------------------------------------

# Full TERYT canonical form: 141201.0001.123/4
_RE_FULL_TERYT = re.compile(
    r"""
    \b
    (\d{6})         # 6-digit TERYT gmina (without rodzaj)
    \.
    (\d{4})         # 4-digit obręb number
    \.
    (\d+(?:/\d+)*)  # działka number: 123, 123/4, 7/2/1
    \b
    """,
    re.VERBOSE,
)

# TERYT obręb code (9-digit) + działka: "141201_1.0001.123/4" (GUGiK format)
_RE_GUGIK = re.compile(
    r"""
    \b
    (\d{7})          # 7-digit TERYT gmina
    \.
    (\d{4})          # 4-digit obręb
    \.
    (\d+(?:/\d+)*)   # działka
    \b
    """,
    re.VERBOSE,
)

# Keyword-anchored pattern: "działka nr 123/4", "dz. 45", "nr dz. 12/1"
# Also catches: "nieruchomość gruntowa nr 45/2"
_RE_KEYWORD = re.compile(
    r"""
    (?:
        dzia(?:ł|l)ka       # działka (with optional missing ł)
        |dz\.?              # abbreviation dz. or dz
        |nr\s+(?:dz\.?)?    # nr dz or just nr
        |nieruchomo(?:ść|sc) # nieruchomość
    )
    \s*(?:nr\.?\s*)?        # optional "nr" before the number
    (\d+(?:/\d+)*)          # the działka number itself
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Bare number pattern: standalone "123/4" — lowest confidence, many false positives
# Only fire if the number looks like a valid parcel: digit+slash+digit
_RE_BARE_PARCEL = re.compile(
    r"""
    (?<!\d)           # not preceded by digit (avoid matching 1/3 of a fraction mid-number)
    (\d{1,5}/\d{1,4}) # działka: 1–5 digits / 1–4 digits  (e.g. 123/4, 9/12)
    (?!\d)            # not followed by digit
    """,
    re.VERBOSE,
)

# TERYT obręb code standalone: 9-char like "141201001"
_RE_OBREB_CODE = re.compile(r"\b(\d{9})\b")

# Obręb keyword: "obreb Wola", "obr. 0001", "obręb nr 14"
_RE_OBREB_KEYWORD = re.compile(
    r"""
    obr(?:ę|e)b(?:u)?      # obręb/obreby with optional diacritic loss
    \.?\s+
    (?:nr\.?\s+)?
    ([\w\s\-łóąćęńśźżŁÓĄĆĘŃŚŹŻ]{1,60}) # obreb name or code
    """,
    re.VERBOSE | re.IGNORECASE,
)


def extract_parcel_ids(
    text: str,
    snippet_radius: int = 40,
) -> list[ParcelMatch]:
    """Extract parcel ID candidates from arbitrary text.

    Returns list of ParcelMatch sorted by confidence DESC.
    Deduplicates on normalized numer.
    """
    clean = unicodedata.normalize("NFC", text)
    seen: dict[str, ParcelMatch] = {}

    # Pass 1: Full TERYT canonical form — highest confidence
    for m in _RE_FULL_TERYT.finditer(clean):
        gmina6, obreb4, numer = m.group(1), m.group(2), m.group(3)
        teryt_obreb = gmina6 + obreb4[:2] if len(obreb4) >= 2 else None
        _reg_parcel(
            seen, m.group(0), numer, obreb4, teryt_obreb,
            confidence=0.90, text=clean, offset=m.start(), radius=snippet_radius,
        )

    # Pass 2: GUGiK 7+4+parcel format
    for m in _RE_GUGIK.finditer(clean):
        gmina7, obreb4, numer = m.group(1), m.group(2), m.group(3)
        _reg_parcel(
            seen, m.group(0), numer, obreb4, gmina7 + obreb4[:2],
            confidence=0.88, text=clean, offset=m.start(), radius=snippet_radius,
        )

    # Pass 3: Keyword-anchored (działka nr, dz., etc.)
    for m in _RE_KEYWORD.finditer(clean):
        numer = m.group(1)
        if numer in seen:
            continue
        _reg_parcel(
            seen, m.group(0), numer, None, None,
            confidence=0.70, text=clean, offset=m.start(), radius=snippet_radius,
        )

    # Pass 4: Bare slash-parcel — only if ≥ 2 chars each side
    for m in _RE_BARE_PARCEL.finditer(clean):
        numer = m.group(1)
        if numer in seen:
            continue
        _reg_parcel(
            seen, m.group(0), numer, None, None,
            confidence=0.55, text=clean, offset=m.start(), radius=snippet_radius,
        )

    return sorted(seen.values(), key=lambda x: x.confidence, reverse=True)


def extract_obreb(text: str) -> tuple[str | None, str | None]:
    """Extract obręb name and/or code from text.

    Returns: (obreb_name_or_code, teryt_obreb_9digit_or_None)
    """
    clean = unicodedata.normalize("NFC", text)

    # Try 9-digit code first
    for m in _RE_OBREB_CODE.finditer(clean):
        code = m.group(1)
        # Sanity: first 7 digits should look like a TERYT gmina code (non-zero)
        if int(code[:2]) > 0:
            return code, code

    # Fall back to keyword match
    m = _RE_OBREB_KEYWORD.search(clean)
    if m:
        return m.group(1).strip(), None

    return None, None


def _reg_parcel(
    seen: dict[str, ParcelMatch],
    raw: str,
    numer: str,
    obreb_raw: str | None,
    teryt_obreb: str | None,
    confidence: float,
    text: str,
    offset: int,
    radius: int,
) -> None:
    existing = seen.get(numer)
    if existing and existing.confidence >= confidence:
        return
    snip = _snippet(text, offset, len(raw), radius)
    seen[numer] = ParcelMatch(
        raw_value=raw,
        numer=numer,
        obreb_raw=obreb_raw,
        teryt_obreb=teryt_obreb,
        confidence=confidence,
        snippet=snip,
        char_offset=offset,
    )


def _snippet(text: str, offset: int, match_len: int, radius: int) -> str:
    start = max(0, offset - radius)
    end = min(len(text), offset + match_len + radius)
    return text[start:end].replace("\n", " ").strip()
