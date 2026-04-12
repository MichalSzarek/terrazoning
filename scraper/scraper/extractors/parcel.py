"""Parcel ID (numer dzialki) and locality/obreb extractor.

Polish parcel ID formats:
  - Simple:      123, 45
  - Subdivision: 123/4, 7/2/1
  - Full TERYT:  141201.0001.123/4

This extractor is intentionally defensive:
  - it rejects common false positives like komornik case numbers and shares,
  - it correlates parcel numbers with nearby locality names,
  - it tolerates OCR-like spacing artefacts from hostile HTML/PDF sources.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass


@dataclass(frozen=True)
class ParcelMatch:
    """Result of a single parcel ID extraction."""

    raw_value: str
    numer: str
    obreb_raw: str | None
    teryt_obreb: str | None
    confidence: float
    snippet: str
    char_offset: int


@dataclass(frozen=True)
class _LocalityCandidate:
    value: str
    offset: int
    confidence: float
    source: str


# ---------------------------------------------------------------------------
# Regex patterns for numer dzialki
# ---------------------------------------------------------------------------

_RE_FULL_TERYT = re.compile(
    r"""
    \b
    (\d{6})
    \.
    (\d{4})
    \.
    (\d+(?:/\d+)*)
    \b
    """,
    re.VERBOSE,
)

_RE_GUGIK = re.compile(
    r"""
    \b
    (\d{7})
    \.
    (\d{4})
    \.
    (\d+(?:/\d+)*)
    \b
    """,
    re.VERBOSE,
)

_RE_KEYWORD = re.compile(
    r"""
    (?:
        numer\w*\s+dzia(?:ł|l)k\w*
        |dzia(?:ł|l)k[a-ząćęłńóśźż]*
        |(?<!\w)dz\.?(?!\w)
        |nieruchomo(?:ść|ści|sc)
    )
    \s*
    (?:ewidencyjn\w*\s*)?
    (?:o\s+)?
    (?:(?:nr|numer)\.?\s*:?\s*)?
    (\d+(?:/\d+)*)
    """,
    re.VERBOSE | re.IGNORECASE,
)

_RE_NR_ONLY = re.compile(
    r"""
    (?<!\w)
    (?:nr|numer)\.?\s*:?\s*
    (\d+(?:/\d+)*)
    """,
    re.VERBOSE | re.IGNORECASE,
)

_RE_BARE_PARCEL = re.compile(
    r"""
    (?<!\d)
    (\d{1,5}/\d{1,4}(?:/\d{1,4})*)
    (?!\d)
    """,
    re.VERBOSE,
)

_RE_SYGNATURA_PREFIX = re.compile(r"(?:[Gg]?[Kk][Mm][Pp]?(?:\s+[A-Z])?)\s+\Z")
_RE_SHARE_CONTEXT = re.compile(
    r"(?:udzia[łl][a-z]*|cz[eę][śs]ci|u[łl]amkow\w*|wysoko[śs]ci)\s+\Z",
    re.IGNORECASE,
)
_RE_OBREB_PREFIX = re.compile(
    r"(?:obr(?:ę|e)b(?:u)?|(?:ob|obr)\.)\s*(?:nr|numer)?\s+\Z",
    re.IGNORECASE,
)

_RE_OBREB_CODE = re.compile(r"\b(\d{9})\b")
_RE_OBREB_KEYWORD = re.compile(
    r"""
    obr(?:ę|e)b(?:u)?
    \.?\s*
    (?:nr|numer)?\.?\s*:?\s*
    (?P<name>[^\n,;:.()]{1,80})
    """,
    re.VERBOSE | re.IGNORECASE,
)

_RE_OBREB_ABBREV = re.compile(
    r"""
    \b(?:ob|obr)\.
    \s*
    (?:
        \d{4}
        \s*[,;]
        \s*
    )?
    (?P<name>[^\n,;:.()]{2,80})
    """,
    re.VERBOSE | re.IGNORECASE,
)

_RE_PROPERTY_COUNT_FALSE_POSITIVE = re.compile(
    r"""
    nieruchomo(?:ść|ści|sc)
    \s+
    \d+
    \s+
    dzia(?:ł|l)k
    """,
    re.VERBOSE | re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Locality / obreb candidate extraction
# ---------------------------------------------------------------------------

_RE_LOCALITY_CONTEXT = re.compile(
    r"""
    (?:
        \bw\s+miejscow(?:o(?:ś|s)ci|osci)\b
        |\bmiejscow(?:o(?:ś|s)ć|osc)\b
        |\bwe?\s+wsi\b
        |\bpo(?:ł|l)\.?\s+w\b
        |\bpo(?:ł|l)(?:oż|oz)on\w*\s+w\b
        |\bpo(?:ł|l)(?:oż|oz)on\w*\s+we\b
        |\bzlokalizowan\w*\s+w\b
        |\busytuowan\w*\s+w\b
    )
    \s+
    (?P<name>[^\n,;:.()]{2,100})
    """,
    re.VERBOSE | re.IGNORECASE,
)

_RE_LOCALITY_GMINA_CONTEXT = re.compile(
    r"""
    \bw\s+gminie\b
    \s+
    (?P<name>[^\n,;:.()]{2,100})
    """,
    re.VERBOSE | re.IGNORECASE,
)

_RE_LOCALITY_AFTER_PROPERTY = re.compile(
    r"""
    (?:
        nieruchomo(?:ść|ści|sc)(?:\s+[a-ząćęłńóśźż]+){0,3}
        |dzia(?:ł|l)k[a-ząćęłńóśźż]*(?:\s+ewidencyjn\w*)?
    )
    \s+w\s+
    (?P<name>[^\n,;:.()]{2,100})
    """,
    re.VERBOSE | re.IGNORECASE,
)

_RE_LOCALITY_BEFORE_GMINA = re.compile(
    r"""
    (?P<name>
        [A-ZĄĆĘŁŃÓŚŹŻ][A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż\- ]{1,60}
    )
    \s+(?:gm\.|gmina)\s+
    """,
    re.VERBOSE,
)

_RE_MAP_MARKER_LOCALITY = re.compile(
    r"""
    map_marker
    \s+
    (?P<name>[^\n,]{2,80})
    (?=,\s*\d{2}-\d{3}\b|\s+Komornik\b)
    """,
    re.VERBOSE | re.IGNORECASE,
)

_RE_POSTAL_LOCALITY = re.compile(
    r"""
    \b\d{2}-\d{3}\s+
    (?P<name>[A-ZĄĆĘŁŃÓŚŹŻ][A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż\- ]{1,80})
    (?=,\s*poczta\b|\s+poczta\b|\s+Komornik\b|\s+Sygnatur(?:a|y)\b|\s+OBWIESZCZENIE\b|$)
    """,
    re.VERBOSE,
)

_RE_ADDRESS_LOCALITY = re.compile(
    r"""
    (?:
        adres\s+nieruchomo(?:ś|s)ci
        |miejsce\s+ogl[eę]dzin:\s*pod\s+adresem
    )
    \s+
    (?:[^,\n]{2,80},\s*)?
    (?:\d{2}-\d{3}\s+)?
    (?P<name>[A-ZĄĆĘŁŃÓŚŹŻ][A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż\- ]{1,80})
    (?=,\s*poczta\b|\s+poczta\b)
    """,
    re.VERBOSE | re.IGNORECASE,
)

_RE_EWID_UNIT = re.compile(
    r"""
    \bjedn\.\s*ewid\.
    \s+
    (?P<name>[A-ZĄĆĘŁŃÓŚŹŻ][A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż\- ]{1,80})
    (?=,\s*obr(?:ę|e)?b\b|\s+obr(?:ę|e)?b\b|,|\.)
    """,
    re.VERBOSE | re.IGNORECASE,
)

_RE_TERMINATOR = re.compile(
    r"""
    \s+
    (?:
        gm\.?
        |gmina
        |pow(?:\.|iat)
        |woj(?:\.|ew[oó]dztwo)?
        |komornik
        |kancelaria
        |sygnatur(?:a|y)
        |obwieszczenie
        |licytacja
        |map_[a-z_]+
        |map_marker
        |ul\.?
        |al\.?
        |pl\.?
        |obj[eę]t\w*
        |przy
        |nr\b
        |dz(?:\.|ia(?:ł|l)k\w*)
        |kw\b
        |pow\.
    )
    .*
    """,
    re.VERBOSE | re.IGNORECASE,
)

_RE_BAD_LOCALITY = re.compile(
    r"""
    ^
    (?:
        komornik
        |kancelaria
        |obwieszczenie
        |licytacja
        |nieruchomo(?:ść|ści|sc)
        |dzia(?:ł|l)k\w*
        |map_[a-z_]+
        |map_marker
        |sygnatur(?:a|y)
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)

_RE_LOCALITY_TAIL = re.compile(
    r"""
    \s+
    (?:
        o\s+(?:numerze|nr|powierzchni|łącznej|laczonej)
        |w\s+rejonie
        |przy\s+(?:ul(?:icy|\.)?|al(?:ei|\.)?|pl(?:acu|\.)?|drodze|trasie|posesji)
        |ul(?:icy|\.)?
        |poczta
        |adres
        |dla
        |posiadaj\w*
        |stanowiąc\w*
        |składaj\w*
        |obejmuj\w*
        |objęt\w*
        |bezpo\w*
        |bezp\w*
        |opis
        |wraz
        |któr\w*
    )
    .*
    """,
    re.VERBOSE | re.IGNORECASE,
)

_MAX_LOCALITY_DISTANCE = 260
_BAD_LOCALITY_TOKENS = {
    "drodze",
    "licytacji",
    "elektronicznej",
    "przeznaczenie",
    "przeznaczeniem",
    "siec",
    "sieć",
    "budowie",
    "bezposrednio",
    "bezpośrednio",
    "bezpśrednio",
    "bezpsrednio",
    "terenie",
    "terenieoatrakcyjnychwalorachwidokowych",
    "granicy",
    "frontowej",
    "wojewodztwie",
    "województwie",
    "ksiedze",
    "księdze",
    "zbioru",
    "dokumentow",
    "dokumentów",
    "ewidencji",
    "gruntow",
    "gruntów",
    "dzialce",
    "działce",
}
_BAD_LOCALITY_EXACT = {
    "bartniczej",
    "graniczna",
    "goscinna",
    "gościnna",
    "niedurnego",
    "ofiar wrzesnia",
    "ofiar września",
    "stodolska",
    "tysiaclecia",
    "tysiąclecia",
}
_NO_GLUE_TOKENS = {
    "a",
    "al",
    "albo",
    "do",
    "dla",
    "bezposrednio",
    "bezpośrednio",
    "bezpśrednio",
    "bezpsrednio",
    "gm",
    "gm.",
    "gmina",
    "gminie",
    "i",
    "kw",
    "na",
    "nr",
    "numerze",
    "o",
    "ob",
    "obr",
    "opis",
    "oraz",
    "poczta",
    "po",
    "pod",
    "pow",
    "pow.",
    "powiecie",
    "przy",
    "rejonie",
    "ul",
    "ul.",
    "ulicy",
    "w",
    "we",
    "wraz",
    "z",
    "ze",
}


def _strip_diacritics(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _stem_locality(value: str) -> tuple[str, ...]:
    suffixes = (
        "owie",
        "owego",
        "owej",
        "owym",
        "ami",
        "ach",
        "ego",
        "emu",
        "owa",
        "owe",
        "owi",
        "ach",
        "ym",
        "ie",
        "u",
        "a",
        "y",
        "e",
    )
    stems: list[str] = []
    for token in _strip_diacritics(value).lower().split():
        stem = token
        for suffix in suffixes:
            if stem.endswith(suffix) and len(stem) - len(suffix) >= 4:
                stem = stem[: -len(suffix)]
                break
        stems.append(stem)
    return tuple(stems)


def _same_locality_family(left: str, right: str) -> bool:
    return _stem_locality(left) == _stem_locality(right)


def _clean_locality_candidate(raw: str) -> str | None:
    value = raw.replace("\xa0", " ").replace("\n", " ").strip(" ,.;:-")
    value = re.sub(r"\s+", " ", value)
    value = re.sub(r"\s*-\s*", "-", value)
    value = _RE_LOCALITY_TAIL.sub("", value).strip(" ,.;:-")
    value = _RE_TERMINATOR.sub("", value).strip(" ,.;:-")
    value = re.sub(
        r"^(?:miejscow(?:o(?:ś|s)ci|osci)|gminie|gmina|wsi|wieś|mieście)\s+",
        "",
        value,
        flags=re.IGNORECASE,
    )
    if not value:
        return None

    repaired: list[str] = []
    for token in value.split():
        token = token.strip(" ,.;:()")
        if not token:
            continue
        normalized_token = _strip_diacritics(token).lower()
        if (
            repaired
            and token.islower()
            and len(token) >= 3
            and normalized_token not in _NO_GLUE_TOKENS
        ):
            previous = repaired[-1]
            if token.startswith("zcz") and not previous.endswith("s"):
                repaired[-1] = previous + "s" + token
            elif token.startswith("z") and previous[-1].lower() in "aąeęiouóy":
                repaired[-1] = previous + "sz" + token[1:]
            else:
                repaired[-1] = previous + token
        else:
            repaired.append(token)

    value = " ".join(repaired).strip(" ,.;:-")
    if not value:
        return None

    if re.fullmatch(r"[A-ZĄĆĘŁŃÓŚŹŻ\- ]+", value):
        value = value.title()
    if not value[0].isupper():
        return None

    if _RE_BAD_LOCALITY.search(value):
        return None
    if any(ch.isdigit() for ch in value):
        return None
    if "." in value:
        return None

    tokens = value.split()
    if not tokens or len(tokens) > 4:
        return None
    if any(len(token) == 1 for token in tokens):
        return None
    if len(tokens) == 1 and len(tokens[0]) > 20:
        return None

    normalized = _strip_diacritics(value).lower()
    if normalized in {"m", "miasto", "miasta", "powiat", "gmina", "dz", "nr", "bn"}:
        return None
    if normalized in _BAD_LOCALITY_EXACT:
        return None
    if any(token in normalized for token in _BAD_LOCALITY_TOKENS):
        return None

    return value


def _collect_locality_candidates(text: str) -> list[_LocalityCandidate]:
    raw_candidates: list[_LocalityCandidate] = []

    patterns = (
        (_RE_LOCALITY_CONTEXT, 0.68, "context"),
        (_RE_LOCALITY_AFTER_PROPERTY, 0.78, "property_context"),
        (_RE_LOCALITY_GMINA_CONTEXT, 0.56, "gmina_context"),
        (_RE_LOCALITY_BEFORE_GMINA, 0.70, "before_gmina"),
        (_RE_MAP_MARKER_LOCALITY, 0.76, "map_marker"),
        (_RE_POSTAL_LOCALITY, 0.74, "postal_code"),
        (_RE_ADDRESS_LOCALITY, 0.88, "address_locality"),
        (_RE_EWID_UNIT, 0.90, "ewid_unit"),
        (_RE_OBREB_KEYWORD, 0.82, "obreb_keyword"),
        (_RE_OBREB_ABBREV, 0.88, "obreb_abbrev"),
    )

    for regex, confidence, source in patterns:
        for match in regex.finditer(text):
            start = match.start("name") if "name" in match.groupdict() else match.start(1)
            raw = match.group("name") if "name" in match.groupdict() else match.group(1)
            cleaned = _clean_locality_candidate(raw)
            if not cleaned:
                continue
            raw_candidates.append(
                _LocalityCandidate(
                    value=cleaned,
                    offset=start,
                    confidence=confidence,
                    source=source,
                )
            )

    deduped: dict[str, _LocalityCandidate] = {}
    for candidate in raw_candidates:
        key = _strip_diacritics(candidate.value).lower()
        existing = deduped.get(key)
        if existing is None or candidate.confidence > existing.confidence:
            deduped[key] = candidate

    return sorted(
        deduped.values(),
        key=lambda item: (-item.confidence, item.offset),
    )


def _match_locality_for_offset(
    candidates: list[_LocalityCandidate],
    offset: int,
) -> _LocalityCandidate | None:
    best: tuple[float, int, _LocalityCandidate] | None = None

    for candidate in candidates:
        distance = abs(candidate.offset - offset)
        if distance > _MAX_LOCALITY_DISTANCE:
            continue
        score = _base_locality_score(candidate, candidates) + max(0.0, 0.16 - (distance / 1000))
        if best is None or score > best[0] or (score == best[0] and distance < best[1]):
            best = (score, distance, candidate)

    return best[2] if best else None


def _base_locality_score(
    candidate: _LocalityCandidate,
    candidates: list[_LocalityCandidate],
) -> float:
    score = candidate.confidence

    corroborated = any(
        other is not candidate
        and abs(other.offset - candidate.offset) <= 140
        and _same_locality_family(candidate.value, other.value)
        for other in candidates
    )
    if candidate.source in {"postal_code", "map_marker"} and corroborated:
        score += 0.10

    competing_context = any(
        other is not candidate
        and abs(other.offset - candidate.offset) <= 180
        and other.source in {
            "context",
            "property_context",
            "before_gmina",
            "obreb_keyword",
            "ewid_unit",
            "address_locality",
        }
        for other in candidates
    )
    if candidate.source == "map_marker" and competing_context and not corroborated:
        score -= 0.08

    return score


def _is_non_parcel_context(text: str, match_start: int, look_back: int = 30) -> bool:
    """Reject case-number/share contexts before a numeric match."""
    prefix = text[max(0, match_start - look_back):match_start]
    prefix = unicodedata.normalize("NFC", prefix)
    return bool(
        _RE_SYGNATURA_PREFIX.search(prefix)
        or _RE_SHARE_CONTEXT.search(prefix)
        or _RE_OBREB_PREFIX.search(prefix)
    )


def _is_postal_code_fragment(text: str, span_end: int) -> bool:
    return bool(re.match(r"-\d{3}\b", text[span_end:span_end + 5]))


def _is_property_count_false_positive(text: str, start: int, end: int) -> bool:
    window = text[start:min(len(text), end + 20)]
    return bool(_RE_PROPERTY_COUNT_FALSE_POSITIVE.search(window))


def _is_bare_postal_prefix_false_positive(text: str, numer: str, match_start: int) -> bool:
    """Reject bare integer candidates that only mirror a nearby postal code.

    Example:
        "Adres nieruchomości 44-105 Gliwice" should never produce parcel "44".
    """
    if "/" in numer:
        return False
    window = text[max(0, match_start - 24):min(len(text), match_start + 48)]
    return bool(re.search(rf"\b{re.escape(numer)}-\d{{3}}\b", window))


def extract_parcel_ids(
    text: str,
    snippet_radius: int = 40,
) -> list[ParcelMatch]:
    """Extract parcel ID candidates from arbitrary text.

    Returns list of ParcelMatch sorted by confidence DESC.
    Deduplicates on normalized numer while keeping the strongest match.
    """
    clean = unicodedata.normalize("NFC", text)
    locality_candidates = _collect_locality_candidates(clean)
    seen: dict[str, ParcelMatch] = {}

    for match in _RE_FULL_TERYT.finditer(clean):
        gmina6, obreb4, numer = match.group(1), match.group(2), match.group(3)
        _reg_parcel(
            seen=seen,
            raw=match.group(0),
            numer=numer,
            obreb_raw=obreb4,
            teryt_obreb=gmina6 + obreb4[:2] if len(obreb4) >= 2 else None,
            confidence=0.90,
            text=clean,
            offset=match.start(),
            radius=snippet_radius,
        )

    for match in _RE_GUGIK.finditer(clean):
        gmina7, obreb4, numer = match.group(1), match.group(2), match.group(3)
        _reg_parcel(
            seen=seen,
            raw=match.group(0),
            numer=numer,
            obreb_raw=obreb4,
            teryt_obreb=gmina7 + obreb4[:2],
            confidence=0.88,
            text=clean,
            offset=match.start(),
            radius=snippet_radius,
        )

    for regex, base_confidence in ((_RE_KEYWORD, 0.72), (_RE_NR_ONLY, 0.66)):
        for match in regex.finditer(clean):
            numer = match.group(1)
            if numer in seen:
                continue
            if _is_property_count_false_positive(clean, match.start(), match.end()):
                continue
            if _is_non_parcel_context(clean, match.start()):
                continue
            if _is_bare_postal_prefix_false_positive(clean, numer, match.start(1)):
                continue
            if _is_postal_code_fragment(clean, match.end(1)):
                continue
            locality = _match_locality_for_offset(locality_candidates, match.start())
            confidence = min(base_confidence + (0.08 if locality else 0.0), 0.82)
            _reg_parcel(
                seen=seen,
                raw=match.group(0),
                numer=numer,
                obreb_raw=locality.value if locality else None,
                teryt_obreb=None,
                confidence=confidence,
                text=clean,
                offset=match.start(),
                radius=snippet_radius,
            )

    for match in _RE_BARE_PARCEL.finditer(clean):
        numer = match.group(1)
        if numer in seen:
            continue
        if _is_non_parcel_context(clean, match.start()):
            continue
        if _is_postal_code_fragment(clean, match.end(1)):
            continue
        locality = _match_locality_for_offset(locality_candidates, match.start())
        confidence = min(0.55 + (0.12 if locality else 0.0), 0.70)
        _reg_parcel(
            seen=seen,
            raw=match.group(0),
            numer=numer,
            obreb_raw=locality.value if locality else None,
            teryt_obreb=None,
            confidence=confidence,
            text=clean,
            offset=match.start(),
            radius=snippet_radius,
        )

    return sorted(seen.values(), key=lambda item: (-item.confidence, item.char_offset))


def extract_obreb(text: str) -> tuple[str | None, str | None]:
    """Extract an obreb/locality name or TERYT code from text."""
    clean = unicodedata.normalize("NFC", text)

    for match in _RE_OBREB_CODE.finditer(clean):
        code = match.group(1)
        if int(code[:2]) > 0:
            return code, code

    keyword_match = _RE_OBREB_KEYWORD.search(clean)
    if keyword_match:
        name = _clean_locality_candidate(keyword_match.group("name"))
        if name:
            return name, None

    candidates = _collect_locality_candidates(clean)
    if candidates:
        best = max(candidates, key=lambda item: (_base_locality_score(item, candidates), -item.offset))
        return best.value, None

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

    snippet = _snippet(text, offset, len(raw), radius)
    seen[numer] = ParcelMatch(
        raw_value=raw,
        numer=numer,
        obreb_raw=obreb_raw,
        teryt_obreb=teryt_obreb,
        confidence=confidence,
        snippet=snippet,
        char_offset=offset,
    )


def _snippet(text: str, offset: int, match_len: int, radius: int) -> str:
    start = max(0, offset - radius)
    end = min(len(text), offset + match_len + radius)
    return text[start:end].replace("\n", " ").strip()
