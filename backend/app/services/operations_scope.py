"""Shared province-scoping helpers for operational scripts."""

from __future__ import annotations

from decimal import Decimal
import unicodedata

from sqlalchemy import case, func

_PROVINCE_SPECS = {
    "podkarpackie": {
        "display_name": "Podkarpackie",
        "db_label": "podkarpackie",
        "teryt_prefix": "18",
    },
    "slaskie": {
        "display_name": "Śląskie",
        "db_label": "śląskie",
        "teryt_prefix": "24",
    },
    "malopolskie": {
        "display_name": "Małopolskie",
        "db_label": "małopolskie",
        "teryt_prefix": "12",
    },
}

_BACKLOG_HINTS = {
    "no_source_configured": (
        "No confirmed MPZP source is configured yet. Prioritize SIP/WFS discovery "
        "before replay work."
    ),
    "source_configured_but_not_loaded": (
        "A source is already configured, but no planning zones are loaded yet. "
        "Probe the source and rerun sync before resolver work."
    ),
    "covered_but_no_delta": (
        "Planning coverage already exists. This is not a source-discovery gap; "
        "check parcel placement, locality quality, or delta thresholds."
    ),
    "covered_but_no_buildable_delta": (
        "Planning coverage and delta intersections exist, but none became a "
        "buildable lead. Review land-use classes and business thresholds."
    ),
}

_DLQ_CATEGORY_HINTS = {
    "parser_issue": (
        "Listing extraction likely missed or malformed a key field. Prefer Bronze/LLM "
        "reparse before another resolver replay."
    ),
    "resolver_ambiguity": (
        "Resolver found candidates or transient ULDK issues but could not select a "
        "single parcel safely. Review locality, area hints, or source text."
    ),
    "missing_planning_source": (
        "Resolution is blocked downstream by missing planning coverage for the likely "
        "municipality. Prioritize MPZP source discovery or raster ingestion."
    ),
    "manual_only_case": (
        "Automatic retries are exhausted or unsupported. This listing now requires "
        "manual analyst review."
    ),
}

_COVERAGE_TERYT_ALIASES = {
    # Kraków EGiB units (JE) resolved from the public MSIP parcel layers should be
    # treated as covered by the city-wide Kraków MPZP source during operator reporting.
    "1261029": "1261011",  # Krowodrza -> Kraków
    "1261039": "1261011",  # Nowa Huta -> Kraków
    "1261049": "1261011",  # Podgórze -> Kraków
    "1261059": "1261011",  # Śródmieście -> Kraków
}


def _ascii_key(value: str | None) -> str:
    if not value:
        return ""
    text = value.strip().replace("ł", "l").replace("Ł", "L")
    normalized = unicodedata.normalize("NFKD", text)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return normalized.lower().replace("-", " ")


def normalize_province(province: str | None) -> str | None:
    """Normalize user-facing province names to stable internal keys."""
    key = _ascii_key(province)
    if not key:
        return None
    compact = " ".join(key.split())
    if compact in _PROVINCE_SPECS:
        return compact
    return None


def province_db_label(province: str | None) -> str | None:
    key = normalize_province(province)
    if key is None:
        return None
    return _PROVINCE_SPECS[key]["db_label"]


def province_display_name(province: str | None) -> str | None:
    key = normalize_province(province)
    if key is None:
        return None
    return _PROVINCE_SPECS[key]["display_name"]


def province_teryt_prefix(province: str | None) -> str | None:
    key = normalize_province(province)
    if key is None:
        return None
    return _PROVINCE_SPECS[key]["teryt_prefix"]


def sql_teryt_prefix_filter(column, province: str):
    """SQLAlchemy predicate filtering a TERYT column to a province."""
    prefix = province_teryt_prefix(province)
    if prefix is None:
        raise ValueError(f"Unsupported province: {province!r}")
    return func.substr(column, 1, 2) == prefix


def sql_listing_province_filter(column, province: str):
    """SQLAlchemy predicate filtering bronze rows by raw wojewodztwo."""
    label = province_db_label(province)
    if label is None:
        raise ValueError(f"Unsupported province: {province!r}")
    return func.lower(func.coalesce(column, "")) == label


def provinces() -> tuple[str, ...]:
    return tuple(_PROVINCE_SPECS)


def coverage_alias_teryt(teryt: str | None) -> str | None:
    if not teryt:
        return teryt
    return _COVERAGE_TERYT_ALIASES.get(teryt, teryt)


def sql_coverage_alias(column):
    """Normalize TERYT-like codes for reporting-level planning coverage checks."""
    whens = {key: value for key, value in _COVERAGE_TERYT_ALIASES.items()}
    return case(whens, value=column, else_=column)


def classify_backlog_status(
    *,
    in_registry: bool,
    has_planning_zones: bool,
    has_delta_rows: bool,
) -> tuple[str, str]:
    """Classify operator backlog rows into source vs. covered-data buckets."""
    if not has_planning_zones:
        status = (
            "source_configured_but_not_loaded"
            if in_registry
            else "no_source_configured"
        )
        return status, _BACKLOG_HINTS[status]

    status = (
        "covered_but_no_buildable_delta"
        if has_delta_rows
        else "covered_but_no_delta"
    )
    return status, _BACKLOG_HINTS[status]


def classify_price_signal(
    *,
    price_zl: Decimal | float | int | None,
    price_per_m2_zl: Decimal | float | int | None,
) -> str:
    """Classify price quality for investor-facing ranking and reporting."""
    if price_zl is None and price_per_m2_zl is None:
        return "missing"
    if price_zl is not None and Decimal(str(price_zl)) < Decimal("1000"):
        return "suspicious"
    if price_per_m2_zl is not None:
        ppm = Decimal(str(price_per_m2_zl))
        if ppm < Decimal("5") or ppm > Decimal("20000"):
            return "suspicious"
    return "reliable"


def lead_missing_metrics(
    *,
    price_zl: Decimal | float | int | None,
    price_per_m2_zl: Decimal | float | int | None,
    area_m2: Decimal | float | int | None,
    max_buildable_area_m2: Decimal | float | int | None,
    max_coverage_pct: Decimal | float | int | None,
    dominant_przeznaczenie: str | None,
) -> tuple[str, ...]:
    """Return explicit missing metrics so leads are never silently partial."""
    missing: list[str] = []
    if price_zl is None:
        missing.append("price_zl")
    if price_per_m2_zl is None:
        missing.append("price_per_m2_zl")
    if area_m2 is None:
        missing.append("area_m2")
    if max_buildable_area_m2 is None:
        missing.append("max_buildable_area_m2")
    if max_coverage_pct is None:
        missing.append("max_coverage_pct")
    if not dominant_przeznaczenie:
        missing.append("dominant_przeznaczenie")
    return tuple(missing)


def classify_lead_quality(
    *,
    price_zl: Decimal | float | int | None,
    price_per_m2_zl: Decimal | float | int | None,
    area_m2: Decimal | float | int | None,
    max_buildable_area_m2: Decimal | float | int | None,
    max_coverage_pct: Decimal | float | int | None,
    dominant_przeznaczenie: str | None,
) -> tuple[str, tuple[str, ...]]:
    """Expose an explicit completeness/quality signal for every lead."""
    missing = lead_missing_metrics(
        price_zl=price_zl,
        price_per_m2_zl=price_per_m2_zl,
        area_m2=area_m2,
        max_buildable_area_m2=max_buildable_area_m2,
        max_coverage_pct=max_coverage_pct,
        dominant_przeznaczenie=dominant_przeznaczenie,
    )
    price_signal = classify_price_signal(
        price_zl=price_zl,
        price_per_m2_zl=price_per_m2_zl,
    )
    if "price_zl" in missing or "price_per_m2_zl" in missing:
        return "missing_financials", missing
    if price_signal == "suspicious":
        return "review_required", missing
    if max_coverage_pct is not None and Decimal(str(max_coverage_pct)) < Decimal("15"):
        # A parcel can still qualify as a lead by absolute buildable area even when
        # the percentage coverage is low. We keep it as a lead, but force analyst
        # review so ranking/reporting does not treat it like a clean full-coverage case.
        return "review_required", missing
    if missing:
        return "partial", missing
    return "complete", missing


def compute_investment_score(
    *,
    confidence_score: Decimal | float | int | None,
    price_zl: Decimal | float | int | None,
    price_per_m2_zl: Decimal | float | int | None,
    max_buildable_area_m2: Decimal | float | int | None,
    max_coverage_pct: Decimal | float | int | None,
    price_signal: str | None = None,
    quality_signal: str | None = None,
) -> float:
    """Return a sortable investor-facing score for ranking opportunities.

    Lower price/m² should help, but cannot dominate obvious quality risks like:
      - suspicious pricing
      - very low MPZP coverage
      - incomplete metrics
    """
    resolved_price_signal = price_signal or classify_price_signal(
        price_zl=price_zl,
        price_per_m2_zl=price_per_m2_zl,
    )
    resolved_quality_signal, _ = (
        (quality_signal, ())
        if quality_signal is not None
        else classify_lead_quality(
            price_zl=price_zl,
            price_per_m2_zl=price_per_m2_zl,
            area_m2=None,
            max_buildable_area_m2=max_buildable_area_m2,
            max_coverage_pct=max_coverage_pct,
            dominant_przeznaczenie="X",
        )
    )

    score = Decimal("0")
    confidence = Decimal(str(confidence_score or 0))
    coverage = Decimal(str(max_coverage_pct or 0))
    buildable_area = Decimal(str(max_buildable_area_m2 or 0))

    score += min(confidence, Decimal("1")) * Decimal("35")
    score += min(coverage, Decimal("100")) * Decimal("0.35")
    score += min(buildable_area, Decimal("5000")) / Decimal("5000") * Decimal("20")

    if price_per_m2_zl is not None:
        ppm = Decimal(str(price_per_m2_zl))
        if ppm > 0:
            price_bonus = max(Decimal("0"), Decimal("30") - min(ppm / Decimal("25"), Decimal("30")))
            score += price_bonus

    if resolved_price_signal == "suspicious":
        score -= Decimal("35")
    elif resolved_price_signal == "missing":
        score -= Decimal("20")

    if resolved_quality_signal == "review_required":
        score -= Decimal("20")
    elif resolved_quality_signal == "missing_financials":
        score -= Decimal("25")
    elif resolved_quality_signal == "partial":
        score -= Decimal("10")

    if coverage and coverage < Decimal("30"):
        score -= Decimal("12")
    if coverage and coverage < Decimal("15"):
        score -= Decimal("18")

    return float(max(score, Decimal("0")).quantize(Decimal("0.01")))


def classify_dlq_error(
    *,
    last_error: str | None,
    attempt_count: int,
    raw_obreb: str | None = None,
    raw_numer_dzialki: str | None = None,
) -> tuple[str, str]:
    """Map raw DLQ reasons to stable operator-facing categories."""
    error = (last_error or "").strip()
    lowered = error.lower()
    raw_obreb_lower = (raw_obreb or "").strip().lower()
    raw_numer_lower = (raw_numer_dzialki or "").strip().lower()

    if (
        attempt_count >= 5
        or "manual_review_required" in lowered
        or "geometry_missing" in lowered
        or "kw_resolution_unsupported" in lowered
        or "unsupported" in lowered
    ):
        category = "manual_only_case"
        return category, _DLQ_CATEGORY_HINTS[category]

    if (
        any(ch.isalpha() for ch in raw_numer_lower)
        or "użytkowanie wieczyste" in raw_obreb_lower
        or "uzytkowanie wieczyste" in raw_obreb_lower
    ):
        category = "parser_issue"
        return category, _DLQ_CATEGORY_HINTS[category]

    if (
        "parse" in lowered
        or "extract" in lowered
        or "missing parcel" in lowered
        or "no parcel number" in lowered
        or "raw_teryt" in lowered
    ):
        category = "parser_issue"
        return category, _DLQ_CATEGORY_HINTS[category]

    if (
        "planning source" in lowered
        or "no_source" in lowered
        or "source_configured" in lowered
    ):
        category = "missing_planning_source"
        return category, _DLQ_CATEGORY_HINTS[category]

    category = "resolver_ambiguity"
    return category, _DLQ_CATEGORY_HINTS[category]
