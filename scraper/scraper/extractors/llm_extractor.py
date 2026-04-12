"""LLM fallback extractor for hostile parcel/locality text.

Regex stays the primary extractor. Gemini is only used as a cheap fallback when:
  - the best regex parcel match is weak (< 0.70), or
  - no usable locality / precinct was extracted.

The fallback is intentionally conservative:
  - it uses JSON Schema constrained output,
  - it validates that the returned parcel number actually exists in the source text,
  - it never overwrites a strong regex hit with a weaker LLM guess.
"""

from __future__ import annotations

import asyncio
import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Any

import google.auth
from google import genai
from google.genai import types
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from app.core.config import settings
from scraper.extractors.parcel import ParcelMatch, extract_obreb, extract_parcel_ids

logger = logging.getLogger(__name__)

_MAX_PROMPT_CHARS = 12_000
_MAX_LLM_ATTEMPTS = 3
_LOCALITY_SUSPECT_TOKENS = (
    "komornik",
    "kancelar",
    "sąd",
    "sad",
    "sygnatur",
    "numerze",
    "rejonowym",
    "ul.",
    "ulica",
    "adres",
)
_PARCEL_TOKEN = re.compile(r"\b\d+(?:/\d+)+\b")

_SYSTEM_PROMPT = """
Jesteś polskim ekspertem geodezyjnym i analitykiem ogłoszeń komorniczych.
Czytasz nieuporządkowany, pełen literówek tekst z obwieszczenia i wyciągasz
wyłącznie dane widoczne w źródle.

Twoje zadanie:
1. znajdź główny numer działki ewidencyjnej będącej przedmiotem licytacji,
2. znajdź nazwę obrębu, wsi, miejscowości albo miasta, w którym ta działka leży,
3. opcjonalnie znajdź nazwę gminy,
4. opcjonalnie znajdź pełny numer księgi wieczystej,
5. opcjonalnie znajdź powierzchnię działki lub nieruchomości gruntowej.

Zasady krytyczne:
- Ignoruj numery budynków, lokali, kancelarii, sal sądowych, ulic, udziałów,
  sygnatury Km/Kmp/GKm, numery Dz.U., uchwał, kodów pocztowych i cen.
- Jeśli tekst zawiera kilka numerów działek dla tej samej nieruchomości, zwróć
  pierwszy główny numer działki wskazany jako część licytowanej nieruchomości.
- Pole precinct_or_city ma zawierać wyłącznie nazwę obrębu, miejscowości lub miasta.
  Nigdy nie wpisuj ulicy ani całego adresu.
- municipality uzupełniaj tylko wtedy, gdy gmina jest wyraźnie podana w tekście.
- kw_number uzupełniaj tylko wtedy, gdy widzisz pełny numer KW.
- area_text uzupełniaj tylko wtedy, gdy wprost widzisz powierzchnię działki lub
  nieruchomości gruntowej; ignoruj powierzchnie lokali, budynków i udziałów.
- Nie zgaduj i nie twórz nowych danych. Zwracaj tylko to, co naprawdę wynika z tekstu.
- Odpowiadasz wyłącznie poprawnym JSON-em zgodnym ze schematem.
""".strip()
_STRICT_JSON_SUFFIX = (
    "\nZwracasz dokładnie jeden obiekt JSON. "
    "Bez markdownu, bez bloków ```json, bez komentarza i bez żadnego wstępu."
)


class LLMParcelExtraction(BaseModel):
    """Structured Gemini fallback output."""

    model_config = ConfigDict(extra="forbid")

    kw_number: str | None = Field(default=None)
    parcel_number: str
    precinct_or_city: str
    municipality: str | None = Field(default=None)
    area_text: str | None = Field(default=None)

    @field_validator("kw_number", "municipality", "area_text", mode="before")
    @classmethod
    def _normalize_optional(cls, value: object) -> object:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @field_validator("parcel_number", "precinct_or_city", mode="before")
    @classmethod
    def _normalize_required(cls, value: object) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("value cannot be blank")
        return text


@dataclass(frozen=True)
class FallbackParcelResult:
    parcel_matches: list[ParcelMatch]
    primary_parcel: ParcelMatch | None
    obreb_name: str | None
    municipality: str | None
    area_text: str | None
    llm_used: bool
    llm_extraction: dict[str, Any] | None
    trigger_reason: str | None


def _normalized_ascii(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()


def _is_usable_locality(value: str | None) -> bool:
    normalized = _normalized_ascii(value)
    if not normalized or normalized.isdigit() or len(normalized) < 3:
        return False
    return not any(token in normalized for token in _LOCALITY_SUSPECT_TOKENS)


def should_use_llm_fallback(
    primary_parcel: ParcelMatch | None,
    obreb_name: str | None,
) -> str | None:
    """Return the fallback trigger reason or None when regex is good enough."""
    if primary_parcel is None:
        return "missing_parcel"
    if primary_parcel.confidence < 0.70:
        return "low_regex_confidence"
    if not _is_usable_locality(obreb_name):
        return "missing_locality"
    return None


def _derive_regex_obreb(
    primary_parcel: ParcelMatch | None,
    raw_text: str,
) -> str | None:
    best_obreb, _ = extract_obreb(raw_text)
    if _is_usable_locality(best_obreb):
        if (
            primary_parcel
            and primary_parcel.obreb_raw
            and primary_parcel.confidence >= 0.70
            and _is_usable_locality(primary_parcel.obreb_raw)
            and _normalized_ascii(primary_parcel.obreb_raw) == _normalized_ascii(best_obreb)
        ):
            return primary_parcel.obreb_raw
        return best_obreb

    if (
        primary_parcel
        and primary_parcel.obreb_raw
        and primary_parcel.confidence >= 0.70
        and _is_usable_locality(primary_parcel.obreb_raw)
    ):
        return primary_parcel.obreb_raw
    return best_obreb


def _snippet(text: str, needle: str, radius: int = 80) -> str:
    start_idx = text.find(needle)
    if start_idx < 0:
        return text[: max(60, radius * 2)].replace("\n", " ").strip()
    start = max(0, start_idx - radius)
    end = min(len(text), start_idx + len(needle) + radius)
    return text[start:end].replace("\n", " ").strip()


def _parcel_exists_in_text(text: str, parcel_number: str) -> bool:
    compact_text = re.sub(r"\s+", "", text)
    compact_parcel = re.sub(r"\s+", "", parcel_number)
    return compact_parcel in compact_text


def _phrase_exists_in_text(text: str, phrase: str | None) -> bool:
    if not phrase:
        return False
    return _normalized_ascii(phrase) in _normalized_ascii(text)


def _llm_confidence(text: str, extraction: LLMParcelExtraction) -> float:
    confidence = 0.72
    if _parcel_exists_in_text(text, extraction.parcel_number):
        confidence += 0.06
    if _phrase_exists_in_text(text, extraction.precinct_or_city):
        confidence += 0.04
    if _phrase_exists_in_text(text, extraction.municipality):
        confidence += 0.02
    if extraction.kw_number and _phrase_exists_in_text(text, extraction.kw_number):
        confidence += 0.02
    return round(min(confidence, 0.84), 2)


def _merge_matches(
    regex_matches: list[ParcelMatch],
    llm_match: ParcelMatch | None,
) -> tuple[list[ParcelMatch], ParcelMatch | None]:
    if llm_match is None:
        primary = regex_matches[0] if regex_matches else None
        return regex_matches, primary

    merged: dict[str, ParcelMatch] = {match.numer: match for match in regex_matches}
    existing = merged.get(llm_match.numer)
    if (
        existing is None
        or llm_match.confidence > existing.confidence
        or (not _is_usable_locality(existing.obreb_raw) and _is_usable_locality(llm_match.obreb_raw))
    ):
        merged[llm_match.numer] = llm_match

    ordered = sorted(merged.values(), key=lambda item: (-item.confidence, item.char_offset))
    primary = ordered[0] if ordered else None
    return ordered, primary


def llm_to_parcel_match(
    extraction: LLMParcelExtraction,
    raw_text: str,
) -> ParcelMatch | None:
    """Convert structured LLM output into a synthetic ParcelMatch."""
    if not _parcel_exists_in_text(raw_text, extraction.parcel_number):
        logger.warning(
            "[LLMExtractor] Rejecting hallucinated parcel_number=%r (not found in text)",
            extraction.parcel_number,
        )
        return None

    locality = extraction.precinct_or_city if _is_usable_locality(extraction.precinct_or_city) else None
    offset = raw_text.find(extraction.parcel_number)
    return ParcelMatch(
        raw_value=extraction.parcel_number,
        numer=extraction.parcel_number,
        obreb_raw=locality,
        teryt_obreb=None,
        confidence=_llm_confidence(raw_text, extraction),
        snippet=_snippet(raw_text, extraction.parcel_number),
        char_offset=max(0, offset),
    )


def _schema_for_gemini() -> dict[str, Any]:
    """Return a Gemini-friendly JSON schema.

    Pydantic emits nullable fields as anyOf[string, null], while Gemini's
    response_json_schema is happiest when optionals are simply omitted from the
    required list. We strip null unions and defaults, keeping the shape strict.
    """
    raw = LLMParcelExtraction.model_json_schema()

    def _convert(node: Any) -> Any:
        if isinstance(node, dict):
            if "anyOf" in node:
                variants = [
                    _convert(item)
                    for item in node["anyOf"]
                    if not (isinstance(item, dict) and item.get("type") == "null")
                ]
                if len(variants) == 1:
                    merged = variants[0]
                    if isinstance(merged, dict):
                        for key in ("description", "title"):
                            if key in node and key not in merged:
                                merged[key] = node[key]
                    return merged

            converted: dict[str, Any] = {}
            for key, value in node.items():
                if key in {"default"}:
                    continue
                converted[key] = _convert(value)
            return converted

        if isinstance(node, list):
            return [_convert(item) for item in node]
        return node

    return _convert(raw)


_LLM_RESPONSE_SCHEMA = _schema_for_gemini()


def _strip_code_fences(payload: str) -> str:
    text = payload.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def _coerce_response_to_extraction(response: Any) -> LLMParcelExtraction | None:
    parsed = getattr(response, "parsed", None)
    if isinstance(parsed, LLMParcelExtraction):
        return parsed
    if isinstance(parsed, dict):
        return LLMParcelExtraction.model_validate(parsed)

    raw_text = _strip_code_fences(str(getattr(response, "text", "") or ""))
    if not raw_text:
        return None

    json_start = raw_text.find("{")
    if json_start > 0:
        raw_text = raw_text[json_start:]

    return LLMParcelExtraction.model_validate_json(raw_text)


def _is_retryable_llm_error(exc: Exception) -> bool:
    message = str(exc).upper()
    return any(token in message for token in ("429", "RESOURCE_EXHAUSTED", "503", "UNAVAILABLE", "DEADLINE"))


class LLMExtractor:
    """Gemini fallback extractor using Vertex AI via ADC."""

    def __init__(
        self,
        project_id: str | None = None,
        location: str | None = None,
        model: str | None = None,
    ) -> None:
        self._warned_unavailable = False
        self.project_id = project_id or settings.gcp_project_id or self._discover_project_id()
        self.location = location or settings.gcp_location
        self.model = model or settings.vertex_model
        self.enabled = bool(settings.llm_fallback_enabled and self.project_id and self.model)
        self._aclient: Any | None = None

        if not self.enabled:
            self._warn_unavailable(
                "LLM fallback disabled — missing project/model config or feature flag off."
            )

    async def __aenter__(self) -> LLMExtractor:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.aclose()

    def _discover_project_id(self) -> str:
        try:
            _, project_id = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            return project_id or ""
        except Exception as exc:
            self._warn_unavailable(f"ADC project discovery failed: {exc}")
            return ""

    def _warn_unavailable(self, message: str) -> None:
        if self._warned_unavailable:
            return
        logger.warning("[LLMExtractor] %s", message)
        self._warned_unavailable = True

    async def _get_client(self) -> Any | None:
        if not self.enabled:
            return None
        if self._aclient is None:
            self._aclient = genai.Client(
                vertexai=True,
                project=self.project_id,
                location=self.location,
                http_options=types.HttpOptions(api_version="v1"),
            ).aio
        return self._aclient

    async def aclose(self) -> None:
        if self._aclient is not None:
            await self._aclient.aclose()
            self._aclient = None

    async def extract(
        self,
        raw_text: str,
        *,
        title: str | None = None,
        regex_parcel: ParcelMatch | None = None,
        regex_obreb: str | None = None,
        regex_gmina: str | None = None,
        regex_kw: str | None = None,
        trigger_reason: str | None = None,
    ) -> LLMParcelExtraction | None:
        """Run Gemini structured extraction or return None when unavailable."""
        client = await self._get_client()
        if client is None:
            return None

        prompt_text = raw_text[:_MAX_PROMPT_CHARS]
        for attempt in range(1, _MAX_LLM_ATTEMPTS + 1):
            system_prompt = _SYSTEM_PROMPT if attempt == 1 else f"{_SYSTEM_PROMPT}{_STRICT_JSON_SUFFIX}"
            try:
                response = await client.models.generate_content(
                    model=self.model,
                    contents=prompt_text,
                    config=types.GenerateContentConfig(
                        system_instruction=system_prompt,
                        temperature=settings.llm_temperature,
                        max_output_tokens=settings.llm_max_output_tokens,
                        response_mime_type="application/json",
                        response_schema=LLMParcelExtraction,
                        seed=7,
                        thinking_config=types.ThinkingConfig(thinking_budget=0),
                    ),
                )
                extraction = _coerce_response_to_extraction(response)
                if extraction is None:
                    logger.warning(
                        "[LLMExtractor] Empty response for trigger=%s (attempt %d/%d)",
                        trigger_reason,
                        attempt,
                        _MAX_LLM_ATTEMPTS,
                    )
                else:
                    logger.info(
                        "[LLMExtractor] model=%s trigger=%s parcel=%s locality=%s municipality=%s area=%s",
                        self.model,
                        trigger_reason or "manual",
                        extraction.parcel_number,
                        extraction.precinct_or_city,
                        extraction.municipality or "—",
                        extraction.area_text or "—",
                    )
                    return extraction
            except ValidationError as exc:
                logger.warning(
                    "[LLMExtractor] Structured output validation failed (attempt %d/%d): %s",
                    attempt,
                    _MAX_LLM_ATTEMPTS,
                    exc,
                )
            except Exception as exc:
                if attempt >= _MAX_LLM_ATTEMPTS or not _is_retryable_llm_error(exc):
                    logger.warning("[LLMExtractor] Gemini fallback failed: %s", exc)
                    return None
                logger.warning(
                    "[LLMExtractor] Retryable Gemini error (attempt %d/%d): %s",
                    attempt,
                    _MAX_LLM_ATTEMPTS,
                    exc,
                )

            if attempt < _MAX_LLM_ATTEMPTS:
                await asyncio.sleep(0.75 * attempt)

        return None


async def extract_with_fallback(
    raw_text: str,
    *,
    title: str | None = None,
    raw_gmina: str | None = None,
    raw_kw: str | None = None,
    llm_extractor: LLMExtractor | None = None,
) -> FallbackParcelResult:
    """Run regex first and optionally enrich with Gemini fallback."""
    regex_matches = extract_parcel_ids(raw_text)
    primary_parcel = regex_matches[0] if regex_matches else None
    obreb_name = _derive_regex_obreb(primary_parcel, raw_text)
    municipality = raw_gmina
    trigger_reason = should_use_llm_fallback(primary_parcel, obreb_name)
    llm_payload: dict[str, Any] | None = None

    if trigger_reason and llm_extractor is not None:
        llm_extraction = await llm_extractor.extract(
            raw_text,
            title=title,
            regex_parcel=primary_parcel,
            regex_obreb=obreb_name,
            regex_gmina=raw_gmina,
            regex_kw=raw_kw,
            trigger_reason=trigger_reason,
        )
        if llm_extraction is not None:
            llm_match = llm_to_parcel_match(llm_extraction, raw_text)
            regex_matches, primary_parcel = _merge_matches(regex_matches, llm_match)
            if llm_match and not _is_usable_locality(obreb_name):
                obreb_name = llm_extraction.precinct_or_city
            if not municipality and llm_extraction.municipality:
                municipality = llm_extraction.municipality
            llm_payload = {
                **llm_extraction.model_dump(),
                "provider": "vertex_ai",
                "model": llm_extractor.model,
                "trigger_reason": trigger_reason,
                "accepted": llm_match is not None,
                "llm_confidence": llm_match.confidence if llm_match else 0.0,
            }
            return FallbackParcelResult(
                parcel_matches=regex_matches,
                primary_parcel=primary_parcel,
                obreb_name=obreb_name,
                municipality=municipality,
                area_text=llm_extraction.area_text,
                llm_used=True,
                llm_extraction=llm_payload,
                trigger_reason=trigger_reason,
            )

    return FallbackParcelResult(
        parcel_matches=regex_matches,
        primary_parcel=primary_parcel,
        obreb_name=obreb_name,
        municipality=municipality,
        area_text=None,
        llm_used=False,
        llm_extraction=llm_payload,
        trigger_reason=trigger_reason,
    )
