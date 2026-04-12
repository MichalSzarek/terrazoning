"""Official licytacje.komornik.pl notice enrichment helpers.

Purpose:
  - fetch the public notice HTML from the current portal's backend,
  - extract safer parcel hints than the SSR/search-page excerpt gives us,
  - provide evidence-backed overrides for edge cases where the current portal
    truncates cadastral detail but older public notice copies exposed it.

This is intentionally narrow and conservative:
  - we only trust public notice content from the official item-back endpoint,
  - we only use archived KW overrides when we have a confirmed public match,
  - we never fabricate parcel numbers from the LLM or from ambiguous city names.
"""

from __future__ import annotations

import html
import logging
import re
from dataclasses import dataclass

import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)

_RE_NOTICE_ID = re.compile(r"/wyszukiwarka/obwieszczenia-o-licytacji/(?P<id>\d+)(?:/|$)")
_RE_HTML_TAG = re.compile(r"<[^>]+>")
_RE_MULTI_SPACE = re.compile(r"\s+")
_RE_SLASH_PARCEL = re.compile(r"\b\d{1,5}/\d{1,4}(?:/\d{1,4})*\b")
_RE_KW = re.compile(r"\b[A-Z]{2}\d[A-Z]/\d{8}/\d\b")


@dataclass(frozen=True)
class KWParcelOverride:
    """Evidence-backed manual fallback for a specific land-registry number."""

    obreb_name: str
    parcel_numbers: tuple[str, ...]
    source_note: str


@dataclass(frozen=True)
class NoticeResolutionHint:
    """Best parcel/locality hint we can derive from official notice sources."""

    notice_id: int | None
    source: str
    obreb_name: str | None
    parcel_numbers: tuple[str, ...]
    plain_text: str
    kw_number: str | None


# Confirmed public edge-case overrides.
# 2026-04-08: KW GL1G/00023264/2 appears on the current portal without parcel
# numbers, but older publicly indexed notice copies exposed:
#   obręb Żerniki
#   działki 15/36, 15/45, 15/58, 15/59, 15/60, 15/62, 15/63, 15/64
# The current two live listings (32027, 32030) are both 20/1320 shares in the
# same KW, so this override lets the resolver recover the real parcels instead
# of chasing the false-positive "44" from zip code 44-105.
_KW_PARCEL_OVERRIDES: dict[str, KWParcelOverride] = {
    "GL1G/00023264/2": KWParcelOverride(
        obreb_name="Żerniki",
        parcel_numbers=(
            "15/36",
            "15/45",
            "15/58",
            "15/59",
            "15/60",
            "15/62",
            "15/63",
            "15/64",
        ),
        source_note=(
            "public archived notice copies indexed on 2026-04-08 for "
            "KW GL1G/00023264/2"
        ),
    ),
}


def normalize_kw_number(value: str | None) -> str | None:
    if not value:
        return None
    match = _RE_KW.search(value.upper().replace(" ", ""))
    return match.group(0) if match else None


def extract_notice_id_from_source_url(source_url: str | None) -> int | None:
    if not source_url:
        return None
    match = _RE_NOTICE_ID.search(source_url)
    return int(match.group("id")) if match else None


def _html_to_text(markup: str) -> str:
    text = _RE_HTML_TAG.sub(" ", markup)
    text = html.unescape(text)
    return _RE_MULTI_SPACE.sub(" ", text).strip()


def _parcel_numbers_from_text(text: str) -> tuple[str, ...]:
    seen: dict[str, None] = {}
    for numer in _RE_SLASH_PARCEL.findall(text):
        seen.setdefault(numer, None)
    return tuple(seen.keys())


class KomornikNoticeEnricher:
    """Fetches the official notice HTML published for a given listing."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        basic_auth: str | None = None,
        timeout_s: float = 20.0,
    ) -> None:
        self.base_url = base_url or settings.komornik_notice_api_base_url.rstrip("/")
        self.basic_auth = basic_auth or settings.komornik_notice_basic_auth
        self.timeout_s = timeout_s

    async def fetch_notice_hint(
        self,
        *,
        source_url: str | None,
        raw_kw: str | None,
    ) -> NoticeResolutionHint | None:
        notice_id = extract_notice_id_from_source_url(source_url)
        plain_text = ""
        kw_number = normalize_kw_number(raw_kw)
        official_parcels: tuple[str, ...] = ()

        if notice_id is not None:
            notice = await self._fetch_notice(notice_id)
            if notice is not None:
                plain_text = notice.plain_text
                official_parcels = _parcel_numbers_from_text(plain_text)
                kw_number = kw_number or normalize_kw_number(plain_text)

        override = _KW_PARCEL_OVERRIDES.get(kw_number or "")
        if override is not None:
            return NoticeResolutionHint(
                notice_id=notice_id,
                source=f"archived_kw_override:{override.source_note}",
                obreb_name=override.obreb_name,
                parcel_numbers=override.parcel_numbers,
                plain_text=plain_text,
                kw_number=kw_number,
            )

        if official_parcels:
            return NoticeResolutionHint(
                notice_id=notice_id,
                source="official_notice",
                obreb_name=None,
                parcel_numbers=official_parcels,
                plain_text=plain_text,
                kw_number=kw_number,
            )

        if notice_id is None or not plain_text:
            return None

        return NoticeResolutionHint(
            notice_id=notice_id,
            source="official_notice",
            obreb_name=None,
            parcel_numbers=(),
            plain_text=plain_text,
            kw_number=kw_number,
        )

    async def _fetch_notice(self, notice_id: int) -> NoticeResolutionHint | None:
        url = f"{self.base_url}/{notice_id}"
        headers = {
            "Authorization": self.basic_auth,
            "Accept-Language": "pl",
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout_s, follow_redirects=True) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            logger.warning(
                "[KomornikNoticeEnricher] notice=%s fetch failed: %s",
                notice_id,
                exc,
            )
            return None

        obj = payload.get("object") if isinstance(payload, dict) else None
        if not isinstance(obj, dict):
            return None

        content = obj.get("content")
        if not isinstance(content, str) or not content.strip():
            return None

        plain_text = _html_to_text(content)
        return NoticeResolutionHint(
            notice_id=notice_id,
            source="official_notice",
            obreb_name=None,
            parcel_numbers=_parcel_numbers_from_text(plain_text),
            plain_text=plain_text,
            kw_number=normalize_kw_number(plain_text),
        )
