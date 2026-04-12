"""Public Kraków MSIP parcel resolver for difficult urban listings.

This resolver is intentionally narrow:
  - it targets Kraków EGiB layers exposed by the public MSIP ArcGIS service,
  - it expects the official komornik notice to contain both:
      * jednostka ewidencyjna (e.g. Podgórze)
      * explicit obręb number (e.g. 94)
  - it resolves parcel geometries directly from the public parcel layer,
    bypassing ULDK when the ULDK urban fallback is too ambiguous or slow.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import httpx
from shapely.geometry import MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry
from shapely.validation import make_valid
from shapely.wkb import dumps as shapely_wkb_dumps

from app.services.uldk import GeometryValidationError, ULDKParcel

logger = logging.getLogger(__name__)

_MSIP_TIMEOUT_S = 20.0
_KRAKOW_JE_LAYER = (
    "https://msip.um.krakow.pl/arcgis/rest/services/Obserwatorium/"
    "GI_EGIB_GREY/MapServer/35/query"
)
_KRAKOW_OBREB_LAYER = (
    "https://msip.um.krakow.pl/arcgis/rest/services/Obserwatorium/"
    "GI_EGIB_GREY/MapServer/34/query"
)
_KRAKOW_PARCEL_LAYER = (
    "https://msip.um.krakow.pl/arcgis/rest/services/Obserwatorium/"
    "GI_EGIB_GREY/MapServer/1/query"
)
_RE_JE_REGION = re.compile(
    r"jedn(?:ostka)?\.?\s*ewid(?:encyjna)?\.?\s+(?P<unit>[A-ZĄĆĘŁŃÓŚŹŻ]"
    r"[A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż.\- ]+?)"
    r"(?:,|\s+)\s*obr[ęe]b(?:ie)?(?:\s+nr)?\s*(?P<region>\d{1,4})\b",
    re.IGNORECASE,
)
_RE_CONTEXTUAL_PARCELS = re.compile(
    r"działki?\s+nr\s+(?P<body>[\d/\s,ioraz]+?)(?:,|\s+)obr",
    re.IGNORECASE,
)
_KRAKOW_JE_PREFIX = {
    "PODGORZE": "P",
    "NOWA HUTA": "NH",
    "KROWODRZA": "K",
    "SRODMIESCIE": "S",
}


@dataclass(frozen=True)
class KrakowNoticeContext:
    unit_name: str
    unit_code: str
    region_code: str
    region_name: str


def _ascii_key(value: str) -> str:
    return (
        value.replace("ł", "l")
        .replace("Ł", "L")
        .replace("ó", "o")
        .replace("Ó", "O")
        .replace("ś", "s")
        .replace("Ś", "S")
        .replace("ą", "a")
        .replace("Ą", "A")
        .replace("ę", "e")
        .replace("Ę", "E")
        .replace("ć", "c")
        .replace("Ć", "C")
        .replace("ń", "n")
        .replace("Ń", "N")
        .replace("ż", "z")
        .replace("Ż", "Z")
        .replace("ź", "z")
        .replace("Ź", "Z")
        .upper()
        .strip()
    )


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def extract_krakow_notice_context(text: str | None) -> tuple[str, str] | None:
    if not text:
        return None
    match = _RE_JE_REGION.search(text)
    if not match:
        return None

    unit_name = re.sub(r"\s+", " ", match.group("unit")).strip(" ,.;")
    region_code = match.group("region").zfill(4)
    if not unit_name:
        return None
    return unit_name, region_code


def _derive_region_name(unit_name: str, region_code: str) -> str | None:
    prefix = _KRAKOW_JE_PREFIX.get(_ascii_key(unit_name))
    if prefix is None:
        return None
    return f"{prefix}-{int(region_code)}"


def extract_contextual_parcel_numbers(text: str | None) -> tuple[str, ...]:
    if not text:
        return ()
    match = _RE_CONTEXTUAL_PARCELS.search(text)
    if not match:
        return ()
    values: dict[str, None] = {}
    for token in re.findall(r"\b\d{1,5}(?:/\d{1,4})?\b", match.group("body")):
        values.setdefault(token, None)
    return tuple(values.keys())


def _esri_rings_to_multipolygon(geometry: dict[str, Any]) -> MultiPolygon:
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise GeometryValidationError("Kraków MSIP returned no polygon rings")

    # Parcel geometries in the Kraków service are simple polygons in practice.
    polygon = Polygon(rings[0], holes=rings[1:] or None)
    valid = make_valid(polygon)
    if valid.is_empty:
        raise GeometryValidationError("Kraków MSIP geometry became empty after validation")
    if isinstance(valid, Polygon):
        return MultiPolygon([valid])
    if isinstance(valid, MultiPolygon):
        return valid
    raise GeometryValidationError(
        f"Kraków MSIP geometry is not polygonal after validation: {valid.geom_type}"
    )


class KrakowMsipResolver:
    """Resolve Kraków parcels from the public MSIP EGiB layers."""

    def __init__(self, timeout_s: float = _MSIP_TIMEOUT_S) -> None:
        self.timeout_s = timeout_s

    async def resolve_from_notice(
        self,
        *,
        plain_text: str,
        parcel_numbers: tuple[str, ...],
    ) -> list[ULDKParcel]:
        context = await self._resolve_context(plain_text)
        if context is None:
            return []

        contextual_parcels = extract_contextual_parcel_numbers(plain_text)
        candidate_numbers = contextual_parcels or parcel_numbers

        resolved: list[ULDKParcel] = []
        seen_ids: set[str] = set()
        for parcel_number in candidate_numbers:
            parcel = await self._resolve_parcel(
                parcel_number=parcel_number,
                context=context,
            )
            if parcel is None or parcel.identyfikator in seen_ids:
                continue
            resolved.append(parcel)
            seen_ids.add(parcel.identyfikator)

        return resolved

    async def _resolve_context(self, plain_text: str) -> KrakowNoticeContext | None:
        extracted = extract_krakow_notice_context(plain_text)
        if extracted is None:
            return None

        unit_name, region_code = extracted
        unit_code = await self._lookup_unit_code(unit_name)
        if unit_code is None:
            return None

        region_name = await self._lookup_region_name(unit_name, region_code)
        if region_name is None:
            region_name = _derive_region_name(unit_name, region_code)
        if region_name is None:
            return None

        return KrakowNoticeContext(
            unit_name=unit_name,
            unit_code=unit_code,
            region_code=region_code,
            region_name=region_name,
        )

    async def _lookup_unit_code(self, unit_name: str) -> str | None:
        where = f"je_nazwa='{_escape_sql_literal(unit_name)}'"
        payload = await self._query(
            _KRAKOW_JE_LAYER,
            where=where,
            out_fields="je_nr,je_nazwa",
            return_geometry=False,
        )
        features = payload.get("features") or []
        if len(features) != 1:
            return None
        attrs = features[0].get("attributes") or {}
        value = attrs.get("je_nr")
        return str(value).strip() if value else None

    async def _lookup_region_name(self, unit_name: str, region_code: str) -> str | None:
        where = (
            f"je_nazwa='{_escape_sql_literal(unit_name)}' "
            f"AND obr_nr='{_escape_sql_literal(region_code)}'"
        )
        payload = await self._query(
            _KRAKOW_OBREB_LAYER,
            where=where,
            out_fields="je_nr,je_nazwa,obr_nr,obr_nazwa",
            return_geometry=False,
        )
        features = payload.get("features") or []
        if len(features) != 1:
            return None
        attrs = features[0].get("attributes") or {}
        value = attrs.get("obr_nazwa")
        return str(value).strip() if value else None

    async def _resolve_parcel(
        self,
        *,
        parcel_number: str,
        context: KrakowNoticeContext,
    ) -> ULDKParcel | None:
        where = (
            f"nr='{_escape_sql_literal(parcel_number)}' "
            f"AND je_nr='{_escape_sql_literal(context.unit_code)}' "
            f"AND obr_nazwa='{_escape_sql_literal(context.region_name)}'"
        )
        payload = await self._query(
            _KRAKOW_PARCEL_LAYER,
            where=where,
            out_fields="nr,je_nr,je_nazwa,obr_nazwa,dzk_ident,pow_ewid",
            return_geometry=True,
            out_sr=2180,
        )
        features = payload.get("features") or []
        if len(features) != 1:
            return None

        feature = features[0]
        attrs = feature.get("attributes") or {}
        geom = feature.get("geometry") or {}
        shape = _esri_rings_to_multipolygon(geom)

        msip_identifier = str(attrs.get("dzk_ident") or "").strip()
        if not msip_identifier:
            return None

        commune = context.unit_code.replace("_", "")
        region = context.region_code
        numer = str(attrs.get("nr") or parcel_number).strip()
        identyfikator = f"{commune}{int(region):02d}.{numer}"
        area = Decimal(str(round(shape.area, 2)))
        wkb_hex = shapely_wkb_dumps(shape, hex=True)

        return ULDKParcel(
            identifier=msip_identifier,
            voivodeship=commune[:2],
            county=commune[:4],
            commune=commune,
            region=region,
            parcel=numer,
            teryt_wojewodztwo=commune[:2],
            teryt_powiat=commune[:4],
            teryt_gmina=commune,
            teryt_obreb=f"{commune}{int(region):02d}",
            numer_dzialki=numer,
            identyfikator=identyfikator,
            geom_shape=shape,
            geom_wkb_hex=wkb_hex,
            area_m2=area,
            was_made_valid=False,
            fetched_at=datetime.now(timezone.utc),
        )

    async def _query(
        self,
        url: str,
        *,
        where: str,
        out_fields: str,
        return_geometry: bool,
        out_sr: int | None = None,
    ) -> dict[str, Any]:
        params = {
            "f": "pjson",
            "where": where,
            "returnGeometry": "true" if return_geometry else "false",
            "outFields": out_fields,
        }
        if out_sr is not None:
            params["outSR"] = str(out_sr)

        async with httpx.AsyncClient(
            timeout=self.timeout_s,
            follow_redirects=True,
        ) as client:
            response = await client.get(url, params=params)
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise GeometryValidationError(
                f"Kraków MSIP query failed for {url}: {payload['error']}"
            )
        return payload
