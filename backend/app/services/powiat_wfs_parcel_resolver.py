"""Public powiat WFS parcel resolvers for difficult Małopolskie listings.

These sources expose parcel geometries directly via public WFS, which makes
them ideal fallbacks when ULDK is ambiguous or the listing only provides a
city/locality plus parcel numbers.

Current coverage:
  - Zakopane via Tatrzański webewid WFS
  - Andrychów via Wadowicki webewid WFS
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal

import httpx
from lxml import etree
from shapely.geometry import MultiPolygon, Polygon
from shapely.validation import make_valid
from shapely.wkb import dumps as shapely_wkb_dumps

from app.services.uldk import GeometryValidationError, ULDKParcel

logger = logging.getLogger(__name__)

_TIMEOUT_S = 20.0
_RE_MULTI_SPACE = re.compile(r"\s+")
_RE_ASCII_JUNK = re.compile(r"[^a-z0-9]+")

_ZAKOPANE_WFS_URL = "https://tatrzanski-wms.webewid.pl/iip/ows"
_ANDRYCHOW_WFS_URL = "https://wadowicki.webewid.pl:20443/iip/ows"
_NOWY_TARG_WFS_URL = "https://nowotarski.geoportal2.pl/map/geoportal/wfse.php"
_CHRZANOW_WFS_URL = "https://chrzanowski.webewid.pl:22443/iip/ows"


def _ascii_key(value: str | None) -> str:
    if not value:
        return ""
    text = (
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
        .lower()
    )
    return _RE_ASCII_JUNK.sub(" ", text).strip()


def _build_feature_filter_20(*clauses: tuple[str, str]) -> str:
    parts = [
        (
            "<fes:PropertyIsEqualTo>"
            f"<fes:ValueReference>{field}</fes:ValueReference>"
            f"<fes:Literal>{value}</fes:Literal>"
            "</fes:PropertyIsEqualTo>"
        )
        for field, value in clauses
    ]
    if len(parts) == 1:
        body = parts[0]
    else:
        body = "<fes:And>" + "".join(parts) + "</fes:And>"
    return f"<fes:Filter xmlns:fes='http://www.opengis.net/fes/2.0'>{body}</fes:Filter>"


def _build_feature_filter_11(*clauses: tuple[str, str]) -> str:
    parts = [
        (
            "<ogc:PropertyIsEqualTo>"
            f"<ogc:PropertyName>{field}</ogc:PropertyName>"
            f"<ogc:Literal>{value}</ogc:Literal>"
            "</ogc:PropertyIsEqualTo>"
        )
        for field, value in clauses
    ]
    if len(parts) == 1:
        body = parts[0]
    else:
        body = "<ogc:And>" + "".join(parts) + "</ogc:And>"
    return f"<ogc:Filter xmlns:ogc='http://www.opengis.net/ogc'>{body}</ogc:Filter>"


def _text_content(element: etree._Element, local_name: str) -> str:
    for child in element:
        if etree.QName(child).localname == local_name:
            return _RE_MULTI_SPACE.sub(" ", "".join(child.itertext())).strip()
    return ""


def _coords_from_poslist(poslist: str) -> list[tuple[float, float]]:
    values = [float(part) for part in poslist.split() if part.strip()]
    if len(values) < 6 or len(values) % 2:
        raise GeometryValidationError(f"Invalid GML posList length: {len(values)}")
    return [(values[idx], values[idx + 1]) for idx in range(0, len(values), 2)]


def _geometry_from_feature(feature: etree._Element) -> MultiPolygon:
    polygons: list[Polygon] = []
    for polygon_node in feature.xpath(".//*[local-name()='Polygon']"):
        exterior_nodes = polygon_node.xpath(
            ".//*[local-name()='exterior']//*[local-name()='posList']/text()"
        )
        if not exterior_nodes:
            continue
        exterior = _coords_from_poslist(exterior_nodes[0])
        holes = [
            _coords_from_poslist(value)
            for value in polygon_node.xpath(
                ".//*[local-name()='interior']//*[local-name()='posList']/text()"
            )
        ]
        polygons.append(Polygon(exterior, holes or None))

    if not polygons:
        raise GeometryValidationError("Powiat WFS returned no polygon geometry")

    valid = make_valid(MultiPolygon(polygons))
    if valid.is_empty:
        raise GeometryValidationError("Powiat WFS geometry became empty after validation")
    if isinstance(valid, Polygon):
        return MultiPolygon([valid])
    if isinstance(valid, MultiPolygon):
        return valid
    raise GeometryValidationError(
        f"Powiat WFS geometry is not polygonal after validation: {valid.geom_type}"
    )


def _build_uldk_parcel(
    *,
    identifier: str,
    parcel_number: str,
    region_code: str,
    shape: MultiPolygon,
) -> ULDKParcel:
    parts = identifier.split(".")
    if len(parts) < 3:
        raise GeometryValidationError(f"Cannot parse WFS parcel identifier: {identifier}")

    commune = parts[0].replace("_", "").zfill(7)
    region = region_code.zfill(4)
    try:
        region_suffix = str(int(region)).zfill(2)
    except ValueError:
        region_suffix = region[-2:]

    identyfikator = f"{commune}{region_suffix}.{parcel_number}"
    wkb_hex = shapely_wkb_dumps(shape, hex=True)
    area = Decimal(str(round(shape.area, 2)))

    return ULDKParcel(
        identifier=identifier,
        voivodeship=commune[:2],
        county=commune[:4],
        commune=commune,
        region=region,
        parcel=parcel_number,
        teryt_wojewodztwo=commune[:2],
        teryt_powiat=commune[:4],
        teryt_gmina=commune,
        teryt_obreb=f"{commune}{region_suffix}",
        numer_dzialki=parcel_number,
        identyfikator=identyfikator,
        geom_shape=shape,
        geom_wkb_hex=wkb_hex,
        area_m2=area,
        was_made_valid=False,
        fetched_at=datetime.now(timezone.utc),
    )


@dataclass(frozen=True)
class PowiatFeature:
    identifier: str
    parcel_number: str
    region_name: str
    region_code: str
    commune_name: str
    shape: MultiPolygon


class PowiatWfsParcelResolver:
    """Resolve selected difficult parcels directly from public powiat WFS."""

    def __init__(self, timeout_s: float = _TIMEOUT_S) -> None:
        self.timeout_s = timeout_s

    async def resolve(
        self,
        *,
        raw_obreb: str | None,
        raw_gmina: str | None,
        plain_text: str,
        parcel_numbers: tuple[str, ...],
    ) -> list[ULDKParcel]:
        raw_obreb_key = _ascii_key(raw_obreb)
        raw_gmina_key = _ascii_key(raw_gmina)
        locality_key = raw_gmina_key or raw_obreb_key
        if locality_key in {"zakopane", "zakopanem"}:
            return await self._resolve_zakopane(plain_text=plain_text, parcel_numbers=parcel_numbers)
        if locality_key in {"andrychow", "andrychowie"}:
            return await self._resolve_andrychow(parcel_numbers=parcel_numbers)
        if raw_obreb_key == "zubrzyca gorna" or raw_gmina_key == "jablonka":
            return await self._resolve_zubrzyca_gorna(parcel_numbers=parcel_numbers)
        if raw_obreb_key in {"kroczymiech", "chrzanow"} or raw_gmina_key.startswith("chrzanow"):
            return await self._resolve_chrzanow(parcel_numbers=parcel_numbers)
        return []

    async def _resolve_zakopane(
        self,
        *,
        plain_text: str,
        parcel_numbers: tuple[str, ...],
    ) -> list[ULDKParcel]:
        region_code_match = re.search(r"\bobr(?:ę|e)?b(?:ie)?(?:\s+nr)?\s*(\d{1,4})\b", plain_text, re.I)
        region_code = region_code_match.group(1).zfill(4) if region_code_match else None

        feature_map: dict[str, list[PowiatFeature]] = {}
        for parcel_number in parcel_numbers:
            feature_map[parcel_number] = await self._query_zakopane(parcel_number)

        if region_code is None:
            region_code = self._infer_common_region_code(feature_map)
        if region_code is None:
            return []

        resolved: list[ULDKParcel] = []
        seen_ids: set[str] = set()
        for parcel_number in parcel_numbers:
            features = feature_map.get(parcel_number, [])
            exact = [
                feature for feature in features
                if _ascii_key(feature.commune_name) == "zakopane"
                and feature.region_code == region_code
                and feature.parcel_number == parcel_number
            ]
            if len(exact) != 1:
                continue
            parcel = _build_uldk_parcel(
                identifier=exact[0].identifier,
                parcel_number=exact[0].parcel_number,
                region_code=exact[0].region_code,
                shape=exact[0].shape,
            )
            if parcel.identyfikator not in seen_ids:
                resolved.append(parcel)
                seen_ids.add(parcel.identyfikator)
        return resolved

    def _infer_common_region_code(
        self,
        feature_map: dict[str, list[PowiatFeature]],
    ) -> str | None:
        region_sets = [
            {
                feature.region_code
                for feature in features
                if _ascii_key(feature.commune_name) == "zakopane"
            }
            for features in feature_map.values()
            if features
        ]
        if len(region_sets) < 2:
            return None

        common = set.intersection(*region_sets)
        if len(common) == 1:
            return next(iter(common))

        support = Counter(
            region_code
            for region_set in region_sets
            for region_code in region_set
        )
        if not support:
            return None

        ranked = support.most_common(2)
        best_code, best_count = ranked[0]
        second_count = ranked[1][1] if len(ranked) > 1 else 0
        if best_count >= 3 and best_count > second_count:
            return best_code
        return None

    async def _resolve_andrychow(
        self,
        *,
        parcel_numbers: tuple[str, ...],
    ) -> list[ULDKParcel]:
        resolved: list[ULDKParcel] = []
        seen_ids: set[str] = set()
        commune_candidates = (
            "andrychow miasto",
            "andrychow obszar wiejski",
        )
        for parcel_number in parcel_numbers:
            features = await self._query_andrychow(parcel_number)
            exact = [
                feature for feature in features
                if _ascii_key(feature.commune_name) in commune_candidates
                and feature.parcel_number == parcel_number
            ]
            if len(exact) != 1:
                continue
            parcel = _build_uldk_parcel(
                identifier=exact[0].identifier,
                parcel_number=exact[0].parcel_number,
                region_code=exact[0].region_code,
                shape=exact[0].shape,
            )
            if parcel.identyfikator not in seen_ids:
                resolved.append(parcel)
                seen_ids.add(parcel.identyfikator)
        return resolved

    async def _resolve_zubrzyca_gorna(
        self,
        *,
        parcel_numbers: tuple[str, ...],
    ) -> list[ULDKParcel]:
        resolved: list[ULDKParcel] = []
        seen_ids: set[str] = set()
        for parcel_number in parcel_numbers:
            features = await self._query_zubrzyca_gorna(parcel_number)
            exact = [
                feature for feature in features
                if _ascii_key(feature.region_name) == "zubrzyca gorna"
                and _ascii_key(feature.commune_name) == "jablonka"
                and feature.parcel_number == parcel_number
            ]
            if len(exact) != 1:
                continue
            parcel = _build_uldk_parcel(
                identifier=exact[0].identifier,
                parcel_number=exact[0].parcel_number,
                region_code=exact[0].region_code,
                shape=exact[0].shape,
            )
            if parcel.identyfikator not in seen_ids:
                resolved.append(parcel)
                seen_ids.add(parcel.identyfikator)
        return resolved

    async def _resolve_chrzanow(
        self,
        *,
        parcel_numbers: tuple[str, ...],
    ) -> list[ULDKParcel]:
        resolved: list[ULDKParcel] = []
        seen_ids: set[str] = set()
        for parcel_number in parcel_numbers:
            features = await self._query_chrzanow(parcel_number)
            exact = [
                feature for feature in features
                if _ascii_key(feature.region_name) == "chrzanow"
                and _ascii_key(feature.commune_name) == "chrzanow miasto"
                and feature.parcel_number == parcel_number
            ]
            if len(exact) != 1:
                continue
            parcel = _build_uldk_parcel(
                identifier=exact[0].identifier,
                parcel_number=exact[0].parcel_number,
                region_code=exact[0].region_code,
                shape=exact[0].shape,
            )
            if parcel.identyfikator not in seen_ids:
                resolved.append(parcel)
                seen_ids.add(parcel.identyfikator)
        return resolved

    async def _query_zakopane(self, parcel_number: str) -> list[PowiatFeature]:
        payload = await self._fetch(
            _ZAKOPANE_WFS_URL,
            params={
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeNames": "ms:dzialki",
                "filter": _build_feature_filter_20(
                    ("NAZWA_GMINY", "Zakopane"),
                    ("NUMER_DZIALKI", parcel_number),
                ),
                "count": "25",
                "srsName": "EPSG:2180",
            },
        )
        return self._parse_features(payload)

    async def _query_andrychow(self, parcel_number: str) -> list[PowiatFeature]:
        payload = await self._fetch(
            _ANDRYCHOW_WFS_URL,
            params={
                "service": "WFS",
                "version": "1.1.0",
                "request": "GetFeature",
                "typeName": "ms:dzialki",
                "filter": _build_feature_filter_11(
                    ("NUMER_DZIALKI", parcel_number),
                ),
                "maxFeatures": "10",
                "srsName": "EPSG:2180",
            },
        )
        return self._parse_features(payload)

    async def _query_zubrzyca_gorna(self, parcel_number: str) -> list[PowiatFeature]:
        payload = await self._fetch(
            _NOWY_TARG_WFS_URL,
            params={
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeNames": "ewns:dzialki",
                "filter": _build_feature_filter_20(
                    ("NAZWA_OBREBU", "ZUBRZYCA GÓRNA"),
                    ("NAZWA_GMINY", "JABŁONKA"),
                    ("NUMER_DZIALKI", parcel_number),
                ),
                "count": "5",
                "srsName": "EPSG:2180",
            },
        )
        return self._parse_features(payload)

    async def _query_chrzanow(self, parcel_number: str) -> list[PowiatFeature]:
        payload = await self._fetch(
            _CHRZANOW_WFS_URL,
            params={
                "service": "WFS",
                "version": "1.1.0",
                "request": "GetFeature",
                "typeName": "ms:dzialki",
                "filter": _build_feature_filter_11(
                    ("NUMER_DZIALKI", parcel_number),
                    ("NAZWA_GMINY", "Chrzanów - miasto"),
                ),
                "maxFeatures": "5",
                "srsName": "EPSG:2180",
            },
        )
        return self._parse_features(payload)

    async def _fetch(self, url: str, *, params: dict[str, str]) -> str:
        async with httpx.AsyncClient(
            timeout=self.timeout_s,
            follow_redirects=True,
            verify=False,
        ) as client:
            response = await client.get(url, params=params)
        response.raise_for_status()
        return response.text

    def _parse_features(self, payload: str) -> list[PowiatFeature]:
        try:
            root = etree.fromstring(payload.encode("utf-8"))
        except Exception as exc:
            raise GeometryValidationError(f"Cannot parse powiat WFS XML payload: {exc}") from exc

        features: list[PowiatFeature] = []
        for feature in root.xpath(".//*[local-name()='dzialki']"):
            identifier = _text_content(feature, "ID_DZIALKI")
            parcel_number = _text_content(feature, "NUMER_DZIALKI")
            region_code = _text_content(feature, "NUMER_OBREBU").zfill(4)
            if not identifier or not parcel_number or not region_code:
                continue
            features.append(
                PowiatFeature(
                    identifier=identifier,
                    parcel_number=parcel_number,
                    region_name=_text_content(feature, "NAZWA_OBREBU"),
                    region_code=region_code,
                    commune_name=_text_content(feature, "NAZWA_GMINY"),
                    shape=_geometry_from_feature(feature),
                )
            )
        return features
