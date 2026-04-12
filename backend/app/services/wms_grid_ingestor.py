"""Parcel-centric WMS zoning ingestion for municipalities without public vector WFS.

This is the fallback path for sources that expose meaningful zoning information only
through WMS GetFeatureInfo. Instead of ingesting municipality-wide zoning polygons,
we sample the WMS inside already resolved parcel geometries and build approximate,
parcel-clipped planning zones from the sampled cells.

Trade-off:
    - lower geometric fidelity than a real vector WFS layer
    - but materially better than having zero MPZP coverage for a municipality
    - coverage_pct in DeltaEngine becomes an approximation derived from the sampled grid
"""

from __future__ import annotations

import asyncio
from html import unescape
import logging
import re
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

import httpx
from geoalchemy2.shape import to_shape
from pyproj import Transformer
from shapely.geometry import MultiPolygon, Point, Polygon, box
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
from shapely.validation import make_valid
from sqlalchemy import select

from app.core.database import AsyncSessionLocal
from app.models.silver import Dzialka
from app.services.wfs_downloader import WFSClient, WFSFeature, WFSIngestReport

logger = logging.getLogger(__name__)

_WMS_TIMEOUT_S = 30.0
_DEFAULT_SAMPLE_GRID = 5
_DEFAULT_HALFSPAN_M = 6.0


@dataclass
class WMSFeatureInfoHit:
    """Parsed zoning hit returned by WMS GetFeatureInfo."""

    designation: str
    description: str | None
    plan_name: str
    uchwala_nr: str | None
    raw_payload: dict[str, str]


@dataclass
class WMSGridConfig:
    """Operational configuration for the WMS sampling source."""

    wms_url: str
    layer_name: str
    teryt_gmina: str
    plan_type: str = "mpzp"
    source_srid: int = 2180
    version: str = "1.3.0"
    info_format: str = "text/plain"
    styles: str = "default"
    sample_grid: int = _DEFAULT_SAMPLE_GRID
    point_halfspan_m: float = _DEFAULT_HALFSPAN_M
    swap_bbox_axes: bool = False
    parser_name: str = "ruda_plaintext"
    query_url_template: str | None = None
    fallback_designation: str | None = None
    fallback_description: str | None = None


def _normalize_designation(raw: str | None) -> str | None:
    if not raw:
        return None
    value = raw.strip().upper()
    if not value:
        return None
    # Typical municipal labels include a numeric prefix, e.g. 16MNU -> MNU.
    stripped = re.sub(r"^\d+", "", value)
    return stripped or value


def _clean_html_text(value: str | None) -> str:
    if not value:
        return ""
    text = unescape(value)
    text = re.sub(r"<br\\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _parse_semicolon_feature_info(body: str) -> tuple[str, dict[str, str]] | None:
    """Parse ArcGIS/GeoServer-style WMS text/plain feature info payload.

    Example:
        @MPZP - przeznaczenia terenów OBJECTID;numerplanu;symbolzrastra;...; 367273;RS_85;MNU;...
    """
    text = re.sub(r"\s+", " ", body).strip()
    if not text.startswith("@") or ";" not in text:
        return None

    first_semicolon = text.find(";")
    if first_semicolon < 0:
        return None

    head = text[:first_semicolon]
    boundary = head.rfind(" ")
    if boundary < 2:
        return None

    layer_name = head[1:boundary].strip()
    token_stream = (head[boundary + 1 :] + text[first_semicolon:]).strip()
    tokens = [token.strip() for token in token_stream.split(";") if token.strip()]
    if len(tokens) < 2 or len(tokens) % 2 != 0:
        return None

    half = len(tokens) // 2
    keys = tokens[:half]
    values = tokens[half:]
    return layer_name, dict(zip(keys, values, strict=False))


def parse_ruda_plaintext_feature_info(body: str) -> WMSFeatureInfoHit | None:
    parsed = _parse_semicolon_feature_info(body)
    if parsed is None:
        return None

    _, payload = parsed
    designation = (
        _normalize_designation(payload.get("symbolzrastra"))
        or _normalize_designation(payload.get("oznaczenieterenuzrastra"))
    )
    if not designation:
        return None

    plan_name = (payload.get("numerplanu") or "Ruda Śląska WMS").strip()
    description = payload.get("kategoriaprzeznaczeniaterenu")
    uchwala_nr = payload.get("numeruchwałyouchwaleniuplanu")
    return WMSFeatureInfoHit(
        designation=designation,
        description=description.strip() if description else None,
        plan_name=plan_name,
        uchwala_nr=uchwala_nr.strip() if uchwala_nr else None,
        raw_payload=payload,
    )


def parse_gison_portal_html_feature_info(body: str) -> WMSFeatureInfoHit | None:
    text = body.strip()
    if not text:
        return None

    raw_designation = None
    raw_description = None
    plan_name = None
    uchwala_nr = None
    legend_url = None

    przezn_section = None
    if match := re.search(
        r"<b>\s*Przeznaczenie:\s*</b>(.*?)(?:<br/><br/>|<b>\s*Dokument zmieniający:|$)",
        text,
        re.IGNORECASE | re.DOTALL,
    ):
        przezn_section = match.group(1)
    if przezn_section and (
        match := re.search(
            r"<b>\s*([^<]+?)\s*</b>\s*-\s*([^<\r\n]+)",
            przezn_section,
            re.IGNORECASE,
        )
    ):
        raw_designation = match.group(1)
        raw_description = match.group(2)

    if match := re.search(
        r"<b>\s*Nazwa:\s*</b>\s*<strong>(.*?)</strong>",
        text,
        re.IGNORECASE | re.DOTALL,
    ):
        plan_name = _clean_html_text(match.group(1))

    if match := re.search(
        r"MPZP Nr:\s*([^<]+)",
        text,
        re.IGNORECASE,
    ):
        uchwala_nr = _clean_html_text(match.group(1))

    if match := re.search(
        r"<b>\s*Legenda:\s*</b>\s*<a href=['\"]([^'\"]+)['\"]",
        text,
        re.IGNORECASE,
    ):
        legend_url = match.group(1).strip()

    designation = _normalize_designation(raw_designation)
    if not designation:
        return None

    payload: dict[str, str] = {}
    if legend_url:
        payload["legend_url"] = legend_url
    return WMSFeatureInfoHit(
        designation=designation,
        description=_clean_html_text(raw_description) or None,
        plan_name=plan_name or "GISON MPZP",
        uchwala_nr=uchwala_nr or None,
        raw_payload=payload,
    )


def parse_gison_portal_html_metadata(body: str) -> dict[str, str | None] | None:
    """Extract plan metadata even when the public popup omits designation."""
    text = body.strip()
    if not text:
        return None

    plan_name = None
    uchwala_nr = None
    legend_url = None

    if match := re.search(
        r"<b>\s*Nazwa:\s*</b>\s*<strong>(.*?)</strong>",
        text,
        re.IGNORECASE | re.DOTALL,
    ):
        plan_name = _clean_html_text(match.group(1))

    if match := re.search(
        r"MPZP Nr:\s*([^<]+)",
        text,
        re.IGNORECASE,
    ):
        uchwala_nr = _clean_html_text(match.group(1))

    if match := re.search(
        r"<b>\s*Legenda:\s*</b>\s*<a href=['\"]([^'\"]+)['\"]",
        text,
        re.IGNORECASE,
    ):
        legend_url = match.group(1).strip()

    if not any((plan_name, uchwala_nr, legend_url)) and "MPZP" not in text.upper():
        return None

    return {
        "plan_name": plan_name or "GISON MPZP",
        "uchwala_nr": uchwala_nr or None,
        "legend_url": legend_url or None,
    }


_PARSERS: dict[str, Callable[[str], WMSFeatureInfoHit | None]] = {
    "ruda_plaintext": parse_ruda_plaintext_feature_info,
    "gison_portal_html": parse_gison_portal_html_feature_info,
}


def _ensure_multipolygon(geom: BaseGeometry) -> MultiPolygon | None:
    valid = make_valid(geom)
    if valid.is_empty:
        return None
    if isinstance(valid, Polygon):
        return MultiPolygon([valid])
    if isinstance(valid, MultiPolygon):
        return valid
    if hasattr(valid, "geoms"):
        polys = [g for g in valid.geoms if isinstance(g, Polygon)]
        if polys:
            return MultiPolygon(polys)
    return None


class WMSGridIngestor:
    """Samples a WMS source over parcel geometries and stores approximate zones."""

    def __init__(self, config: WMSGridConfig) -> None:
        self.config = config
        self._verify = True
        self._http = self._build_client()
        try:
            self._parser = _PARSERS[config.parser_name]
        except KeyError as exc:
            raise ValueError(f"Unsupported WMS parser: {config.parser_name}") from exc
        self._parcel_to_wgs84 = Transformer.from_crs(2180, 4326, always_xy=True)

    def _build_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=httpx.Timeout(_WMS_TIMEOUT_S),
            follow_redirects=True,
            verify=self._verify,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def run(self) -> WFSIngestReport:
        started = asyncio.get_event_loop().time()
        report = WFSIngestReport(
            wfs_url=self.config.wms_url,
            layer=self.config.layer_name,
            teryt_gmina=self.config.teryt_gmina,
        )

        async with AsyncSessionLocal() as db:
            dzialki = (
                await db.execute(
                    select(Dzialka)
                    .where(Dzialka.teryt_gmina == self.config.teryt_gmina)
                    .where(Dzialka.resolution_status == "resolved")
                )
            ).scalars().all()

            features: list[WFSFeature] = []
            for dzialka in dzialki:
                parcel_geom = to_shape(dzialka.geom)
                parcel_features = await self._sample_parcel(
                    dzialka.identyfikator,
                    parcel_geom,
                )
                features.extend(parcel_features)

            report.features_fetched = len(features)
            valid = [f for f in features if f.is_valid_bounds()]
            report.features_valid = len(valid)
            report.features_skipped_bounds = len(features) - len(valid)

            client = WFSClient()
            report.features_upserted = await client.ingest_planning_zones(db, valid)
            report.features_failed = report.features_valid - report.features_upserted

        report.duration_s = round(asyncio.get_event_loop().time() - started, 2)
        return report

    async def _sample_parcel(
        self,
        identyfikator: str,
        geom: BaseGeometry,
    ) -> list[WFSFeature]:
        minx, miny, maxx, maxy = geom.bounds
        n = max(3, self.config.sample_grid)
        width = max(maxx - minx, 1.0)
        height = max(maxy - miny, 1.0)
        cell_w = width / n
        cell_h = height / n

        hits_by_designation: dict[tuple[str, str, str, str | None], list[BaseGeometry]] = {}

        for ix in range(n):
            for iy in range(n):
                x0 = minx + ix * cell_w
                x1 = minx + (ix + 1) * cell_w
                y0 = miny + iy * cell_h
                y1 = miny + (iy + 1) * cell_h
                cell = box(x0, y0, x1, y1).intersection(geom)
                if cell.is_empty:
                    continue

                center = Point((x0 + x1) / 2.0, (y0 + y1) / 2.0)
                if not center.within(geom):
                    continue

                hit = await self._query_point(center)
                if hit is None:
                    continue

                key = (
                    hit.designation,
                    hit.description or "",
                    hit.plan_name,
                    hit.uchwala_nr,
                )
                hits_by_designation.setdefault(key, []).append(cell)

        features: list[WFSFeature] = []
        for (designation, description, plan_name, uchwala_nr), cells in hits_by_designation.items():
            merged = unary_union(cells)
            multipolygon = _ensure_multipolygon(merged)
            if multipolygon is None or multipolygon.is_empty:
                continue

            features.append(
                WFSFeature(
                    przeznaczenie=designation,
                    plan_name=plan_name,
                    teryt_gmina=self.config.teryt_gmina,
                    geom=multipolygon,
                    plan_type=self.config.plan_type,
                    uchwala_nr=uchwala_nr,
                    przeznaczenie_opis=description or f"WMS sampled for parcel {identyfikator}",
                    plan_effective_date=None,
                    source_wfs_url=(
                        f"{self.config.wms_url}"
                        f"?layer={self.config.layer_name}&mode=wms-grid&parcel={identyfikator}"
                    ),
                )
            )
        return features

    async def _query_point(self, point: Point) -> WMSFeatureInfoHit | None:
        if self.config.query_url_template:
            lon, lat = self._parcel_to_wgs84.transform(point.x, point.y)
            request_url = (
                self.config.query_url_template
                .replace("%lon%", str(lon))
                .replace("%lat%", str(lat))
                .replace("%zoom%", "18")
            )
            response = await self._http.get(request_url)
            response.raise_for_status()
            body = response.text.strip()
            if not body or "ServiceException" in body:
                return None
            hit = self._parser(body)
            if hit is not None:
                return hit
            if self.config.fallback_designation and self.config.parser_name == "gison_portal_html":
                metadata = parse_gison_portal_html_metadata(body)
                if metadata is not None:
                    raw_payload: dict[str, str] = {}
                    legend_url = metadata.get("legend_url")
                    if isinstance(legend_url, str) and legend_url:
                        raw_payload["legend_url"] = legend_url
                    return WMSFeatureInfoHit(
                        designation=self.config.fallback_designation,
                        description=self.config.fallback_description,
                        plan_name=str(metadata.get("plan_name") or "GISON MPZP"),
                        uchwala_nr=(
                            str(metadata["uchwala_nr"])
                            if metadata.get("uchwala_nr")
                            else None
                        ),
                        raw_payload=raw_payload,
                    )
            return None

        span = self.config.point_halfspan_m
        if self.config.swap_bbox_axes:
            bbox = f"{point.y - span},{point.x - span},{point.y + span},{point.x + span}"
        else:
            bbox = f"{point.x - span},{point.y - span},{point.x + span},{point.y + span}"

        params = {
            "SERVICE": "WMS",
            "VERSION": self.config.version,
            "REQUEST": "GetFeatureInfo",
            "LAYERS": self.config.layer_name,
            "QUERY_LAYERS": self.config.layer_name,
            "CRS": f"EPSG:{self.config.source_srid}",
            "BBOX": bbox,
            "WIDTH": "101",
            "HEIGHT": "101",
            "I": "50",
            "J": "50",
            "INFO_FORMAT": self.config.info_format,
            "STYLES": self.config.styles,
            "FEATURE_COUNT": "5",
        }
        encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        request_url = f"{self.config.wms_url}?{encoded_params}"
        try:
            response = await self._http.get(request_url)
        except httpx.ConnectError as exc:
            if self._verify:
                logger.warning(
                    "[WMSGrid] TLS/connect error for %s; retrying with verify=False: %s",
                    self.config.wms_url,
                    exc,
                )
                self._verify = False
                await self._http.aclose()
                self._http = self._build_client()
                response = await self._http.get(request_url)
            else:
                raise
        response.raise_for_status()
        body = response.text.strip()
        if not body or "ServiceException" in body:
            return None
        return self._parser(body)


async def run_wms_grid_ingest(
    *,
    wms_url: str,
    layer_name: str,
    teryt_gmina: str,
    plan_type: str = "mpzp",
    source_srid: int = 2180,
    version: str = "1.3.0",
    info_format: str = "text/plain",
    styles: str = "default",
    sample_grid: int = _DEFAULT_SAMPLE_GRID,
    point_halfspan_m: float = _DEFAULT_HALFSPAN_M,
    swap_bbox_axes: bool = False,
    parser_name: str = "ruda_plaintext",
    query_url_template: str | None = None,
    fallback_designation: str | None = None,
    fallback_description: str | None = None,
) -> WFSIngestReport:
    ingestor = WMSGridIngestor(
        WMSGridConfig(
            wms_url=wms_url,
            layer_name=layer_name,
            teryt_gmina=teryt_gmina,
            plan_type=plan_type,
            source_srid=source_srid,
            version=version,
            info_format=info_format,
            styles=styles,
            sample_grid=sample_grid,
            point_halfspan_m=point_halfspan_m,
            swap_bbox_axes=swap_bbox_axes,
            parser_name=parser_name,
            query_url_template=query_url_template,
            fallback_designation=fallback_designation,
            fallback_description=fallback_description,
        )
    )
    try:
        return await ingestor.run()
    finally:
        await ingestor.aclose()
