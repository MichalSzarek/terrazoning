"""WFSClient — Planning Zone ingestion from WFS endpoints → gold.planning_zones.

Local-First GIS Commandment: planning zones MUST be cached locally before use.
This module is run once (or periodically) as a scheduled ingestion job, NOT
in the hot path of the resolution pipeline.

Architecture:
    WFS endpoint (MPZP / POG / Studium)
         │
         ▼  httpx GET → GeoJSON (preferred) or GML (fallback)
    WFSClient.fetch_features(url, layer, teryt_gmina, ...)
         │  parse → list[WFSFeature]
         │  reproject to EPSG:2180 (pyproj, if source CRS ≠ 2180)
         │  ST_MakeValid on every geometry
         ▼
    WFSClient.ingest_planning_zones(db, features)
         │  from_shape(geom, srid=2180) → GeoAlchemy2 WKBElement
         │  pg_insert(PlanningZone).on_conflict_do_update(source_url+przeznaczenie+geom)
         ▼
    gold.planning_zones

Supported WFS output formats:
    1. GeoJSON  (OUTPUTFORMAT=application/json) — primary
    2. GML 3.x  (WFS default)                  — fallback via lxml

WFS source status (2025):
    The old national WFS (integracja.gugik.gov.pl) is permanently offline.
    GUGiK now provides only a WMS aggregation (mapy.geoportal.gov.pl/wss/ext/
    KrajowaIntegracjaMiejscowychPlanowZagospodarowaniaPrzestrzennego) — visualization only.
    For actual polygon data use municipality-level WFS services or seed_test_zones.py for testing.

Note: WFS layers, property names, and CRS vary by municipality.
Use WFSFieldMapping to adapt to a specific service.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Optional
from uuid import UUID

import httpx
from geoalchemy2.shape import from_shape
from pyproj import Transformer
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform as shapely_transform
from shapely.validation import make_valid
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.gold import PlanningZone

logger = logging.getLogger(__name__)

_INVALID_XML_CHAR_RE = re.compile(
    r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]"
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_WFS_TIMEOUT_S = 60.0          # WFS responses can be large (> 10 MB GML)
_WFS_MAX_FEATURES = 10_000     # safety cap per request
_SLIVER_THRESHOLD_M2 = 0.5     # geometries smaller than this are discarded

# EPSG:2180 coordinate bounds for Poland — sanity check after reprojection
_EPSG2180_X_RANGE = (140_000, 900_000)
_EPSG2180_Y_RANGE = (100_000, 800_000)


# ---------------------------------------------------------------------------
# Field mapping — configurable per WFS layer
# ---------------------------------------------------------------------------

@dataclass
class WFSFieldMapping:
    """Maps WFS feature property keys to PlanningZone columns.

    Defaults match the Krajowa Integracja MPZP (GUGiK) schema.
    Override for municipality-specific WFS layers.
    """
    przeznaczenie: str = "przeznaczenie"
    plan_name: str = "nazwa_planu"
    teryt_gmina: str = "gmina_teryt"
    uchwala_nr: str = "uchwala"
    przeznaczenie_opis: str = "opis_przeznaczenia"
    plan_effective_date: str = "data_wejscia_w_zycie"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class WFSFeature:
    """Single parsed planning zone feature from a WFS response.

    Geometry is always in EPSG:2180 after fetch_features() returns.
    """
    przeznaczenie: str
    plan_name: str
    teryt_gmina: str
    geom: BaseGeometry              # Shapely MultiPolygon, EPSG:2180
    plan_type: str                  # 'mpzp' | 'pog' | 'studium'
    uchwala_nr: Optional[str] = None
    przeznaczenie_opis: Optional[str] = None
    plan_effective_date: Optional[date] = None
    source_wfs_url: Optional[str] = None

    def is_valid_bounds(self) -> bool:
        """Check that the geometry centroid falls within EPSG:2180 Poland bounds."""
        c = self.geom.centroid
        x_ok = _EPSG2180_X_RANGE[0] <= c.x <= _EPSG2180_X_RANGE[1]
        y_ok = _EPSG2180_Y_RANGE[0] <= c.y <= _EPSG2180_Y_RANGE[1]
        return x_ok and y_ok


@dataclass
class WFSIngestReport:
    """Summary of a WFS ingestion run."""
    wfs_url: str
    layer: str
    teryt_gmina: str
    features_fetched: int = 0
    features_valid: int = 0
    features_skipped_bounds: int = 0
    features_skipped_sliver: int = 0
    features_upserted: int = 0
    features_failed: int = 0
    duration_s: float = 0.0
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# WFSClient
# ---------------------------------------------------------------------------

class WFSClient:
    """Async WFS client for planning zone ingestion.

    Usage (async context manager):
        async with WFSClient() as client:
            report = await client.fetch_and_ingest(
                db=db,
                wfs_url="https://...",
                layer_name="app:mpzp_strefy",
                plan_type="mpzp",
                teryt_gmina="1412011",
            )
    """

    def __init__(
        self,
        timeout_s: float = _WFS_TIMEOUT_S,
        max_features: int = _WFS_MAX_FEATURES,
    ) -> None:
        self._timeout = httpx.Timeout(timeout_s)
        self._max_features = max_features
        self._http: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "WFSClient":
        self._http = httpx.AsyncClient(timeout=self._timeout, follow_redirects=True)
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._http:
            await self._http.aclose()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_features(
        self,
        wfs_url: str,
        layer_name: str,
        plan_type: str,
        teryt_gmina: str,
        *,
        source_srid: int = 2180,
        bbox_2180: Optional[tuple[float, float, float, float]] = None,
        field_mapping: Optional[WFSFieldMapping] = None,
        cql_filter: Optional[str] = None,
        wfs_version: str = "2.0.0",
        prefer_json: bool = True,
        swap_xy: bool = False,
    ) -> list[WFSFeature]:
        """Fetch WFS features and return them reprojected to EPSG:2180.

        Args:
            wfs_url:     WFS service base URL (without query params)
            layer_name:  WFS TYPENAMES / layer identifier
            plan_type:   'mpzp' | 'pog' | 'studium'
            teryt_gmina: 7-char TERYT gmina code (for metadata + optional CQL filter)
            source_srid: CRS of the WFS source (default 2180; use 4326 for national WFS)
            bbox_2180:   Optional (xmin, ymin, xmax, ymax) bounding box in EPSG:2180
            field_mapping: Maps WFS property names → PlanningZone columns
            cql_filter:  Optional OGC CQL_FILTER string (e.g. "gmina_teryt='1412011'")

        Returns:
            List of WFSFeature instances with geometry in EPSG:2180.
        """
        assert self._http is not None, "Use WFSClient as async context manager"

        mapping = field_mapping or WFSFieldMapping()
        params = self._build_request_params(
            layer_name, bbox_2180, source_srid, cql_filter,
            wfs_version=wfs_version, prefer_json=prefer_json,
        )

        logger.info(
            "[WFS] Fetching layer=%s teryt=%s from %s (srid=%d wfs=%s json=%s)",
            layer_name, teryt_gmina, wfs_url, source_srid, wfs_version, prefer_json,
        )

        try:
            response = await self._http.get(wfs_url, params=params)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error("[WFS] HTTP %d for %s: %s", exc.response.status_code, wfs_url, exc)
            raise
        except httpx.RequestError as exc:
            logger.error("[WFS] Request failed for %s: %s", wfs_url, exc)
            raise

        content_type = response.headers.get("content-type", "")
        if "json" in content_type or response.text.lstrip().startswith("{"):
            raw_features = self._parse_geojson_response(response.text)
        else:
            raw_features = self._parse_gml_response(response.text)

        logger.info("[WFS] Received %d raw features from %s", len(raw_features), layer_name)

        features: list[WFSFeature] = []
        for raw in raw_features:
            parsed = self._parse_feature(
                raw, plan_type=plan_type, teryt_gmina=teryt_gmina,
                source_srid=source_srid, mapping=mapping, wfs_url=wfs_url,
                swap_xy=swap_xy,
            )
            if parsed is not None:
                features.append(parsed)

        logger.info(
            "[WFS] Parsed %d valid features (discarded %d)",
            len(features), len(raw_features) - len(features),
        )
        return features

    async def ingest_planning_zones(
        self,
        db: AsyncSession,
        features: list[WFSFeature],
    ) -> int:
        """Upsert WFSFeature list into gold.planning_zones.

        Conflict resolution: if a row with the same (teryt_gmina, przeznaczenie, plan_type)
        already exists from the same source_wfs_url, refresh it (geometry + metadata).
        New zones are inserted.

        Returns number of rows upserted.
        """
        if not features:
            return 0

        upserted = 0
        failed = 0

        for feat in features:
            geom_wkb = from_shape(feat.geom, srid=2180)
            c = feat.geom.centroid
            geom_hash = f"{round(c.x)}_{round(c.y)}"

            try:
                stmt = (
                    pg_insert(PlanningZone)
                    .values(
                        plan_type=feat.plan_type,
                        plan_name=feat.plan_name,
                        uchwala_nr=feat.uchwala_nr,
                        teryt_gmina=feat.teryt_gmina,
                        przeznaczenie=feat.przeznaczenie,
                        przeznaczenie_opis=feat.przeznaczenie_opis,
                        geom=geom_wkb,
                        geom_hash=geom_hash,
                        source_wfs_url=feat.source_wfs_url,
                        ingested_at=datetime.now(timezone.utc),
                        plan_effective_date=feat.plan_effective_date,
                    )
                    # Upsert key: same source + same gmina + same designation + same location
                    .on_conflict_do_update(
                        constraint="uq_planning_zones_spatial_key",
                        set_={
                            "geom": geom_wkb,
                            "plan_name": feat.plan_name,
                            "przeznaczenie_opis": feat.przeznaczenie_opis,
                            "ingested_at": datetime.now(timezone.utc),
                            "plan_effective_date": feat.plan_effective_date,
                            "updated_at": datetime.now(timezone.utc),
                        },
                    )
                )
                await db.execute(stmt)
                upserted += 1
            except Exception as exc:
                logger.warning(
                    "[WFS] Failed to upsert zone przeznaczenie=%r teryt=%s: %s",
                    feat.przeznaczenie, feat.teryt_gmina, exc,
                )
                failed += 1

        await db.commit()
        logger.info(
            "[WFS] Ingestion complete: upserted=%d failed=%d",
            upserted, failed,
        )
        return upserted

    async def fetch_and_ingest(
        self,
        db: AsyncSession,
        wfs_url: str,
        layer_name: str,
        plan_type: str,
        teryt_gmina: str,
        *,
        source_srid: int = 2180,
        bbox_2180: Optional[tuple[float, float, float, float]] = None,
        field_mapping: Optional[WFSFieldMapping] = None,
        cql_filter: Optional[str] = None,
        wfs_version: str = "2.0.0",
        prefer_json: bool = True,
        swap_xy: bool = False,
    ) -> WFSIngestReport:
        """Convenience method: fetch features then ingest them, return full report."""
        import asyncio as _asyncio

        t_start = _asyncio.get_event_loop().time()
        report = WFSIngestReport(
            wfs_url=wfs_url,
            layer=layer_name,
            teryt_gmina=teryt_gmina,
        )

        features = await self.fetch_features(
            wfs_url=wfs_url,
            layer_name=layer_name,
            plan_type=plan_type,
            teryt_gmina=teryt_gmina,
            source_srid=source_srid,
            bbox_2180=bbox_2180,
            field_mapping=field_mapping,
            cql_filter=cql_filter,
            wfs_version=wfs_version,
            prefer_json=prefer_json,
            swap_xy=swap_xy,
        )
        report.features_fetched = len(features)

        # Count discards that already happened in fetch_features
        valid = [f for f in features if f.is_valid_bounds()]
        report.features_skipped_bounds = len(features) - len(valid)
        report.features_valid = len(valid)

        report.features_upserted = await self.ingest_planning_zones(db, valid)
        report.features_failed = report.features_valid - report.features_upserted
        report.duration_s = round(_asyncio.get_event_loop().time() - t_start, 2)

        logger.info(
            "[WFS] fetch_and_ingest done: fetched=%d valid=%d upserted=%d "
            "skipped_bounds=%d in %.1fs",
            report.features_fetched, report.features_valid, report.features_upserted,
            report.features_skipped_bounds, report.duration_s,
        )
        return report

    async def fetch_by_bbox_tiles(
        self,
        wfs_url: str,
        layer_name: str,
        plan_type: str,
        teryt_gmina: str,
        bbox_2180: tuple[float, float, float, float],
        *,
        tile_size_m: float = 10_000.0,
        source_srid: int = 2180,
        field_mapping: Optional[WFSFieldMapping] = None,
        cql_filter: Optional[str] = None,
        max_concurrent: int = 4,
    ) -> list[WFSFeature]:
        """Split a large bbox into tiles and fetch each tile independently.

        Solves the WFS national bottleneck (Red Flag 3): a single GetFeature request
        covering a whole gmina can time out or hit the WFS server's feature cap.
        Tiling keeps every request to roughly one obręb-sized area (≤ 10 km × 10 km),
        well within WFS server time limits and the _WFS_MAX_FEATURES cap.

        Tiles are fetched concurrently (up to max_concurrent at a time — be polite
        to public GUGiK / municipal WFS servers).

        Border-touching features (same geometry returned by two adjacent tiles) are
        deduplicated using centroid coordinates rounded to 1 m — sufficient precision
        for planning zone polygons whose centroids are never within 1 m of each other.

        Args:
            bbox_2180:      (xmin, ymin, xmax, ymax) in EPSG:2180 covering the gmina.
            tile_size_m:    Side length of each square tile in metres (default 10 km).
            max_concurrent: Max simultaneous HTTP requests (default 4).

        Returns:
            Deduplicated list of WFSFeature instances in EPSG:2180.
        """
        import asyncio as _asyncio
        import math

        assert self._http is not None, "Use WFSClient as async context manager"

        xmin, ymin, xmax, ymax = bbox_2180
        x_count = max(1, math.ceil((xmax - xmin) / tile_size_m))
        y_count = max(1, math.ceil((ymax - ymin) / tile_size_m))
        total_tiles = x_count * y_count

        logger.info(
            "[WFS] Tiled fetch: bbox=(%.0f,%.0f→%.0f,%.0f) tile=%.0fm "
            "grid=%dx%d=%d layer=%s teryt=%s",
            xmin, ymin, xmax, ymax, tile_size_m,
            x_count, y_count, total_tiles, layer_name, teryt_gmina,
        )

        # Build tile bounding boxes (no overlap — adjacent tiles share an edge,
        # not a band, so border features appear in at most two tiles)
        tiles: list[tuple[float, float, float, float]] = []
        for xi in range(x_count):
            for yi in range(y_count):
                tx_min = xmin + xi * tile_size_m
                ty_min = ymin + yi * tile_size_m
                tx_max = min(xmin + (xi + 1) * tile_size_m, xmax)
                ty_max = min(ymin + (yi + 1) * tile_size_m, ymax)
                tiles.append((tx_min, ty_min, tx_max, ty_max))

        semaphore = _asyncio.Semaphore(max_concurrent)

        async def _fetch_one(tile_bbox: tuple[float, float, float, float]) -> list[WFSFeature]:
            async with semaphore:
                try:
                    return await self.fetch_features(
                        wfs_url=wfs_url,
                        layer_name=layer_name,
                        plan_type=plan_type,
                        teryt_gmina=teryt_gmina,
                        source_srid=source_srid,
                        bbox_2180=tile_bbox,
                        field_mapping=field_mapping,
                        cql_filter=cql_filter,
                    )
                except Exception as exc:
                    logger.warning(
                        "[WFS] Tile (%.0f,%.0f,%.0f,%.0f) failed: %s — skipping tile",
                        *tile_bbox, exc,
                    )
                    return []

        tile_results = await _asyncio.gather(*[_fetch_one(t) for t in tiles])

        # Deduplicate border-touching features.
        # Key: (centroid_x rounded to 1 m, centroid_y rounded to 1 m, przeznaczenie)
        # Planning zone polygons in Poland are never ≤ 1 m apart in centroid space,
        # so this uniquely identifies each feature without WKB comparison overhead.
        seen: set[tuple[int, int, str]] = set()
        merged: list[WFSFeature] = []
        total_raw = sum(len(r) for r in tile_results)
        for tile_feats in tile_results:
            for feat in tile_feats:
                c = feat.geom.centroid
                key = (round(c.x), round(c.y), feat.przeznaczenie)
                if key not in seen:
                    seen.add(key)
                    merged.append(feat)

        duplicates_removed = total_raw - len(merged)
        logger.info(
            "[WFS] Tiled merge complete: raw=%d unique=%d border_dupes_removed=%d "
            "tiles_ok=%d/%d",
            total_raw, len(merged), duplicates_removed,
            sum(1 for r in tile_results if r), total_tiles,
        )
        return merged

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_request_params(
        self,
        layer_name: str,
        bbox_2180: Optional[tuple[float, float, float, float]],
        source_srid: int,
        cql_filter: Optional[str],
        wfs_version: str = "2.0.0",
        prefer_json: bool = True,
    ) -> dict[str, str]:
        is_v2 = wfs_version.startswith("2.")
        params: dict[str, str] = {
            "SERVICE": "WFS",
            "VERSION": wfs_version,
            "REQUEST": "GetFeature",
            # WFS 2.0.0 → TYPENAMES, WFS 1.x → TYPENAME
            ("TYPENAMES" if is_v2 else "TYPENAME"): layer_name,
            # WFS 2.0.0 → COUNT, WFS 1.x → MAXFEATURES
            ("COUNT" if is_v2 else "MAXFEATURES"): str(self._max_features),
            "SRSNAME": f"urn:ogc:def:crs:EPSG::{source_srid}",
        }
        if prefer_json:
            params["OUTPUTFORMAT"] = "application/json"
        if bbox_2180 is not None:
            xmin, ymin, xmax, ymax = bbox_2180
            params["BBOX"] = (
                f"{xmin},{ymin},{xmax},{ymax},"
                f"urn:ogc:def:crs:EPSG::{source_srid}"
            )
        if cql_filter:
            params["CQL_FILTER"] = cql_filter
        return params

    def _parse_geojson_response(self, body: str) -> list[dict[str, Any]]:
        """Parse a WFS GeoJSON FeatureCollection body."""
        import json

        try:
            fc = json.loads(body)
        except json.JSONDecodeError as exc:
            logger.error("[WFS] Failed to parse GeoJSON response: %s", exc)
            return []

        if fc.get("type") != "FeatureCollection":
            logger.warning("[WFS] Unexpected GeoJSON type: %s", fc.get("type"))
            return []

        return fc.get("features", [])

    def _parse_gml_response(self, body: str) -> list[dict[str, Any]]:
        """Parse WFS GML 3.x response into pseudo-GeoJSON feature list.

        Handles both WFS 2.0.0 (GML 3.2) and WFS 1.1.0 (GML 3.1.1) responses,
        including ArcGIS Server WFS output. Does NOT require OGR/GDAL.
        """
        try:
            from lxml import etree
        except ImportError:
            logger.error("[WFS] lxml not installed — cannot parse GML responses")
            return []

        try:
            root = etree.fromstring(body.encode())
        except etree.XMLSyntaxError as exc:
            sanitized_body = _INVALID_XML_CHAR_RE.sub("", body)
            if sanitized_body != body:
                logger.warning(
                    "[WFS] GML XML parse error; retrying after stripping invalid control chars: %s",
                    exc,
                )
                try:
                    root = etree.fromstring(sanitized_body.encode())
                except etree.XMLSyntaxError as sanitized_exc:
                    logger.error("[WFS] GML XML parse error after sanitization: %s", sanitized_exc)
                    return []
            else:
                logger.error("[WFS] GML XML parse error: %s", exc)
                return []

        # Check for OGC exception response
        root_local = root.tag.split("}")[-1] if "}" in root.tag else root.tag
        if root_local == "ExceptionReport":
            exc_els = root.findall(".//{http://www.opengis.net/ows/1.1}ExceptionText")
            if not exc_els:
                exc_els = root.findall(".//{http://www.opengis.net/ows}ExceptionText")
            msg = exc_els[0].text.strip() if exc_els else body[:300]
            logger.error("[WFS] OGC ExceptionReport: %s", msg)
            return []

        _GML_NS = [
            "http://www.opengis.net/gml/3.2",
            "http://www.opengis.net/gml",
        ]
        _MEMBER_TAGS = [
            "{http://www.opengis.net/wfs/2.0}member",
            "{http://www.opengis.net/wfs}member",
            "{http://www.opengis.net/wfs}featureMember",
            "{http://www.opengis.net/gml/3.2}featureMember",
            "{http://www.opengis.net/gml}featureMember",
        ]

        members: list = []
        for tag in _MEMBER_TAGS:
            members = root.findall(tag)
            if members:
                break
        if not members:
            # ArcGIS sometimes wraps in featureMembers (plural)
            for tag in [
                "{http://www.opengis.net/wfs/2.0}members",
                "{http://www.opengis.net/wfs}featureMembers",
            ]:
                container = root.find(tag)
                if container is not None:
                    members = list(container)
                    break
        if not members:
            # Last resort: direct non-bounding children
            members = [
                c for c in root
                if not (c.tag.split("}")[-1] if "}" in c.tag else c.tag)
                .startswith("boundedBy")
            ]

        features: list[dict[str, Any]] = []
        for member in members:
            feature_el = member[0] if len(member) else member
            props: dict[str, Any] = {}
            geom_shapely = None

            for child in feature_el:
                local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                geom = _try_parse_gml_geometry(child, _GML_NS)
                if geom is not None:
                    geom_shapely = geom
                    continue
                if child.text and child.text.strip():
                    props[local] = child.text.strip()

            if geom_shapely is None:
                continue

            from shapely.geometry import mapping as _sm
            features.append({
                "type": "Feature",
                "properties": props,
                "geometry": _sm(geom_shapely),
            })

        logger.info("[WFS] GML parsing extracted %d features", len(features))
        return features

    def _parse_feature(
        self,
        raw: dict[str, Any],
        plan_type: str,
        teryt_gmina: str,
        source_srid: int,
        mapping: WFSFieldMapping,
        wfs_url: str,
        swap_xy: bool = False,
    ) -> Optional[WFSFeature]:
        """Parse one raw GeoJSON feature into a WFSFeature.

        Returns None if the feature should be discarded (invalid geometry, missing
        required fields, sliver, or out-of-bounds).
        """
        props = raw.get("properties") or {}
        geom_json = raw.get("geometry")

        if not geom_json:
            logger.debug("[WFS] Feature missing geometry, skipping")
            return None

        # --- Required field: przeznaczenie ---
        przeznaczenie = _get_prop(props, mapping.przeznaczenie)
        if not przeznaczenie:
            logger.debug("[WFS] Feature missing przeznaczenie, skipping props=%r", props)
            return None

        # --- Geometry parsing ---
        try:
            geom = shape(geom_json)
        except Exception as exc:
            logger.warning("[WFS] Failed to parse geometry: %s", exc)
            return None

        # Apply ST_MakeValid equivalent
        if not geom.is_valid:
            geom = make_valid(geom)
            logger.debug("[WFS] Applied make_valid on %r", przeznaczenie)

        # Reproject to EPSG:2180 if source is different
        if source_srid != 2180:
            try:
                geom = _reproject_to_2180(geom, source_srid, swap_xy=swap_xy)
            except Exception as exc:
                logger.warning(
                    "[WFS] Reprojection from EPSG:%d to 2180 failed: %s",
                    source_srid, exc,
                )
                return None

        # Ensure MultiPolygon
        geom = _ensure_multipolygon(geom)
        if geom is None:
            logger.debug("[WFS] Non-polygonal geometry discarded for %r", przeznaczenie)
            return None

        # Sliver check
        if geom.area < _SLIVER_THRESHOLD_M2:
            logger.debug(
                "[WFS] Sliver discarded: area=%.4f m² przeznaczenie=%r",
                geom.area, przeznaczenie,
            )
            return None

        # --- Optional fields ---
        plan_name = _get_prop(props, mapping.plan_name) or f"Plan {teryt_gmina}"
        gmina = _get_prop(props, mapping.teryt_gmina) or teryt_gmina
        uchwala_nr = _get_prop(props, mapping.uchwala_nr)
        przeznaczenie_opis = _get_prop(props, mapping.przeznaczenie_opis)
        effective_date = _parse_date(_get_prop(props, mapping.plan_effective_date))

        feat = WFSFeature(
            przeznaczenie=przeznaczenie.strip().upper(),
            plan_name=plan_name,
            teryt_gmina=gmina[:7],          # clamp to 7 chars
            geom=geom,
            plan_type=plan_type,
            uchwala_nr=uchwala_nr,
            przeznaczenie_opis=przeznaczenie_opis,
            plan_effective_date=effective_date,
            source_wfs_url=wfs_url,
        )

        if not feat.is_valid_bounds():
            logger.warning(
                "[WFS] SEVERITY:MEDIUM — geometry centroid out of EPSG:2180 Poland bounds: "
                "przeznaczenie=%r centroid=(%f, %f)",
                przeznaczenie, feat.geom.centroid.x, feat.geom.centroid.y,
            )
            return None

        return feat


# ---------------------------------------------------------------------------
# GML geometry parsing helpers (no OGR/GDAL required)
# ---------------------------------------------------------------------------

def _try_parse_gml_geometry(
    element: Any,
    gml_namespaces: list[str],
) -> Optional[BaseGeometry]:
    """Attempt to parse a GML geometry element to Shapely. Returns None if not a geometry."""
    from shapely.geometry import MultiPolygon
    for gml_ns in gml_namespaces:
        polys = _collect_gml_polygons(element, gml_ns)
        if polys:
            return polys[0] if len(polys) == 1 else MultiPolygon(polys)
    return None


def _collect_gml_polygons(element: Any, gml_ns: str) -> list[Any]:
    """Recursively collect Shapely Polygon objects from a GML element."""
    from shapely.geometry import Polygon

    polys: list[Any] = []
    local = element.tag.split("}")[-1] if "}" in element.tag else element.tag

    if local == "Polygon":
        p = _parse_gml_polygon(element, gml_ns)
        if p:
            polys.append(p)
        return polys

    if local in ("MultiSurface", "MultiPolygon", "Surface"):
        from lxml import etree  # already validated to be installed
        for poly_el in element.iter(f"{{{gml_ns}}}Polygon"):
            p = _parse_gml_polygon(poly_el, gml_ns)
            if p:
                polys.append(p)
        return polys

    # Recurse into children that share the same GML namespace
    for child in element:
        child_ns = (
            child.tag.split("}")[0].lstrip("{") if "}" in child.tag else ""
        )
        if child_ns == gml_ns:
            polys.extend(_collect_gml_polygons(child, gml_ns))

    return polys


def _parse_gml_polygon(element: Any, gml_ns: str) -> Optional[Any]:
    """Parse a single gml:Polygon element into a Shapely Polygon."""
    from shapely.geometry import Polygon

    exterior: list[tuple[float, float]] = []
    holes: list[list[tuple[float, float]]] = []

    for child in element:
        local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        # NOTE: do NOT use `or` on lxml elements — truth-testing of elements
        # is unreliable (FutureWarning: text-only elements evaluate as False).
        pos_list = child.find(f".//{{{gml_ns}}}posList")
        if pos_list is None:
            pos_list = child.find(f".//{{{gml_ns}}}coordinates")
        if pos_list is None or not pos_list.text:
            continue
        coords = _parse_pos_list(
            pos_list.text,
            dimension=_infer_gml_coordinate_dimension(pos_list, child, element),
        )
        if local in ("exterior", "outerBoundaryIs"):
            exterior = coords
        elif local in ("interior", "innerBoundaryIs"):
            holes.append(coords)

    if len(exterior) < 3:
        return None
    try:
        return Polygon(exterior, holes)
    except Exception as exc:
        logger.debug("[WFS] GML Polygon creation failed: %s", exc)
        return None


def _infer_gml_coordinate_dimension(*elements: Any) -> int:
    """Infer GML coordinate tuple size from srsDimension / dimension attributes."""
    for element in elements:
        if element is None:
            continue
        for attr_name in ("srsDimension", "dimension"):
            value = getattr(element, "get", lambda *_: None)(attr_name)
            if value and str(value).isdigit():
                dimension = int(value)
                if dimension >= 2:
                    return dimension
    return 2


def _parse_pos_list(text: str, dimension: int = 2) -> list[tuple[float, float]]:
    """Parse GML coordinate string and project any 3D tuples down to (x, y)."""
    try:
        nums = list(map(float, text.split()))
    except ValueError:
        return []
    if dimension < 2:
        dimension = 2
    return [
        (nums[i], nums[i + 1])
        for i in range(0, len(nums) - (dimension - 1), dimension)
        if i + 1 < len(nums)
    ]


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def _reproject_to_2180(
    geom: BaseGeometry, source_srid: int, swap_xy: bool = False
) -> BaseGeometry:
    """Reproject a Shapely geometry from source_srid to EPSG:2180.

    swap_xy=True: swap (x, y) before reprojection — needed when the WFS server
    returns coordinates in standard EPSG axis order (Northing, Easting) for
    projected CRS, while always_xy=True expects (Easting, Northing).
    """
    if swap_xy:
        geom = shapely_transform(lambda x, y, *args: (y, x), geom)
    transformer = Transformer.from_crs(
        f"EPSG:{source_srid}", "EPSG:2180", always_xy=True
    )
    return shapely_transform(transformer.transform, geom)


def _ensure_multipolygon(geom: BaseGeometry) -> Optional[BaseGeometry]:
    """Cast Polygon → MultiPolygon; discard non-polygonal geometries."""
    from shapely.geometry import MultiPolygon, Polygon, GeometryCollection

    if isinstance(geom, MultiPolygon):
        return geom
    if isinstance(geom, Polygon):
        return MultiPolygon([geom])
    if isinstance(geom, GeometryCollection):
        polys = [g for g in geom.geoms if isinstance(g, (Polygon, MultiPolygon))]
        if polys:
            from shapely.ops import unary_union
            merged = unary_union(polys)
            return _ensure_multipolygon(merged)
    return None


def _get_prop(props: dict[str, Any], key: str) -> Optional[str]:
    """Get a property value, trying lowercase and original key."""
    v = props.get(key) or props.get(key.lower()) or props.get(key.upper())
    return str(v).strip() if v is not None and str(v).strip() else None


def _parse_date(value: Optional[str]) -> Optional[date]:
    """Try common date formats for WFS date fields."""
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d.%m.%Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    logger.debug("[WFS] Unrecognised date format: %r", value)
    return None


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------

async def run_wfs_ingest(
    wfs_url: str,
    layer_name: str,
    plan_type: str,
    teryt_gmina: str,
    source_srid: int = 2180,
    cql_filter: Optional[str] = None,
    field_mapping: Optional[WFSFieldMapping] = None,
    wfs_version: str = "2.0.0",
    prefer_json: bool = True,
    swap_xy: bool = False,
) -> WFSIngestReport:
    """Run a single WFS ingestion cycle — usable from Cloud Run Jobs or CLI."""
    from app.core.database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        async with WFSClient() as client:
            return await client.fetch_and_ingest(
                db=db,
                wfs_url=wfs_url,
                layer_name=layer_name,
                plan_type=plan_type,
                teryt_gmina=teryt_gmina,
                source_srid=source_srid,
                cql_filter=cql_filter,
                field_mapping=field_mapping,
                wfs_version=wfs_version,
                prefer_json=prefer_json,
                swap_xy=swap_xy,
            )


if __name__ == "__main__":
    import asyncio as _asyncio
    import logging as _logging

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    # Example: Krajowa Integracja MPZP — override wfs_url to use in production
    _report = _asyncio.run(run_wfs_ingest(
        wfs_url="https://integracja.gugik.gov.pl/cgi-bin/KrajowaIntegracjaUzytkowaniaTerenu",
        layer_name="app:ump_strefy_przeznaczenia",
        plan_type="mpzp",
        teryt_gmina="1412011",
        source_srid=4326,
        cql_filter="gmina_teryt='1412011'",
    ))

    print(f"\n{'='*60}")
    print("WFS INGEST COMPLETE")
    print(f"{'='*60}")
    print(f"  Layer           : {_report.layer}")
    print(f"  Gmina TERYT     : {_report.teryt_gmina}")
    print(f"  Fetched         : {_report.features_fetched}")
    print(f"  Valid           : {_report.features_valid}")
    print(f"  Upserted        : {_report.features_upserted}")
    print(f"  Skipped (bounds): {_report.features_skipped_bounds}")
    print(f"  Failed          : {_report.features_failed}")
    print(f"  Duration        : {_report.duration_s}s")
    print(f"{'='*60}")
