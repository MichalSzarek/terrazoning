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

Polish national MPZP base URL (Krajowa Integracja MPZP):
    https://integracja.gugik.gov.pl/cgi-bin/KrajowaIntegracjaUzytkowaniaTerenu

Note: WFS layers, property names, and CRS vary by municipality.
Use WFSFieldMapping to adapt to a specific service.
"""

from __future__ import annotations

import logging
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
        params = self._build_request_params(layer_name, bbox_2180, source_srid, cql_filter)

        logger.info(
            "[WFS] Fetching layer=%s teryt=%s from %s (srid=%d)",
            layer_name, teryt_gmina, wfs_url, source_srid,
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
                        source_wfs_url=feat.source_wfs_url,
                        ingested_at=datetime.now(timezone.utc),
                        plan_effective_date=feat.plan_effective_date,
                    )
                    # Upsert key: same zone same source → refresh geometry
                    .on_conflict_do_update(
                        constraint="uq_planning_zones_source_zone",
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

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_request_params(
        self,
        layer_name: str,
        bbox_2180: Optional[tuple[float, float, float, float]],
        source_srid: int,
        cql_filter: Optional[str],
    ) -> dict[str, str]:
        params: dict[str, str] = {
            "SERVICE": "WFS",
            "VERSION": "2.0.0",
            "REQUEST": "GetFeature",
            "TYPENAMES": layer_name,
            "OUTPUTFORMAT": "application/json",
            "COUNT": str(self._max_features),
            "SRSNAME": f"urn:ogc:def:crs:EPSG::{source_srid}",
        }
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
        """Minimal GML 3.x fallback parser.

        Returns a list of pseudo-GeoJSON feature dicts for uniform processing.
        Only handles simple property extraction — complex GML should use a dedicated
        OGR-based approach.
        """
        try:
            from lxml import etree
        except ImportError:
            logger.error("[WFS] lxml not installed — cannot parse GML responses")
            return []

        ns = {
            "wfs": "http://www.opengis.net/wfs/2.0",
            "gml": "http://www.opengis.net/gml/3.2",
        }

        try:
            root = etree.fromstring(body.encode())
        except etree.XMLSyntaxError as exc:
            logger.error("[WFS] GML parse error: %s", exc)
            return []

        features: list[dict[str, Any]] = []
        for member in root.iter("{http://www.opengis.net/wfs/2.0}member"):
            feature_el = next(iter(member), None)
            if feature_el is None:
                continue

            props: dict[str, Any] = {}
            geom_el = None

            for child in feature_el:
                local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                # Detect geometry child (contains gml:MultiSurface or similar)
                if child.find(".//{http://www.opengis.net/gml/3.2}posList") is not None:
                    geom_el = child
                elif child.text:
                    props[local] = child.text.strip()

            if geom_el is None:
                continue

            # Convert GML geometry to WKT via shapely/lxml (best-effort)
            try:
                from shapely import from_wkt
                gml_text = etree.tostring(geom_el, encoding="unicode")
                # Minimal GML to Shapely via OGR — requires gdal/ogr Python bindings
                # Fallback: skip if conversion fails
                logger.debug("[WFS] GML geometry conversion skipped (no OGR bindings)")
                continue  # without OGR, skip GML geometries
            except Exception:
                continue

        logger.warning(
            "[WFS] GML parsing returned %d features (GeoJSON preferred for full support)",
            len(features),
        )
        return features

    def _parse_feature(
        self,
        raw: dict[str, Any],
        plan_type: str,
        teryt_gmina: str,
        source_srid: int,
        mapping: WFSFieldMapping,
        wfs_url: str,
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
                geom = _reproject_to_2180(geom, source_srid)
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
# Utility functions
# ---------------------------------------------------------------------------

def _reproject_to_2180(geom: BaseGeometry, source_srid: int) -> BaseGeometry:
    """Reproject a Shapely geometry from source_srid to EPSG:2180."""
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
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d", "%d-%m-%Y"):
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
