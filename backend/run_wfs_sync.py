"""Operational WFS sync — loads real MPZP planning zones for resolved parcels.

Iterates over distinct teryt_gmina codes found in silver.dzialki and, for each
code that has a known WFS endpoint in WFS_REGISTRY, fetches the planning zone
polygons and upserts them into gold.planning_zones.

National WFS status (2025):
    The old national WFS (integracja.gugik.gov.pl) is permanently offline.
    GUGiK now provides only a WMS aggregation — no polygon download.
    Data must be sourced from municipality-level WFS/REST APIs.

WFS_REGISTRY covers confirmed endpoints for:
    Śląskie  : Katowice (2469011), Gliwice (2466011), Szczekociny (2416085)
    Małopolska: Kraków (1261011)

Adding a new municipality:
    1. Find the city's WFS endpoint (geoportal, MSIP, e-mapa.net).
    2. Run:  uv run python run_wfs_sync.py --probe <URL> --layer <LAYER>
    3. Note the field names from GetCapabilities DescribeFeatureType.
    4. Add a WFSRegistryEntry to WFS_REGISTRY below.

Usage:
    uv run python run_wfs_sync.py               # sync all gminy in registry
    uv run python run_wfs_sync.py --limit 3     # sync first 3 matched gminy
    uv run python run_wfs_sync.py --teryt 2469011   # sync single gmina
    uv run python run_wfs_sync.py --wfs-url <URL> --layer-name <LAYER> --teryt <CODE>
    uv run python run_wfs_sync.py -v            # verbose / debug output
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import zipfile
from dataclasses import asdict, dataclass, field
from io import BytesIO

import httpx
from sqlalchemy import distinct, select, text

from app.core.database import AsyncSessionLocal
from app.models.silver import Dzialka
from app.services.operations_scope import (
    coverage_alias_teryt,
    normalize_province,
    province_display_name,
    province_teryt_prefix,
)
from app.services.wfs_downloader import (
    WFSFieldMapping,
    WFSIngestReport,
    run_wfs_ingest,
)
from app.services.gison_raster_ingestor import (
    fetch_gison_wykazplanow,
    has_manual_gison_legend_override,
    probe_gison_raster_source,
    run_gison_raster_ingest,
)
from app.services.wms_grid_ingestor import run_wms_grid_ingest

logger = logging.getLogger("run_wfs_sync")

_RE_TERYT = re.compile(r"^\d{7}$")
_RE_APP_XML_ENTRY = re.compile(r"\.(xml|gml)$", re.IGNORECASE)
_APP_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "pl,en-US;q=0.9,en;q=0.8",
}

_UNCOVERED_COVERAGE_OVERRIDES: dict[str, tuple[str, str]] = {
    "2405011": (
        "no_source_available",
        "Public e-mapa assets currently fail with connection errors and the remaining GISON endpoint only exposes facade coverage metadata, not safe zoning semantics.",
    ),
    "2404042": (
        "gison_raster_candidate",
        "Plan 003 exposes live WMS + GeoTIFF assets for the active parcel and now has a conservative legend override candidate centered on the sampled KD-d road swatch.",
    ),
    "2403052": (
        "gison_raster_candidate",
        "Public GISON parcel-popup assets expose plan-specific designations for overlapping plans (2012, 2016, 2024); promote through query-backed parcel sampling rather than raw raster legend guesses.",
    ),
    "1201022": (
        "gison_raster_candidate",
        "Public plan-specific WMS/GeoTIFF assets are live; continue the raster/legend workstream and promote only after safe parcel-level semantics are confirmed.",
    ),
    "1201092": (
        "gison_raster_candidate",
        "Plan 001 already yields a stable parcel sample color; continue the raster/legend workstream and add a manual override only when the legend is verified.",
    ),
    "1215082": (
        "gison_raster_candidate",
        "Public GISON raster assets and parcel-linked legend/uchwała links are live; continue source work with query-backed or manual-legend ingest instead of treating the gmina as source-less.",
    ),
    "1206105": (
        "no_source_available",
        "Continue MPZP source discovery; current public endpoints expose APP/metadata but not usable zoning features.",
    ),
    "1216145": (
        "no_source_available",
        "Continue source discovery and check local SIP/BIP for municipal MPZP assets.",
    ),
}


# ---------------------------------------------------------------------------
# WFS Registry
# ---------------------------------------------------------------------------

@dataclass
class WFSRegistryEntry:
    """Configuration for a single municipality's MPZP WFS endpoint."""
    label: str                          # Human-readable city/gmina name
    wfs_url: str                        # WFS base URL
    layer_name: str                     # WFS TYPENAMES / layer identifier
    source_srid: int                    # CRS of the WFS source
    field_mapping: WFSFieldMapping      # Maps WFS property names → PlanningZone columns
    source_kind: str = "wfs"           # 'wfs' | 'wms_grid' | 'gison_raster'
    plan_type: str = "mpzp"
    wfs_version: str = "2.0.0"         # ArcGIS often needs "1.1.0"
    prefer_json: bool = True            # Set False for ArcGIS WFS (no JSON output)
    cql_filter: str | None = None       # Optional server-side filter
    swap_xy: bool = False               # True when server returns (Northing, Easting) order
    info_format: str = "text/plain"     # WMS GetFeatureInfo format for source_kind=wms_grid
    styles: str = "default"
    sample_grid: int = 5
    point_halfspan_m: float = 6.0
    parser_name: str = "ruda_plaintext"
    legend_url: str | None = None
    min_confidence: float = 0.35
    plan_name: str | None = None
    geotiff_url: str | None = None
    sample_bbox_2180: tuple[float, float, float, float] | None = None
    query_url_template: str | None = None
    fallback_designation: str | None = None
    fallback_description: str | None = None


# Confirmed WFS endpoints for municipalities present in silver.dzialki.
# TERYT gmina code → WFSRegistryEntry
#
# EPSG codes used:
#   2177 = PUWG 2000 zone 5  (λ₀=18°, West Poland — Bielsko-Biała, Częstochowa)
#   2178 = PUWG 2000 zone 6  (λ₀=21°, Central Poland — Katowice, Kraków)
#   2180 = PUWG 1992         (single national zone — most national datasets)
WFS_REGISTRY: dict[str, WFSRegistryEntry | list[WFSRegistryEntry]] = {

    # ------------------------------------------------------------------
    # Śląskie — Katowice (powiat grodzki, 2469011)
    # Source: emapa.katowice.eu  /  JARC system
    # Layers: MPZP:Plany_obowiązujące (plan boundaries)
    #         MPZP:MPZP_Tereny        (land-use zones — USE THIS)
    # WFS: ArcGIS Server — no JSON output, returns GML 3.1.1
    # ------------------------------------------------------------------
    "2469011": [
        WFSRegistryEntry(
            label="Katowice",
            wfs_url="https://emapa.katowice.eu/arcgis/services/MPZP/MapServer/WFSServer",
            layer_name="MPZP:MPZP_Tereny",
            source_srid=2177,   # Katowice in CS2000/6 zone (EPSG:2177, λ₀=18°)
            wfs_version="1.1.0",
            prefer_json=False,
            field_mapping=WFSFieldMapping(
                przeznaczenie="WR_SYMBOL",
                przeznaczenie_opis="OPIS_PRZEZN",
                plan_name="PLAN_ID",
                uchwala_nr="NRUCHWALY",
                plan_effective_date="DATA_WEJSCIA",
                teryt_gmina="",          # city-specific service, no gmina field
            ),
        ),
        WFSRegistryEntry(
            label="Katowice Studium",
            wfs_url="https://emapa.katowice.eu/arcgis/services/Studium/MapServer/WFSServer",
            layer_name="Studium:Obszary_polityki_przestrzennej",
            source_srid=2177,
            plan_type="studium",
            wfs_version="2.0.0",
            prefer_json=False,
            swap_xy=True,
            field_mapping=WFSFieldMapping(
                przeznaczenie="SYMB_KIER_PRZEZN",
                przeznaczenie_opis="OZNACZ_OBSZ",
                plan_name="STUDIUM_ID",
                uchwala_nr="",
                plan_effective_date="DATA_UTW",
                teryt_gmina="",
            ),
        ),
    ],

    # ------------------------------------------------------------------
    # Śląskie — Gliwice city (powiat grodzki, 2466011)
    # Source: geoportal.gliwice.eu  /  MSIP Gliwice
    # Layers confirmed: G_RIPP_STREFA_OBOW  (binding land-use zones)
    # WFS: GeoServer — supports JSON output natively
    # Fields: SYMPRZEZ (short symbol), PRZEZNACZENIE (full name),
    #         TYTUL (plan title), NRUCHWALY (resolution nr), OBOWIAZUJEOD (date)
    # ------------------------------------------------------------------
    "2466011": [
        WFSRegistryEntry(
            label="Gliwice",
            wfs_url="https://geoportal.gliwice.eu/isdp/gs/ows/wfs-mpzp",
            layer_name="G_RIPP_STREFA_OBOW",
            source_srid=2177,
            wfs_version="2.0.0",
            prefer_json=True,
            # GeoServer returns (Northing, Easting) when CRS axis order is northing-first
            swap_xy=True,
            field_mapping=WFSFieldMapping(
                przeznaczenie="SYMPRZEZ",       # short symbol: MN, U, ZP …
                przeznaczenie_opis="PRZEZNACZENIE",
                plan_name="TYTUL",
                uchwala_nr="NRUCHWALY",
                plan_effective_date="OBOWIAZUJEOD",
                teryt_gmina="",
            ),
        ),
        WFSRegistryEntry(
            label="Gliwice Studium",
            wfs_url="https://geoportal.gliwice.eu/isdp/gs/ows/wfs-suikzp",
            layer_name="default:RippStrefaStudium",
            source_srid=2177,
            plan_type="studium",
            wfs_version="2.0.0",
            prefer_json=False,
            swap_xy=True,
            field_mapping=WFSFieldMapping(
                przeznaczenie="symPrzez",
                przeznaczenie_opis="przeznaczenie",
                plan_name="nazwaOficjalna",
                uchwala_nr="nrUchwaly",
                plan_effective_date="obowiazujeOd",
                teryt_gmina="",
            ),
        ),
    ],

    # ------------------------------------------------------------------
    # Śląskie — Szczekociny (gmina miejsko-wiejska, 2416085)
    # Source: mapa.inspire-hub.pl / Gmina Szczekociny
    # Layer: mpzp (zoning polygons with designation symbol + description)
    # WFS: QGIS Server — GeoJSON output works and returns lon/lat coordinates
    # Fields: oznaczenie (symbol), opis (description), numer_uchwaly (resolution nr)
    # ------------------------------------------------------------------
    "2416085": WFSRegistryEntry(
        label="Szczekociny",
        wfs_url="https://mapa.inspire-hub.pl/ows/gmina_szczekociny",
        layer_name="mpzp",
        source_srid=4326,
        wfs_version="1.1.0",
        prefer_json=True,
        field_mapping=WFSFieldMapping(
            przeznaczenie="oznaczenie",
            przeznaczenie_opis="opis",
            plan_name="numer_uchwaly",
            uchwala_nr="numer_uchwaly",
            plan_effective_date="",
            teryt_gmina="",
        ),
    ),

    # ------------------------------------------------------------------
    # Śląskie — Orzesze (gmina miejska, 2408031)
    # Source: e-mapa / iGeoMap vector MPZP service
    # WFS exposes zoning polygons as lay63 ("Przeznaczenia terenu")
    # Fields:
    #   etykieta -> short designation symbol (e.g. 1UE, U.11, MN)
    #   opis     -> full designation description
    #   plan     -> local plan code
    # WFS: MapServer 1.1.0, GML only (GeoJSON / WFS 2.0 unavailable)
    # ------------------------------------------------------------------
    "2408031": WFSRegistryEntry(
        label="Orzesze",
        wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/240803",
        layer_name="lay63",
        source_srid=2180,
        wfs_version="1.1.0",
        prefer_json=False,
        field_mapping=WFSFieldMapping(
            przeznaczenie="etykieta",
            przeznaczenie_opis="opis",
            plan_name="plan",
            uchwala_nr="",
            plan_effective_date="",
            teryt_gmina="",
        ),
    ),

    # ------------------------------------------------------------------
    # Małopolska — Gmina Oświęcim (gmina wiejska, 1213062)
    # Source: iGeoMap vector MPZP service
    # WFS exposes zoning polygons as lay63 ("Przeznaczenia terenu")
    # Fields:
    #   etykieta -> short designation symbol
    #   opis     -> full designation description
    #   plan     -> local plan code
    # WFS: MapServer 1.1.0, GML only
    # ------------------------------------------------------------------
    "1213062": [
        WFSRegistryEntry(
            label="Gmina Oświęcim",
            wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/121306",
            layer_name="lay63",
            source_srid=2180,
            wfs_version="1.1.0",
            prefer_json=False,
            field_mapping=WFSFieldMapping(
                przeznaczenie="etykieta",
                przeznaczenie_opis="opis",
                plan_name="plan",
                uchwala_nr="",
                plan_effective_date="",
                teryt_gmina="",
            ),
        ),
        WFSRegistryEntry(
            label="Gmina Oświęcim Studium",
            wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/121306/studium",
            layer_name="lay63",
            source_srid=2180,
            plan_type="studium",
            wfs_version="1.1.0",
            prefer_json=False,
            field_mapping=WFSFieldMapping(
                przeznaczenie="etykieta",
                przeznaczenie_opis="opis",
                plan_name="plan",
                uchwala_nr="",
                plan_effective_date="",
                teryt_gmina="",
            ),
        ),
    ],

    # ------------------------------------------------------------------
    # Małopolska — Brzeszcze (obszar wiejski, 1213024)
    # Source: iGeoMap vector MPZP service
    # WFS exposes zoning polygons as lay63 ("Przeznaczenia terenu")
    # WFS: MapServer 1.1.0, GML only
    # ------------------------------------------------------------------
    "1213024": WFSRegistryEntry(
        label="Brzeszcze",
        wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/121302",
        layer_name="lay63",
        source_srid=2180,
        wfs_version="1.1.0",
        prefer_json=False,
        field_mapping=WFSFieldMapping(
            przeznaczenie="etykieta",
            przeznaczenie_opis="opis",
            plan_name="plan",
            uchwala_nr="",
            plan_effective_date="",
            teryt_gmina="",
        ),
    ),

    # ------------------------------------------------------------------
    # Małopolska — Nowy Wiśnicz (obszar wiejski, 1201065)
    # Source: iGeoMap vector MPZP service
    # WFS exposes zoning polygons as lay63 ("Przeznaczenia terenu")
    # WFS: MapServer 1.1.0, GML only
    # ------------------------------------------------------------------
    "1201065": [
        WFSRegistryEntry(
            label="Nowy Wiśnicz",
            wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/120106",
            layer_name="lay63",
            source_srid=2180,
            wfs_version="1.1.0",
            prefer_json=False,
            field_mapping=WFSFieldMapping(
                przeznaczenie="etykieta",
                przeznaczenie_opis="opis",
                plan_name="plan",
                uchwala_nr="",
                plan_effective_date="",
                teryt_gmina="",
            ),
        ),
        WFSRegistryEntry(
            label="Nowy Wiśnicz Studium",
            wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/120106/studium",
            layer_name="lay63",
            source_srid=2180,
            plan_type="studium",
            wfs_version="1.1.0",
            prefer_json=False,
            field_mapping=WFSFieldMapping(
                przeznaczenie="etykieta",
                przeznaczenie_opis="opis",
                plan_name="plan",
                uchwala_nr="",
                plan_effective_date="",
                teryt_gmina="",
            ),
        ),
    ],

    # ------------------------------------------------------------------
    # Małopolska — Zakopane (powiat grodzki, 1217011)
    # Source: iGeoMap vector MPZP service
    # WFS exposes zoning polygons as lay63 ("Przeznaczenia terenu")
    # WFS: MapServer 1.1.0, GML only
    # ------------------------------------------------------------------
    "1217011": [
        WFSRegistryEntry(
            label="Zakopane",
            wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/121701",
            layer_name="lay63",
            source_srid=2180,
            wfs_version="1.1.0",
            prefer_json=False,
            field_mapping=WFSFieldMapping(
                przeznaczenie="etykieta",
                przeznaczenie_opis="opis",
                plan_name="plan",
                uchwala_nr="",
                plan_effective_date="",
                teryt_gmina="",
            ),
        ),
        WFSRegistryEntry(
            label="Zakopane Studium",
            wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/121701/studium",
            layer_name="lay63",
            source_srid=2180,
            plan_type="studium",
            wfs_version="1.1.0",
            prefer_json=False,
            field_mapping=WFSFieldMapping(
                przeznaczenie="etykieta",
                przeznaczenie_opis="opis",
                plan_name="plan",
                uchwala_nr="",
                plan_effective_date="",
                teryt_gmina="",
            ),
        ),
    ],

    # ------------------------------------------------------------------
    # Małopolska — Wadowice / Chocznia (obszar wiejski, 1218095)
    # Source: iGeoMap vector MPZP service
    # WFS exposes zoning polygons as lay63 ("Przeznaczenia terenu")
    # WFS: MapServer 1.1.0, GML only
    # ------------------------------------------------------------------
    "1218095": [
        WFSRegistryEntry(
            label="Wadowice / Chocznia",
            wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/121809",
            layer_name="lay63",
            source_srid=2180,
            wfs_version="1.1.0",
            prefer_json=False,
            field_mapping=WFSFieldMapping(
                przeznaczenie="etykieta",
                przeznaczenie_opis="opis",
                plan_name="plan",
                uchwala_nr="",
                plan_effective_date="",
                teryt_gmina="",
            ),
        ),
        WFSRegistryEntry(
            label="Wadowice / Chocznia Studium",
            wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/121809/studium",
            layer_name="lay63",
            source_srid=2180,
            plan_type="studium",
            wfs_version="1.1.0",
            prefer_json=False,
            field_mapping=WFSFieldMapping(
                przeznaczenie="etykieta",
                przeznaczenie_opis="opis",
                plan_name="plan",
                uchwala_nr="",
                plan_effective_date="",
                teryt_gmina="",
            ),
        ),
    ],

    # ------------------------------------------------------------------
    # Małopolska — Jabłonka / Zubrzyca Górna (obszar wiejski, 1211052)
    # Source: e-mapa / iGeoMap raster MPZP service
    # Public wykazplanow exposes plan 002 with live WMS + legend assets.
    # Current parcel sampling requires axis swap to align with the plan source.
    # ------------------------------------------------------------------
    "1211052": WFSRegistryEntry(
        label="Jabłonka / Zubrzyca Górna",
        wfs_url="https://mpzp.igeomap.pl/cgi-bin/plany/121105/002",
        layer_name="plany002",
        source_srid=2180,
        source_kind="gison_raster",
        wfs_version="1.1.1",
        prefer_json=False,
        swap_xy=True,
        legend_url="https://mpzp.igeomap.pl/doc/nowytarg/jablonka/002.jpg",
        plan_name="Jabłonka MPZP 002",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Małopolska — Andrychów (miasto, 1218014)
    # Source: e-mapa / iGeoMap raster MPZP service
    # Plan 06 exposes a working GeoTIFF and public PDF legend, but its WMS
    # endpoint is dead. Parcel-aligned sampling uses the GeoTIFF asset with
    # bbox axis swap detected by parcel-aware probes.
    # ------------------------------------------------------------------
    "1218014": WFSRegistryEntry(
        label="Andrychów",
        wfs_url="https://mpzp.igeomap.pl/cgi-bin/plany/121801/06",
        layer_name="plany06",
        source_srid=2180,
        source_kind="gison_raster",
        wfs_version="1.1.1",
        prefer_json=False,
        swap_xy=True,
        legend_url="https://mpzp.igeomap.pl/doc/wadowice/andrychow/06.pdf",
        geotiff_url="https://andrychow.e-mapa.net/wykazplanow/tiff/121801/06",
        sample_bbox_2180=(
            522108.36900690314,
            218814.17189786863,
            526644.0156603662,
            222747.11112049688,
        ),
        plan_name="Andrychów MPZP 06",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Małopolska — Skawina (gmina miejsko-wiejska, 1206114)
    # Source: GISON MPZP portal with parcel-level GetPortalInfo semantics.
    # Public WMS is queryable, but the reliable designation symbol comes from
    # the portal popup HTML, not from public WFS vectors.
    # ------------------------------------------------------------------
    "1206114": WFSRegistryEntry(
        label="Skawina",
        wfs_url="https://rastry.gison.pl/cgi-bin/mapserv?map=/home/vboxuser/mpzp/skawina_web.map",
        layer_name="mpzp_gmina",
        source_srid=3857,
        source_kind="wms_grid",
        wfs_version="1.3.0",
        prefer_json=False,
        parser_name="gison_portal_html",
        query_url_template=(
            "https://mpzp.gison.pl/app/Feature/GetPortalInfo"
            "?profil=skawina&layers=MPZP&lng=%lon%&lat=%lat%"
        ),
        plan_name="Skawina MPZP portal",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Małopolska — Wielka Wieś / Giebułtów (gmina wiejska, 1206152)
    # Source: GISON MPZP portal with parcel-level GetPortalInfo semantics.
    # The advertised vector endpoint is not safely fetchable yet, but the popup
    # endpoint returns designation, uchwała and legend URLs for real parcels.
    # ------------------------------------------------------------------
    "1206152": WFSRegistryEntry(
        label="Wielka Wieś / Giebułtów",
        wfs_url="https://rastry.gison.pl/cgi-bin/mapserv?map=/home/vboxuser/mpzp/wielkawies_web.map",
        layer_name="mpzp",
        source_srid=3857,
        source_kind="wms_grid",
        wfs_version="1.3.0",
        prefer_json=False,
        parser_name="gison_portal_html",
        query_url_template=(
            "https://mpzp.gison.pl/app/Feature/GetPortalInfo"
            "?profil=wielkawies&layers=MPZP&przeznaczenia=tak&lng=%lon%&lat=%lat%"
        ),
        plan_name="Wielka Wieś MPZP portal",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Małopolska — Gołcza (gmina wiejska, 1208022)
    # Source: GISON MPZP portal with parcel-level GetPortalInfo semantics.
    # Popup HTML returns live plan metadata and rural designation symbols.
    # ------------------------------------------------------------------
    "1208022": WFSRegistryEntry(
        label="Gołcza",
        wfs_url="https://rastry.gison.pl/cgi-bin/mapserv?map=/home/vboxuser/mpzp/golcza_web.map",
        layer_name="mpzp",
        source_srid=3857,
        source_kind="wms_grid",
        wfs_version="1.3.0",
        prefer_json=False,
        parser_name="gison_portal_html",
        query_url_template=(
            "https://mpzp.gison.pl/app/Feature/GetPortalInfo"
            "?profil=golcza&layers=MPZP&lng=%lon%&lat=%lat%"
        ),
        plan_name="Gołcza MPZP portal",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Śląskie — Chybie / Frelichów / Zarzecze (gmina wiejska, 2403052)
    # Source: GISON parcel-level MPZP popup endpoint. This avoids the unsafe
    # multi-plan raster guessing path and uses public designation semantics
    # returned directly for parcel points.
    # ------------------------------------------------------------------
    "2403052": WFSRegistryEntry(
        label="Chybie / Frelichów / Zarzecze",
        wfs_url="https://rastry.gison.pl/cgi-bin/mapserv?map=/home/vboxuser/mpzp/chybie_web.map",
        layer_name="mpzp",
        source_srid=3857,
        source_kind="wms_grid",
        wfs_version="1.3.0",
        prefer_json=False,
        parser_name="gison_portal_html",
        query_url_template=(
            "https://mpzp.gison.pl/app/Feature/GetPortalInfoDzialka"
            "?profil=chybie&layers=MPZP&lng=%lon%&lat=%lat%"
        ),
        plan_name="Chybie MPZP portal",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Śląskie — Knurów (gmina miejska, 2405011)
    # Source: GISON MPZP portal with parcel-level designation semantics.
    # The public popup returns plan metadata and explicit zoning symbols for
    # the active parcel in Knurów.
    # ------------------------------------------------------------------
    "2405011": WFSRegistryEntry(
        label="Knurów",
        wfs_url="https://rastry.gison.pl/cgi-bin/mapserv?map=/home/vboxuser/mpzp/knurow_web.map",
        layer_name="mpzp",
        source_srid=3857,
        source_kind="wms_grid",
        wfs_version="1.3.0",
        prefer_json=False,
        parser_name="gison_portal_html",
        query_url_template=(
            "https://mpzp.gison.pl/app/Feature/GetPortalInfoDzialka"
            "?profil=knurow&layers=MPZP&przeznaczenia=tak"
            "&lng=%lon%&lat=%lat%&zoom=17&zmianyrevert=true&zmianyzmian=true"
        ),
        plan_name="Knurów MPZP portal",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Śląskie — Pszczyna / Łąka (gmina miejsko-wiejska, 2410055)
    # Source: GISON MPZP portal with parcel-level designation semantics.
    # The public popup for the active Łąka parcel returns a modern MPZP plan
    # and explicit zoning symbol (e.g. 32MN1).
    # ------------------------------------------------------------------
    "2410055": WFSRegistryEntry(
        label="Pszczyna / Łąka",
        wfs_url="https://rastry.gison.pl/cgi-bin/mapserv?map=/home/vboxuser/mpzp/pszczyna_web.map",
        layer_name="mpzp",
        source_srid=3857,
        source_kind="wms_grid",
        wfs_version="1.3.0",
        prefer_json=False,
        parser_name="gison_portal_html",
        query_url_template=(
            "https://mpzp.gison.pl/app/Feature/GetPortalInfoDzialka"
            "?profil=pszczyna&layers=MPZP&przeznaczenia=tak&lng=%lon%&lat=%lat%"
        ),
        plan_name="Pszczyna MPZP portal",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Śląskie — Czerwionka-Leszczyny / Dębieńsko (gmina miejsko-wiejska, 2412014)
    # Source: GISON MPZP portal with confirmed parcel-level plan membership.
    # The public popup returns plan metadata and legend URL for the active
    # parcel, but omits the zoning symbol. We persist a conservative
    # non-buildable coverage placeholder so the gmina is tracked as "covered"
    # without inventing a buildable designation.
    # ------------------------------------------------------------------
    "2412014": WFSRegistryEntry(
        label="Czerwionka-Leszczyny / Dębieńsko",
        wfs_url="https://rastry.gison.pl/cgi-bin/mapserv?map=/home/vboxuser/mpzp/czerwionkaleszczyny_web.map",
        layer_name="mpzp",
        source_srid=3857,
        source_kind="wms_grid",
        wfs_version="1.3.0",
        prefer_json=False,
        parser_name="gison_portal_html",
        query_url_template=(
            "https://mpzp.gison.pl/app/Feature/GetPortalInfoDzialka"
            "?profil=czerwionkaleszczyny&layers=MPZP&przeznaczenia=tak&lng=%lon%&lat=%lat%"
        ),
        fallback_designation="MPZP_UNK",
        fallback_description=(
            "MPZP coverage confirmed by the public GISON portal, "
            "but the popup omits the zoning symbol."
        ),
        plan_name="Czerwionka-Leszczyny MPZP portal",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Małopolska — Gorlice (miasto, 1205011)
    # Source: GISON MPZP portal using the `gorlicemiasto` profile slug.
    # Popup HTML returns designation semantics for real parcel centroids.
    # ------------------------------------------------------------------
    "1205011": WFSRegistryEntry(
        label="Gorlice",
        wfs_url="https://rastry.gison.pl/cgi-bin/mapserv?map=/home/vboxuser/mpzp/gorlicemiasto_web.map",
        layer_name="mpzp",
        source_srid=3857,
        source_kind="wms_grid",
        wfs_version="1.3.0",
        prefer_json=False,
        parser_name="gison_portal_html",
        query_url_template=(
            "https://mpzp.gison.pl/app/Feature/GetPortalInfo"
            "?profil=gorlicemiasto&layers=MPZP&lng=%lon%&lat=%lat%"
        ),
        plan_name="Gorlice MPZP portal",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Małopolska — Zawoja (gmina wiejska, 1215082)
    # Source: GISON raster MPZP service
    # Public WMS is live in EPSG:3857 and current Silver parcels land inside
    # plan Z01, whose public JPG legend is stable enough for a conservative
    # manual override.
    # ------------------------------------------------------------------
    "1215082": WFSRegistryEntry(
        label="Zawoja",
        wfs_url="https://rastry.gison.pl/cgi-bin/mapserv?map=/home/vboxuser/mpzp/zawoja_web.map",
        layer_name="mpzp",
        source_srid=3857,
        source_kind="gison_raster",
        wfs_version="1.1.1",
        prefer_json=False,
        legend_url="https://rastry.gison.pl/mpzp-public/zawoja/legendy/Z01_2019_84_X_legenda.jpg",
        plan_name="Zawoja MPZP Z01",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Małopolska — Żegocina / Bełdno (obszar wiejski, 1201092)
    # Source: e-mapa / iGeoMap raster MPZP service
    # Public wykazplanow exposes several overlapping plans; plan 001 is the
    # active raster source for the current parcel in Bełdno and uses a stable
    # orange swatch that maps conservatively to buildable housing/service use.
    # ------------------------------------------------------------------
    "1201092": WFSRegistryEntry(
        label="Żegocina / Bełdno",
        wfs_url="https://mpzp.igeomap.pl/cgi-bin/plany/120109/001",
        layer_name="plany001",
        source_srid=2180,
        source_kind="gison_raster",
        wfs_version="1.1.1",
        prefer_json=False,
        legend_url="https://mpzp.igeomap.pl/doc/bochnia/zegocina/001.jpg",
        plan_name="Żegocina MPZP 001",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Małopolska — Bochnia / Chełm (obszar wiejski, 1201022)
    # Source: e-mapa / iGeoMap raster MPZP service
    # Public wykazplanow exposes several plans, but only plan 003 returns a
    # stable non-transparent parcel sample for the active Chełm parcel.
    # Production sync uses a conservative manual legend override derived from
    # the rendered PDF legend and keeps the current parcel in the rural family.
    # ------------------------------------------------------------------
    "1201022": WFSRegistryEntry(
        label="Bochnia / Chełm",
        wfs_url="https://mpzp.igeomap.pl/cgi-bin/plany/120102/003",
        layer_name="plany003",
        source_srid=2180,
        source_kind="gison_raster",
        wfs_version="1.1.1",
        prefer_json=False,
        legend_url="https://mpzp.igeomap.pl/doc/bochnia/bochnia/003_legenda.pdf",
        geotiff_url="https://bochnia.e-mapa.net/wykazplanow/tiff/120102/003",
        sample_bbox_2180=(
            593297.478,
            224690.926,
            610009.63,
            249608.373,
        ),
        plan_name="Bochnia MPZP 003",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Śląskie — Jeleśnia (gmina wiejska, 2417042)
    # Source: e-mapa / iGeoMap raster MPZP service
    # Public wykazplanow exposes plan 009 with a live WMS, GeoTIFF asset,
    # and a public PDF legend. Production sync uses the plan-specific
    # manual legend override stored in gison_raster_ingestor.py.
    # ------------------------------------------------------------------
    "2417042": WFSRegistryEntry(
        label="Jeleśnia",
        wfs_url="https://mpzp.igeomap.pl/cgi-bin/plany/241704/009",
        layer_name="plany009",
        source_srid=2180,
        source_kind="gison_raster",
        wfs_version="1.1.1",
        prefer_json=False,
        legend_url="https://mpzp.igeomap.pl/doc/zywiec/jelesnia/009_legenda.pdf",
        plan_name="Jeleśnia MPZP 009",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Śląskie — Kamienica Polska (gmina wiejska, 2404042)
    # Source: e-mapa / iGeoMap raster MPZP service
    # Public wykazplanow exposes plan 003 with a live WMS, GeoTIFF asset,
    # and a public JPG legend. Production sync uses a conservative manual
    # legend override centered on the sampled KD-d road swatch for the active
    # parcel, with adjacent buildable/rural tones kept for future parcels.
    # ------------------------------------------------------------------
    "2404042": [
        WFSRegistryEntry(
            label="Kamienica Polska",
            wfs_url="https://mpzp.igeomap.pl/cgi-bin/plany/240404/003",
            layer_name="plany003",
            source_srid=2180,
            source_kind="gison_raster",
            wfs_version="1.1.1",
            prefer_json=False,
            legend_url="https://mpzp.igeomap.pl/doc/czestochowa/kamienicapolska/003.jpg",
            geotiff_url="https://kamienicapolska.e-mapa.net/wykazplanow/tiff/240404/003",
            plan_name="Kamienica Polska MPZP 003",
            field_mapping=WFSFieldMapping(),
        ),
        WFSRegistryEntry(
            label="Kamienica Polska Studium",
            wfs_url="https://mpzp.igeomap.pl/cgi-bin/plany/240404/studium",
            layer_name="lay63",
            source_srid=2180,
            plan_type="studium",
            wfs_version="1.1.0",
            prefer_json=False,
            field_mapping=WFSFieldMapping(
                przeznaczenie="etykieta",
                przeznaczenie_opis="opis",
                plan_name="plan",
                uchwala_nr="",
                plan_effective_date="",
                teryt_gmina="",
            ),
        ),
    ],

    # ------------------------------------------------------------------
    # Śląskie — Pawłowice / Warszowice (gmina wiejska, 2410042)
    # Source: e-mapa / iGeoMap raster MPZP service
    # Public wykazplanow exposes plan 011 with a live WMS, GeoTIFF asset,
    # and a public JPG legend. Production sync uses a conservative manual
    # legend override centered on the sampled service-zone swatch for the
    # active parcel in Warszowice.
    # ------------------------------------------------------------------
    "2410042": WFSRegistryEntry(
        label="Pawłowice / Warszowice",
        wfs_url="https://mpzp.igeomap.pl/cgi-bin/plany/241004/011",
        layer_name="plany011",
        source_srid=2180,
        source_kind="gison_raster",
        wfs_version="1.1.1",
        prefer_json=False,
        legend_url="https://mpzp.igeomap.pl/doc/pszczyna/pawlowice/011.jpg",
        geotiff_url="https://pawlowice.e-mapa.net/wykazplanow/tiff/241004/011",
        plan_name="Pawłowice MPZP 011",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Śląskie — Żarki / Przybynów (gmina miejsko-wiejska, 2409055)
    # Source: public GISON raster MPZP service.
    # The active parcel in Przybynów resolves to plan Z03 via GetPortalInfo
    # using WGS84 coordinates and a public PNG legend. Production sync uses
    # a conservative manual legend override centered on the sampled ML swatch.
    # ------------------------------------------------------------------
    "2409055": WFSRegistryEntry(
        label="Żarki / Przybynów",
        wfs_url="https://rastry.gison.pl/cgi-bin/mapserv?map=/home/vboxuser/mpzp/zarki_web.map",
        layer_name="app.RysunkiAktuPlanowania.MPZP",
        source_srid=3857,
        source_kind="gison_raster",
        wfs_version="1.1.1",
        prefer_json=False,
        legend_url="https://rastry.gison.pl/mpzp-public/zarki/legendy/Z03_2014_280_XLI_legenda.png",
        plan_name="Żarki MPZP Z03",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Śląskie — Mykanów / Grabowa (gmina wiejska, 2404112)
    # Source: e-mapa / iGeoMap raster MPZP service
    # Public wykazplanow exposes plan 082 with a live WMS, GeoTIFF asset,
    # and a public JPG legend. Production sync uses the plan-specific
    # manual legend override stored in gison_raster_ingestor.py.
    # ------------------------------------------------------------------
    "2404112": WFSRegistryEntry(
        label="Mykanów / Grabowa",
        wfs_url="https://mpzp.igeomap.pl/cgi-bin/plany/240411/082",
        layer_name="plany082",
        source_srid=2180,
        source_kind="gison_raster",
        wfs_version="1.1.1",
        prefer_json=False,
        legend_url="https://mpzp.igeomap.pl/doc/czestochowa/mykanow/082.jpg",
        geotiff_url="https://mykanow.pl/wp-content/uploads/2024/08/22_IV_2024_Rysunek.tif",
        plan_name="Mykanów MPZP 082",
        field_mapping=WFSFieldMapping(),
    ),

    # ------------------------------------------------------------------
    # Śląskie — Koziegłowy (obszar wiejski, 2409025)
    # Source: iGeoMap vector MPZP service
    # WFS exposes zoning polygons as lay63 ("Przeznaczenia terenu")
    # WFS: MapServer 1.1.0, GML only
    # ------------------------------------------------------------------
    "2409025": [
        WFSRegistryEntry(
            label="Koziegłowy",
            wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/240902",
            layer_name="lay63",
            source_srid=2180,
            wfs_version="1.1.0",
            prefer_json=False,
            field_mapping=WFSFieldMapping(
                przeznaczenie="etykieta",
                przeznaczenie_opis="opis",
                plan_name="plan",
                uchwala_nr="",
                plan_effective_date="",
                teryt_gmina="",
            ),
        ),
        WFSRegistryEntry(
            label="Koziegłowy Studium",
            wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/240902/studium",
            layer_name="lay63",
            source_srid=2180,
            plan_type="studium",
            wfs_version="1.1.0",
            prefer_json=False,
            field_mapping=WFSFieldMapping(
                przeznaczenie="etykieta",
                przeznaczenie_opis="opis",
                plan_name="plan",
                uchwala_nr="",
                plan_effective_date="",
                teryt_gmina="",
            ),
        ),
    ],

    # ------------------------------------------------------------------
    # Śląskie — Gilowice (gmina wiejska, 2417032)
    # Source: iGeoMap vector MPZP service
    # WFS exposes zoning polygons as lay63 ("Przeznaczenia terenu")
    # WFS: MapServer 1.1.0, GML only
    # ------------------------------------------------------------------
    "2417032": WFSRegistryEntry(
        label="Gilowice",
        wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/241703",
        layer_name="lay63",
        source_srid=2180,
        wfs_version="1.1.0",
        prefer_json=False,
        field_mapping=WFSFieldMapping(
            przeznaczenie="etykieta",
            przeznaczenie_opis="opis",
            plan_name="plan",
            uchwala_nr="",
            plan_effective_date="",
            teryt_gmina="",
        ),
    ),

    # ------------------------------------------------------------------
    # Śląskie — Łękawica (gmina wiejska, 2417072)
    # Source: iGeoMap vector MPZP service
    # WFS exposes zoning polygons as lay63 ("Przeznaczenia terenu")
    # WFS: MapServer 1.1.0, GML only
    # ------------------------------------------------------------------
    "2417072": WFSRegistryEntry(
        label="Łękawica",
        wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/241707",
        layer_name="lay63",
        source_srid=2180,
        wfs_version="1.1.0",
        prefer_json=False,
        field_mapping=WFSFieldMapping(
            przeznaczenie="etykieta",
            przeznaczenie_opis="opis",
            plan_name="plan",
            uchwala_nr="",
            plan_effective_date="",
            teryt_gmina="",
        ),
    ),

    # ------------------------------------------------------------------
    # Śląskie — Wilamowice (gmina miejsko-wiejska, 2402093)
    # Source: iGeoMap vector MPZP service
    # WFS exposes zoning polygons as lay63 ("Przeznaczenia terenu")
    # WFS: MapServer 1.1.0, GML only
    # ------------------------------------------------------------------
    "2402093": WFSRegistryEntry(
        label="Wilamowice",
        wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/240209",
        layer_name="lay63",
        source_srid=2180,
        wfs_version="1.1.0",
        prefer_json=False,
        field_mapping=WFSFieldMapping(
            przeznaczenie="etykieta",
            przeznaczenie_opis="opis",
            plan_name="plan",
            uchwala_nr="",
            plan_effective_date="",
            teryt_gmina="",
        ),
    ),

    # ------------------------------------------------------------------
    # Śląskie — Jasienica (gmina wiejska, 2402052)
    # Source: iGeoMap vector MPZP service
    # WFS exposes zoning polygons as lay63 ("Przeznaczenia terenu")
    # WFS: MapServer 1.1.0, GML only
    # ------------------------------------------------------------------
    "2402052": WFSRegistryEntry(
        label="Jasienica",
        wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/240205",
        layer_name="lay63",
        source_srid=2180,
        wfs_version="1.1.0",
        prefer_json=False,
        field_mapping=WFSFieldMapping(
            przeznaczenie="etykieta",
            przeznaczenie_opis="opis",
            plan_name="plan",
            uchwala_nr="",
            plan_effective_date="",
            teryt_gmina="",
        ),
    ),

    # ------------------------------------------------------------------
    # Śląskie — Bestwina (gmina wiejska, 2402022)
    # Source: iGeoMap vector MPZP service
    # WFS exposes zoning polygons as lay63 ("Przeznaczenia terenu")
    # WFS: MapServer 1.1.0, GML only
    # ------------------------------------------------------------------
    "2402022": WFSRegistryEntry(
        label="Bestwina",
        wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/240202",
        layer_name="lay63",
        source_srid=2180,
        wfs_version="1.1.0",
        prefer_json=False,
        field_mapping=WFSFieldMapping(
            przeznaczenie="etykieta",
            przeznaczenie_opis="opis",
            plan_name="plan",
            uchwala_nr="",
            plan_effective_date="",
            teryt_gmina="",
        ),
    ),

    # ------------------------------------------------------------------
    # Śląskie — Imielin (powiat bieruńsko-lędziński, 2414021)
    # Source: iGeoMap vector MPZP service
    # WFS exposes zoning polygons as lay63 ("Przeznaczenia terenu")
    # WFS: MapServer 1.1.0, GML only
    # ------------------------------------------------------------------
    "2414021": [
        WFSRegistryEntry(
            label="Imielin",
            wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/241402",
            layer_name="lay63",
            source_srid=2180,
            wfs_version="1.1.0",
            prefer_json=False,
            field_mapping=WFSFieldMapping(
                przeznaczenie="etykieta",
                przeznaczenie_opis="opis",
                plan_name="plan",
                uchwala_nr="",
                plan_effective_date="",
                teryt_gmina="",
            ),
        ),
        WFSRegistryEntry(
            label="Imielin Studium",
            wfs_url="https://vmpzp.igeomap.pl/cgi-bin/plany/241402/studium",
            layer_name="lay63",
            source_srid=2180,
            plan_type="studium",
            wfs_version="1.1.0",
            prefer_json=False,
            field_mapping=WFSFieldMapping(
                przeznaczenie="etykieta",
                przeznaczenie_opis="opis",
                plan_name="plan",
                uchwala_nr="",
                plan_effective_date="",
                teryt_gmina="",
            ),
        ),
    ],

    # ------------------------------------------------------------------
    # Śląskie — Ruda Śląska (powiat grodzki, 2472011)
    # Source: public WMS only — no public zoning WFS/feature service.
    # We approximate parcel-clipped planning zones by sampling the queryable
    # "MPZP - przeznaczenia terenów" layer with WMS GetFeatureInfo.
    # ------------------------------------------------------------------
    "2472011": WFSRegistryEntry(
        source_kind="wms_grid",
        label="Ruda Śląska",
        wfs_url="https://psip.rudaslaska.pl/gpservices/WMS/rs_plan_wms",
        layer_name="MPZP - przeznaczenia terenów",
        source_srid=2180,
        field_mapping=WFSFieldMapping(),
        plan_type="mpzp",
        wfs_version="1.3.0",
        prefer_json=False,
        cql_filter=None,
        swap_xy=True,
        info_format="text/plain",
        styles="default",
        sample_grid=5,
        point_halfspan_m=8.0,
        parser_name="ruda_plaintext",
    ),

    # ------------------------------------------------------------------
    # Małopolska — Kraków city (powiat grodzki, 1261011)
    # Source: msip.um.krakow.pl  /  MSIP Kraków (ArcGIS Server)
    # Layer: Przeznaczenia_MPZP_-_skala_do_1_1000 (1:1000 zones, most detail)
    # WFS: ArcGIS Server — no JSON, GML 3.1.1 output
    # Fields: Oznaczenie (designation symbol), Nazwa_MPZP (plan name),
    #         Uchwalenie (resolution nr), opis_oznac (description),
    #         Data_obowiazywania (effective date)
    # ------------------------------------------------------------------
    "1261011": WFSRegistryEntry(
        label="Kraków",
        wfs_url=(
            "https://msip.um.krakow.pl/arcgis/services/Obserwatorium/"
            "BP_MPZP/MapServer/WFSServer"
        ),
        layer_name="Przeznaczenia_MPZP_-_skala_do_1_1000",
        source_srid=2178,
        wfs_version="1.1.0",
        prefer_json=False,
        # Kraków ArcGIS returns coords in standard EPSG axis order (Northing, Easting)
        # unlike Katowice which returns (Easting, Northing). Must swap before reprojection.
        swap_xy=True,
        field_mapping=WFSFieldMapping(
            przeznaczenie="Oznaczenie",
            przeznaczenie_opis="opis_oznac",
            plan_name="Nazwa_MPZP",
            uchwala_nr="Uchwalenie",
            plan_effective_date="Data_obowiazywania",
            teryt_gmina="",
        ),
    ),
}


def _registry_entries_for_teryt(teryt: str) -> list[WFSRegistryEntry]:
    entry = WFS_REGISTRY.get(teryt)
    if entry is None:
        return []
    if isinstance(entry, list):
        return entry
    return [entry]


def _registry_source_count() -> int:
    return sum(len(_registry_entries_for_teryt(teryt)) for teryt in WFS_REGISTRY)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

@dataclass
class WFSSyncReport:
    total_gminy: int = 0
    matched_gminy: int = 0
    completed_gminy: int = 0
    failed_gminy: int = 0
    skipped_no_config: int = 0
    total_features_fetched: int = 0
    total_features_upserted: int = 0
    per_gmina: list[dict] = field(default_factory=list)


@dataclass
class UncoveredGmina:
    teryt: str
    dzialki_count: int
    parcels: str
    localities: str
    sample_url: str | None
    in_registry: bool
    coverage_category: str
    next_action: str


@dataclass
class APPProbeReport:
    source_url: str
    content_kind: str
    xml_entry_name: str | None
    akt_count: int
    document_count: int
    drawing_count: int
    multisurface_count: int
    polygon_count: int
    zoning_feature_count: int
    designation_field_hits: int
    sample_title: str | None

    @property
    def looks_like_zoning_dataset(self) -> bool:
        return self.zoning_feature_count > 0 or self.designation_field_hits > 0


@dataclass
class GisonPlanParcelMatch:
    plan_code: str
    parcel_match_count: int
    parcel_samples: list[str] = field(default_factory=list)
    swapped_bbox_match_count: int = 0
    swapped_bbox_samples: list[str] = field(default_factory=list)

    @property
    def bbox_axes_suspect(self) -> bool:
        return self.parcel_match_count == 0 and self.swapped_bbox_match_count > 0


async def _probe_gison_wms_health(wms_url: str | None) -> str | None:
    if not wms_url:
        return None
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0), follow_redirects=True) as client:
            response = await client.get(
                wms_url,
                params={"SERVICE": "WMS", "REQUEST": "GetCapabilities", "VERSION": "1.1.1"},
            )
        if response.status_code != 200:
            return "dead"
        text_body = response.text
        if "WMS_Capabilities" in text_body or "WMT_MS_Capabilities" in text_body:
            return "ok"
    except Exception:
        return "dead"
    return "dead"


async def _probe_gison_geotiff_health(geotiff_url: str | None) -> str | None:
    if not geotiff_url:
        return None
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(20.0),
            follow_redirects=True,
            headers={"Range": "bytes=0-15"},
        ) as client:
            response = await client.get(geotiff_url)
        if response.status_code not in (200, 206):
            return "dead"
        payload = response.content[:4]
        if payload in (b"II*\x00", b"MM\x00*"):
            return "ok"
    except Exception:
        return "dead"
    return "dead"


def _classify_gison_plan_state(
    *,
    parcel_match: GisonPlanParcelMatch,
    wms_url: str | None,
    legend_url: str | None,
    geotiff_url: str | None,
    app_url: str | None,
) -> str:
    if parcel_match.bbox_axes_suspect:
        return "bbox_axis_suspect"
    if parcel_match.parcel_match_count > 0:
        if legend_url and has_manual_gison_legend_override(legend_url):
            return "ready"
        if legend_url:
            return "manual_override_required"
        if wms_url or geotiff_url or app_url:
            return "source_discovered"
    if wms_url or geotiff_url or legend_url or app_url:
        return "source_discovered"
    return "source_discovered"


def _classify_gison_probe_state(
    *,
    designation: str | None,
    legend_entries: int,
) -> str:
    if designation:
        return "ready"
    if legend_entries <= 0:
        return "legend_missing_semantics"
    return "manual_override_required"


def _classify_uncovered_gmina(
    *,
    teryt: str,
    in_registry: bool,
) -> tuple[str, str]:
    if teryt in _UNCOVERED_COVERAGE_OVERRIDES:
        return _UNCOVERED_COVERAGE_OVERRIDES[teryt]

    if in_registry:
        entries = _registry_entries_for_teryt(teryt)
        if any(entry.source_kind == "gison_raster" for entry in entries):
            return (
                "gison_raster_candidate",
                "Probe the configured raster source, validate legend semantics, and rerun sync for this TERYT.",
            )
        return (
            "manual_backlog",
            "A source is configured but coverage is still missing; manually probe the source and inspect sync failures.",
        )

    return (
        "no_source_available",
        "No confirmed planning source is configured yet; continue municipal WFS/WMS/SIP discovery for this TERYT.",
    )


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

async def _fetch_teryt_gminy(
    limit: int | None = None,
    teryt_filter: str | None = None,
    province: str | None = None,
) -> list[str]:
    province = normalize_province(province)
    async with AsyncSessionLocal() as db:
        stmt = (
            select(distinct(Dzialka.teryt_gmina))
            .where(Dzialka.teryt_gmina.is_not(None))
            .order_by(Dzialka.teryt_gmina.asc())
        )
        result = await db.execute(stmt)
        codes = [
            v for v in result.scalars().all()
            if isinstance(v, str) and _RE_TERYT.fullmatch(v)
        ]

    prefix = province_teryt_prefix(province) if province else ""
    if prefix:
        codes = [c for c in codes if c.startswith(prefix)]
    if teryt_filter:
        codes = [c for c in codes if c == teryt_filter]
        # If not in silver.dzialki but in registry, allow explicit test run
        if not codes and _RE_TERYT.fullmatch(teryt_filter) and teryt_filter in WFS_REGISTRY:
            codes = [teryt_filter]
    if limit:
        codes = codes[:limit]
    return codes


async def _fetch_uncovered_gminy(
    limit: int = 20,
    province: str | None = None,
) -> list[UncoveredGmina]:
    """List gminy that have resolved parcels but no planning zone coverage yet."""
    province = normalize_province(province)
    prefix = province_teryt_prefix(province) if province else ""
    query = text(
        """
        WITH covered AS (
            SELECT DISTINCT teryt_gmina
            FROM gold.planning_zones
            WHERE teryt_gmina IS NOT NULL
        )
        SELECT
            d.teryt_gmina AS teryt,
            COUNT(*)::int AS dzialki_count,
            array_to_string(
                array_agg(DISTINCT d.numer_dzialki ORDER BY d.numer_dzialki),
                ', '
            ) AS parcels,
            array_to_string(
                array_remove(array_agg(DISTINCT NULLIF(rl.raw_obreb, '') ORDER BY NULLIF(rl.raw_obreb, '')), NULL),
                ' | '
            ) AS localities,
            MIN(rl.source_url) AS sample_url,
            MIN(c.teryt_gmina) AS covered_via
        FROM silver.dzialki d
        LEFT JOIN silver.listing_parcels lp ON lp.dzialka_id = d.id
        LEFT JOIN bronze.raw_listings rl ON rl.id = lp.listing_id
        LEFT JOIN covered c ON c.teryt_gmina = CASE d.teryt_gmina
            WHEN '1261029' THEN '1261011'
            WHEN '1261039' THEN '1261011'
            WHEN '1261049' THEN '1261011'
            WHEN '1261059' THEN '1261011'
            ELSE d.teryt_gmina
        END
        WHERE d.teryt_gmina IS NOT NULL
          AND (:prefix = '' OR substr(d.teryt_gmina, 1, 2) = :prefix)
          AND c.teryt_gmina IS NULL
        GROUP BY d.teryt_gmina
        ORDER BY COUNT(*) DESC, d.teryt_gmina ASC
        LIMIT :limit
        """
    )

    async with AsyncSessionLocal() as db:
        rows = (await db.execute(query, {"limit": limit, "prefix": prefix})).mappings().all()

    return [
        UncoveredGmina(
            teryt=row["teryt"],
            dzialki_count=row["dzialki_count"],
            parcels=row["parcels"] or "",
            localities=row["localities"] or "",
            sample_url=row["sample_url"],
            in_registry=row["teryt"] in WFS_REGISTRY,
            coverage_category=_classify_uncovered_gmina(
                teryt=row["teryt"],
                in_registry=row["teryt"] in WFS_REGISTRY,
            )[0],
            next_action=_classify_uncovered_gmina(
                teryt=row["teryt"],
                in_registry=row["teryt"] in WFS_REGISTRY,
            )[1],
        )
        for row in rows
        if isinstance(row["teryt"], str) and _RE_TERYT.fullmatch(row["teryt"])
        if coverage_alias_teryt(row["teryt"]) == row["teryt"]
    ]


async def _probe_app_url(source_url: str) -> APPProbeReport:
    """Inspect a public APP ZIP/XML and tell whether it contains zoning features."""
    timeout = httpx.Timeout(60.0)
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            response = await client.get(source_url, headers=_APP_HTTP_HEADERS)
            response.raise_for_status()
            payload = response.content
    except httpx.ConnectError as exc:
        logger.warning(
            "APP probe SSL/connect error for %s; retrying with TLS verification disabled: %s",
            source_url,
            exc,
        )
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            verify=False,
        ) as client:
            response = await client.get(source_url, headers=_APP_HTTP_HEADERS)
            response.raise_for_status()
            payload = response.content

    content_type = (response.headers.get("content-type") or "").lower()
    content_kind = "xml"
    xml_entry_name: str | None = None

    if zipfile.is_zipfile(BytesIO(payload)):
        content_kind = "zip"
        with zipfile.ZipFile(BytesIO(payload)) as zf:
            candidates = [
                name for name in zf.namelist()
                if _RE_APP_XML_ENTRY.search(name)
            ]
            if not candidates:
                raise ValueError("ZIP has no XML/GML entries to inspect")
            xml_entry_name = sorted(candidates)[0]
            payload = zf.read(xml_entry_name)
    elif "zip" in content_type:
        content_kind = "zip-unknown"

    xml_text = payload.decode("utf-8", errors="ignore")

    def _count(pattern: str) -> int:
        return len(re.findall(pattern, xml_text, flags=re.IGNORECASE))

    title_match = re.search(r"<app:tytul>(.*?)</app:tytul>", xml_text, flags=re.IGNORECASE | re.DOTALL)
    sample_title = re.sub(r"\s+", " ", title_match.group(1)).strip() if title_match else None

    return APPProbeReport(
        source_url=source_url,
        content_kind=content_kind,
        xml_entry_name=xml_entry_name,
        akt_count=_count(r"<[^>]*AktPlanowaniaPrzestrzennego"),
        document_count=_count(r"<[^>]*DokumentFormalny"),
        drawing_count=_count(r"<[^>]*RysunekAktuPlanowaniaPrzestrzennego"),
        multisurface_count=_count(r"<[^>]*MultiSurface"),
        polygon_count=_count(r"<[^>]*Polygon"),
        zoning_feature_count=_count(r"<[^>]*StrefaPlanistyczna"),
        designation_field_hits=(
            _count(r"przeznaczenie")
            + _count(r"oznaczenie")
            + _count(r"symbol")
            + _count(r"opis_przezn")
        ),
        sample_title=sample_title,
    )


async def _match_gison_plan_parcels(
    *,
    teryt_gmina: str,
    plan_code: str,
    bbox_2180: tuple[float, float, float, float] | None,
    sample_limit: int = 6,
) -> GisonPlanParcelMatch:
    if not bbox_2180:
        return GisonPlanParcelMatch(plan_code=plan_code, parcel_match_count=0)

    def _swap_bbox_axes(bbox: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
        minx, miny, maxx, maxy = bbox
        return (miny, minx, maxy, maxx)

    async def _fetch_identifiers(bbox: tuple[float, float, float, float]) -> list[str]:
        minx, miny, maxx, maxy = bbox
        query = text(
            """
            SELECT d.identyfikator
            FROM silver.dzialki d
            WHERE d.teryt_gmina = :teryt
              AND ST_Intersects(
                  ST_PointOnSurface(d.geom),
                  ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, 2180)
              )
            ORDER BY d.identyfikator ASC
            """
        )

        async with AsyncSessionLocal() as db:
            return [
                row[0]
                for row in (await db.execute(
                    query,
                    {
                        "teryt": teryt_gmina,
                        "minx": minx,
                        "miny": miny,
                        "maxx": maxx,
                        "maxy": maxy,
                    },
                )).all()
                if isinstance(row[0], str)
            ]

    direct_identifiers, swapped_identifiers = await asyncio.gather(
        _fetch_identifiers(bbox_2180),
        _fetch_identifiers(_swap_bbox_axes(bbox_2180)),
    )

    return GisonPlanParcelMatch(
        plan_code=plan_code,
        parcel_match_count=len(direct_identifiers),
        parcel_samples=direct_identifiers[:sample_limit],
        swapped_bbox_match_count=len(swapped_identifiers),
        swapped_bbox_samples=swapped_identifiers[:sample_limit],
    )


async def _annotate_gison_plans_with_parcels(
    *,
    plans: list,
    teryt_gmina: str,
    sample_limit: int = 6,
) -> dict[str, GisonPlanParcelMatch]:
    matches = await asyncio.gather(
        *[
            _match_gison_plan_parcels(
                teryt_gmina=teryt_gmina,
                plan_code=plan.plan_code,
                bbox_2180=plan.bbox_2180,
                sample_limit=sample_limit,
            )
            for plan in plans
        ]
    )
    return {match.plan_code: match for match in matches}


async def _annotate_gison_plan_assets(
    *,
    plans: list,
    parcel_matches: dict[str, GisonPlanParcelMatch],
) -> dict[str, dict[str, str | None]]:
    candidate_plans = [
        plan for plan in plans
        if (
            parcel_matches.get(plan.plan_code, GisonPlanParcelMatch(plan.plan_code, 0)).parcel_match_count > 0
            or parcel_matches.get(plan.plan_code, GisonPlanParcelMatch(plan.plan_code, 0)).swapped_bbox_match_count > 0
        )
    ]
    if not candidate_plans:
        return {}

    health_rows = await asyncio.gather(
        *[
            asyncio.gather(
                _probe_gison_wms_health(plan.wms_url),
                _probe_gison_geotiff_health(plan.geotiff_url),
            )
            for plan in candidate_plans
        ]
    )
    result: dict[str, dict[str, str | None]] = {}
    for plan, (wms_health, geotiff_health) in zip(candidate_plans, health_rows, strict=False):
        result[plan.plan_code] = {
            "wms_health": wms_health,
            "geotiff_health": geotiff_health,
        }
    return result


async def run_wfs_sync(
    limit: int | None = None,
    teryt_filter: str | None = None,
    override_url: str | None = None,
    override_layer: str | None = None,
    override_srid: int = 2180,
    province: str | None = None,
) -> WFSSyncReport:
    province = normalize_province(province)
    gminy = await _fetch_teryt_gminy(
        limit=limit,
        teryt_filter=teryt_filter,
        province=province,
    )
    report = WFSSyncReport(total_gminy=len(gminy))

    logger.info(
        "[WFSSync] %d distinct gmina code(s) in silver.dzialki; registry has %d gmina keys / %d source entries",
        len(gminy), len(WFS_REGISTRY), _registry_source_count(),
    )

    for index, teryt in enumerate(gminy, start=1):
        # Resolve entry: explicit override > registry lookup
        if override_url and override_layer:
            entries = [WFSRegistryEntry(
                label=f"override:{teryt}",
                wfs_url=override_url,
                layer_name=override_layer,
                source_srid=override_srid,
                field_mapping=WFSFieldMapping(),
            )]
        elif teryt in WFS_REGISTRY:
            entries = _registry_entries_for_teryt(teryt)
        else:
            logger.info(
                "[WFSSync] %d/%d teryt=%s — no WFS configured, skipping "
                "(add an entry to WFS_REGISTRY to enable)",
                index, len(gminy), teryt,
            )
            report.skipped_no_config += 1
            continue

        report.matched_gminy += 1
        for entry in entries:
            logger.info(
                "[WFSSync] %d/%d teryt=%s (%s, %s) → %s",
                index, len(gminy), teryt, entry.label, entry.plan_type, entry.wfs_url,
            )

            try:
                if entry.source_kind == "wms_grid":
                    ingest = await run_wms_grid_ingest(
                        wms_url=entry.wfs_url,
                        layer_name=entry.layer_name,
                        teryt_gmina=teryt,
                        plan_type=entry.plan_type,
                        source_srid=entry.source_srid,
                        version=entry.wfs_version,
                        info_format=entry.info_format,
                        styles=entry.styles,
                        sample_grid=entry.sample_grid,
                        point_halfspan_m=entry.point_halfspan_m,
                        swap_bbox_axes=entry.swap_xy,
                        parser_name=entry.parser_name,
                        query_url_template=entry.query_url_template,
                        fallback_designation=entry.fallback_designation,
                        fallback_description=entry.fallback_description,
                    )
                elif entry.source_kind == "gison_raster":
                    if not entry.legend_url:
                        raise ValueError("gison_raster source requires legend_url")
                    ingest = await run_gison_raster_ingest(
                        wms_url=entry.wfs_url,
                        layer_name=entry.layer_name,
                        legend_url=entry.legend_url,
                        teryt_gmina=teryt,
                        plan_type=entry.plan_type,
                        source_srid=entry.source_srid,
                        version=entry.wfs_version,
                        styles=entry.styles,
                        info_format=entry.info_format,
                        sample_grid=entry.sample_grid,
                        point_halfspan_m=entry.point_halfspan_m,
                        min_confidence=entry.min_confidence,
                        plan_name=entry.plan_name,
                        swap_bbox_axes=entry.swap_xy,
                        geotiff_url=entry.geotiff_url,
                        sample_bbox_2180=entry.sample_bbox_2180,
                    )
                else:
                    ingest = await run_wfs_ingest(
                        wfs_url=entry.wfs_url,
                        layer_name=entry.layer_name,
                        plan_type=entry.plan_type,
                        teryt_gmina=teryt,
                        source_srid=entry.source_srid,
                        cql_filter=entry.cql_filter,
                        field_mapping=entry.field_mapping,
                        wfs_version=entry.wfs_version,
                        prefer_json=entry.prefer_json,
                        swap_xy=entry.swap_xy,
                    )
            except Exception as exc:
                report.failed_gminy += 1
                logger.error(
                    "[WFSSync] teryt=%s (%s, %s) FAILED: %s",
                    teryt, entry.label, entry.plan_type, exc,
                    exc_info=logger.isEnabledFor(logging.DEBUG),
                )
                report.per_gmina.append(
                    {
                        "teryt": teryt,
                        "label": entry.label,
                        "plan_type": entry.plan_type,
                        "status": "failed",
                        "error": str(exc),
                    }
                )
                continue

            report.completed_gminy += 1
            report.total_features_fetched += ingest.features_fetched
            report.total_features_upserted += ingest.features_upserted
            report.per_gmina.append({
                "teryt": teryt,
                "label": entry.label,
                "plan_type": entry.plan_type,
                "status": "ok",
                "fetched": ingest.features_fetched,
                "upserted": ingest.features_upserted,
                "skipped_bounds": ingest.features_skipped_bounds,
                "failed": ingest.features_failed,
                "duration_s": ingest.duration_s,
            })
            logger.info(
                "[WFSSync] teryt=%s (%s, %s) done — fetched=%d upserted=%d failed=%d "
                "skipped_bounds=%d in %.1fs",
                teryt, entry.label, entry.plan_type,
                ingest.features_fetched, ingest.features_upserted, ingest.features_failed,
                ingest.features_skipped_bounds, ingest.duration_s,
            )

    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync MPZP planning zones for gminy in silver.dzialki from WFS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python run_wfs_sync.py               # sync all gminy in WFS_REGISTRY
  uv run python run_wfs_sync.py --teryt 2469011
  uv run python run_wfs_sync.py --wfs-url <URL> --layer-name <LAYER> --teryt <CODE>
  uv run python run_wfs_sync.py --list-registry
""",
    )
    p.add_argument("--limit", type=int, default=None, help="Max gminy to process")
    p.add_argument("--teryt", default=None, help="Sync only this gmina TERYT code")
    p.add_argument("--wfs-url", default=None, help="Override WFS URL (requires --teryt)")
    p.add_argument("--layer-name", default=None, help="Override layer name (requires --wfs-url)")
    p.add_argument("--source-srid", type=int, default=2180, help="Override source SRID")
    p.add_argument(
        "--province",
        choices=["slaskie", "malopolskie"],
        default=None,
        help="Limit sync/listing commands to one province",
    )
    p.add_argument("--list-registry", action="store_true", help="Print registry and exit")
    p.add_argument(
        "--list-uncovered",
        action="store_true",
        help="Print top gminy with silver parcels but no planning_zones coverage yet",
    )
    p.add_argument(
        "--probe-app-url",
        default=None,
        help="Inspect a public APP ZIP/XML and report whether it contains zoning features",
    )
    p.add_argument("--probe-gison-wms", default=None, help="Probe one raster-backed GISON/iGeoMap WMS URL")
    p.add_argument("--probe-gison-index", default=None, help="Parse one e-mapa/wykazplanow page into plan-specific raster sources")
    p.add_argument("--probe-gison-layer", default=None, help="Layer name for --probe-gison-wms")
    p.add_argument("--probe-gison-legend-url", default=None, help="Legend URL for --probe-gison-wms")
    p.add_argument(
        "--probe-gison-bbox",
        default=None,
        help="BBox for the probe as xmin,ymin,xmax,ymax in the source CRS",
    )
    p.add_argument(
        "--probe-gison-srid",
        type=int,
        default=2180,
        help="Source SRID for --probe-gison-wms (default: 2180)",
    )
    p.add_argument(
        "--probe-gison-swap-bbox-axes",
        action="store_true",
        help="Swap X/Y order in the probe bbox before GetMap/GetFeatureInfo (useful for some e-mapa plan links)",
    )
    p.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    return p.parse_args()


def _registry_items(province: str | None = None) -> list[tuple[str, WFSRegistryEntry]]:
    province = normalize_province(province)
    prefix = province_teryt_prefix(province) if province else None
    items: list[tuple[str, WFSRegistryEntry]] = []
    for teryt in sorted(WFS_REGISTRY):
        if prefix and not teryt.startswith(prefix):
            continue
        for entry in _registry_entries_for_teryt(teryt):
            items.append((teryt, entry))
    return items


def _print_registry(province: str | None = None) -> None:
    items = _registry_items(province)
    suffix = f" — {province_display_name(province)}" if province else ""
    print(f"\n{'='*70}")
    print(f"  WFS Registry — {len(items)} entries{suffix}")
    print(f"{'='*70}")
    for teryt, e in items:
        print(
            f"  {teryt}  {e.label:<20}  kind={e.source_kind:<8} "
            f"srid={e.source_srid}  ver={e.wfs_version}"
        )
        print(f"         layer={e.layer_name}")
    print(f"{'='*70}\n")


def _print_uncovered(gminy: list[UncoveredGmina], province: str | None = None) -> None:
    suffix = f" — {province_display_name(province)}" if province else ""
    print(f"\n{'='*90}")
    print(f"  Uncovered Gminy — {len(gminy)} shown{suffix}")
    print(f"{'='*90}")
    for row in gminy:
        registry = "yes" if row.in_registry else "no"
        print(
            f"  {row.teryt}  parcels={row.dzialki_count:<2}  "
            f"registry={registry:<3}  nums={row.parcels}"
        )
        print(f"         category={row.coverage_category}")
        if row.localities:
            print(f"         localities={row.localities}")
        if row.sample_url:
            print(f"         sample={row.sample_url}")
        print(f"         next_action={row.next_action}")
    print(f"{'='*90}\n")


def _print_app_probe(report: APPProbeReport) -> None:
    print(f"\n{'='*70}")
    print("  APP Probe")
    print(f"{'='*70}")
    print(f"  Source URL          : {report.source_url}")
    print(f"  Content kind        : {report.content_kind}")
    print(f"  XML entry           : {report.xml_entry_name or '-'}")
    print(f"  Akt count           : {report.akt_count}")
    print(f"  Dokument count      : {report.document_count}")
    print(f"  Rysunek count       : {report.drawing_count}")
    print(f"  MultiSurface count  : {report.multisurface_count}")
    print(f"  Polygon count       : {report.polygon_count}")
    print(f"  Zoning features     : {report.zoning_feature_count}")
    print(f"  Designation hits    : {report.designation_field_hits}")
    if report.sample_title:
        print(f"  Sample title        : {report.sample_title[:120]}")
    verdict = "yes" if report.looks_like_zoning_dataset else "no"
    print(f"  Looks like zoning   : {verdict}")
    print(f"{'='*70}\n")


def _print_gison_probe(report: dict) -> None:
    print(f"\n{'='*70}")
    print("  GISON Raster Probe")
    print(f"{'='*70}")
    print(f"  WMS URL            : {report['wms_url']}")
    print(f"  Layer              : {report['layer_name']}")
    print(f"  Legend URL         : {report['legend_url']}")
    print(f"  Source state       : {report.get('source_state') or '-'}")
    if report.get("manual_legend_override"):
        print("  Override           : manual legend override available")
    if report.get("error"):
        print(f"  Error              : {report['error']}")
    print(f"  Sampled color      : {report['sampled_color_hex']}")
    print(f"  Legend entries     : {report['legend_entries']}")
    print(f"  Designation        : {report.get('designation') or '-'}")
    print(f"  Label              : {report.get('label') or '-'}")
    print(f"  Confidence         : {report.get('confidence') if report.get('confidence') is not None else '-'}")
    print(f"  Matched by         : {report.get('matched_by') or '-'}")
    print(f"  Plan hint          : {report.get('plan_name_hint') or '-'}")
    excerpt = report.get("raw_feature_info_excerpt") or "-"
    print(f"  FeatureInfo        : {excerpt[:180]}")
    print(f"{'='*70}\n")


def _print_summary(report: WFSSyncReport, province: str | None = None) -> None:
    suffix = f" — {province_display_name(province)}" if province else ""
    print(f"\n{'='*60}")
    print(f"WFS SYNC COMPLETE{suffix}")
    print(f"{'='*60}")
    print(f"  Gminy in DB        : {report.total_gminy}")
    print(f"  Matched in registry: {report.matched_gminy}")
    print(f"  Skipped (no config): {report.skipped_no_config}")
    print(f"  Completed          : {report.completed_gminy}")
    print(f"  Failed             : {report.failed_gminy}")
    print(f"  Features fetched   : {report.total_features_fetched}")
    print(f"  Features upserted  : {report.total_features_upserted}")
    if report.per_gmina:
        print(f"\n  Per-gmina:")
        for g in report.per_gmina:
            status = g["status"]
            if status == "ok":
                print(
                    f"    {g['teryt']} {g['label']:<20}  "
                    f"fetched={g['fetched']} upserted={g['upserted']} "
                    f"in {g['duration_s']:.1f}s"
                )
            else:
                print(f"    {g['teryt']} {g['label']:<20}  FAILED: {g.get('error','')[:60]}")
    print(f"{'='*60}")


async def main() -> None:
    args = _parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.list_registry:
        if args.json:
            print(json.dumps(
                [
                    {
                        "teryt": teryt,
                        "label": entry.label,
                        "kind": entry.source_kind,
                        "source_srid": entry.source_srid,
                        "wfs_version": entry.wfs_version,
                        "layer_name": entry.layer_name,
                    }
                    for teryt, entry in _registry_items(args.province)
                ],
                ensure_ascii=False,
                indent=2,
            ))
        else:
            _print_registry(args.province)
        return

    if args.list_uncovered:
        uncovered = await _fetch_uncovered_gminy(province=args.province)
        if args.json:
            print(json.dumps([asdict(row) for row in uncovered], ensure_ascii=False, indent=2))
        else:
            _print_uncovered(uncovered, args.province)
        return

    if args.probe_app_url:
        probe = await _probe_app_url(args.probe_app_url)
        if args.json:
            print(json.dumps(asdict(probe), ensure_ascii=False, indent=2))
        else:
            _print_app_probe(probe)
        return

    if args.probe_gison_wms:
        if not args.probe_gison_layer or not args.probe_gison_legend_url or not args.probe_gison_bbox:
            print(
                "ERROR: --probe-gison-wms requires --probe-gison-layer, "
                "--probe-gison-legend-url, and --probe-gison-bbox."
            )
            raise SystemExit(1)
        try:
            bbox = tuple(float(part.strip()) for part in args.probe_gison_bbox.split(","))
        except ValueError:
            print("ERROR: --probe-gison-bbox must be xmin,ymin,xmax,ymax")
            raise SystemExit(1)
        if len(bbox) != 4:
            print("ERROR: --probe-gison-bbox must contain exactly four comma-separated numbers")
            raise SystemExit(1)
        probe = await probe_gison_raster_source(
            wms_url=args.probe_gison_wms,
            layer_name=args.probe_gison_layer,
            legend_url=args.probe_gison_legend_url,
            bbox=bbox,  # type: ignore[arg-type]
            source_srid=args.probe_gison_srid,
            swap_bbox_axes=args.probe_gison_swap_bbox_axes,
        )
        payload = asdict(probe)
        payload["manual_legend_override"] = has_manual_gison_legend_override(args.probe_gison_legend_url)
        payload["source_state"] = _classify_gison_probe_state(
            designation=probe.designation,
            legend_entries=probe.legend_entries,
        )
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            _print_gison_probe(payload)
        return

    if args.probe_gison_index:
        plans = await fetch_gison_wykazplanow(args.probe_gison_index)
        parcel_matches: dict[str, GisonPlanParcelMatch] = {}
        asset_health: dict[str, dict[str, str | None]] = {}
        if args.teryt:
            if not _RE_TERYT.fullmatch(args.teryt):
                print("ERROR: --teryt for --probe-gison-index must be a 7-digit TERYT code")
                raise SystemExit(1)
            parcel_matches = await _annotate_gison_plans_with_parcels(
                plans=plans,
                teryt_gmina=args.teryt,
            )
            asset_health = await _annotate_gison_plan_assets(
                plans=plans,
                parcel_matches=parcel_matches,
            )

        payload = []
        for plan in plans:
            parcel_match = parcel_matches.get(plan.plan_code, GisonPlanParcelMatch(plan.plan_code, 0))
            plan_assets = asset_health.get(plan.plan_code, {})
            payload.append({
                "plan_code": plan.plan_code,
                "name": plan.name,
                "legend_url": plan.legend_url,
                "app_url": plan.app_url,
                "geotiff_url": plan.geotiff_url,
                "geoportal_url": plan.geoportal_url,
                "wms_url": plan.wms_url,
                "bbox_2180": plan.bbox_2180,
                "parcel_match_count": parcel_match.parcel_match_count,
                "parcel_samples": parcel_match.parcel_samples,
                "swapped_bbox_match_count": parcel_match.swapped_bbox_match_count,
                "swapped_bbox_samples": parcel_match.swapped_bbox_samples,
                "bbox_axes_suspect": parcel_match.bbox_axes_suspect,
                "manual_legend_override": has_manual_gison_legend_override(plan.legend_url),
                "wms_health": plan_assets.get("wms_health"),
                "geotiff_health": plan_assets.get("geotiff_health"),
                "source_state": _classify_gison_plan_state(
                    parcel_match=parcel_match,
                    wms_url=plan.wms_url,
                    legend_url=plan.legend_url,
                    geotiff_url=plan.geotiff_url,
                    app_url=plan.app_url,
                ),
            })
        payload.sort(
            key=lambda item: (
                item["parcel_match_count"],
                bool(item["wms_url"]),
                bool(item["legend_url"]),
                item["plan_code"],
            ),
            reverse=True,
        )
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print("\n" + "=" * 70)
            print(f"  GISON Index Probe — {args.probe_gison_index}")
            if args.teryt:
                print(f"  Parcel matching for TERYT: {args.teryt}")
            print("=" * 70)
            print(f"  Plans found: {len(payload)}")
            for item in payload:
                print(f"  - {item['plan_code']}  {item['name']}")
                if args.teryt:
                    print(f"    Parcels: {item['parcel_match_count']}")
                    if item["parcel_samples"]:
                        print(f"    Sample : {', '.join(item['parcel_samples'])}")
                    if item["swapped_bbox_match_count"]:
                        print(f"    Swapped: {item['swapped_bbox_match_count']}")
                        if item["swapped_bbox_samples"]:
                            print(f"    Swap ex: {', '.join(item['swapped_bbox_samples'])}")
                    if item["bbox_axes_suspect"]:
                        print("    Axis?  : parcel matches appear only after swapping bbox axes")
                print(f"    State  : {item['source_state']}")
                if item["manual_legend_override"]:
                    print("    Override: manual legend override available")
                if item.get("wms_health"):
                    print(f"    WMS ok : {item['wms_health']}")
                if item.get("geotiff_health"):
                    print(f"    TIFF ok: {item['geotiff_health']}")
                print(f"    WMS    : {item['wms_url'] or '-'}")
                print(f"    Legend : {item['legend_url'] or '-'}")
                print(f"    GeoTIFF: {item['geotiff_url'] or '-'}")
                print(f"    APP    : {item['app_url'] or '-'}")
                print(f"    BBOX   : {item['bbox_2180'] or '-'}")
            print("=" * 70)
        return

    if args.wfs_url and not args.teryt:
        print("ERROR: --wfs-url requires --teryt to identify the gmina.")
        raise SystemExit(1)

    report = await run_wfs_sync(
        limit=args.limit,
        teryt_filter=args.teryt,
        override_url=args.wfs_url,
        override_layer=args.layer_name,
        override_srid=args.source_srid,
        province=args.province,
    )
    if args.json:
        print(json.dumps(asdict(report), ensure_ascii=False, indent=2))
    else:
        _print_summary(report, args.province)

    # Exit with error code if any gmina failed
    if report.failed_gminy:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
