"""Public APP GML ingestion for municipalities that expose only plan extents.

This path is intentionally conservative:
  - it ingests public APP / GML / ZIP payloads published by municipalities,
  - extracts the spatial extent of the planning act,
  - stores it as a conservative non-buildable `mpzp` planning zone,
  - and does not invent binding land-use designations.

Use this when a municipality publishes a legally meaningful APP dataset but does
not yet expose parcel-safe MPZP zoning classes through WFS/WMS/legend assets.
"""

from __future__ import annotations

import asyncio
import logging
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO

import httpx

from app.core.database import AsyncSessionLocal
from app.services.wfs_downloader import WFSClient, WFSFeature, WFSFieldMapping, WFSIngestReport

logger = logging.getLogger(__name__)

_APP_TIMEOUT_S = 60.0
_APP_XML_ENTRY = re.compile(r"\.(xml|gml)$", re.IGNORECASE)
_APP_HEADERS = {
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


@dataclass
class AppGMLConfig:
    source_url: str
    teryt_gmina: str
    source_srid: int
    plan_type: str = "mpzp"
    swap_xy: bool = False
    fixed_designation: str = "MPZP_PROJ"
    description_prefix: str = "APP project extent"


def _extract_app_payload(payload: bytes) -> tuple[bytes, str]:
    if zipfile.is_zipfile(BytesIO(payload)):
        with zipfile.ZipFile(BytesIO(payload)) as zf:
            candidates = [name for name in zf.namelist() if _APP_XML_ENTRY.search(name)]
            if not candidates:
                raise ValueError("APP ZIP has no XML/GML entry")
            entry_name = sorted(candidates)[0]
            return zf.read(entry_name), entry_name
    return payload, "payload.xml"


def parse_app_gml_payload(
    payload: bytes,
    *,
    source_url: str,
    teryt_gmina: str,
    source_srid: int,
    fixed_designation: str = "MPZP_PROJ",
    plan_type: str = "mpzp",
    swap_xy: bool = False,
    description_prefix: str = "APP project extent",
) -> list[WFSFeature]:
    xml_payload, _ = _extract_app_payload(payload)
    body = xml_payload.decode("utf-8", errors="ignore")

    client = WFSClient()
    raw_features = client._parse_gml_response(body)
    mapping = WFSFieldMapping(
        przeznaczenie="tytul",
        plan_name="tytul",
        uchwala_nr="identifier",
        przeznaczenie_opis="tytul",
        plan_effective_date="obowiazujeOd",
    )

    features: list[WFSFeature] = []
    for raw in raw_features:
        parsed = client._parse_feature(
            raw,
            plan_type=plan_type,
            teryt_gmina=teryt_gmina,
            source_srid=source_srid,
            mapping=mapping,
            wfs_url=source_url,
            swap_xy=swap_xy,
        )
        if parsed is None:
            continue

        title = (parsed.plan_name or "").strip()
        parsed.przeznaczenie = fixed_designation
        parsed.przeznaczenie_opis = (
            f"{description_prefix}: {title}" if title else description_prefix
        )
        features.append(parsed)

    return features


async def run_app_gml_ingest(
    *,
    source_url: str,
    teryt_gmina: str,
    source_srid: int,
    plan_type: str = "mpzp",
    swap_xy: bool = False,
    fixed_designation: str = "MPZP_PROJ",
    description_prefix: str = "APP project extent",
) -> WFSIngestReport:
    started = asyncio.get_event_loop().time()
    report = WFSIngestReport(
        wfs_url=source_url,
        layer="app_gml",
        teryt_gmina=teryt_gmina,
        started_at=datetime.now(timezone.utc),
    )

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(_APP_TIMEOUT_S),
        follow_redirects=True,
        headers=_APP_HEADERS,
    ) as http:
        response = await http.get(source_url)
        response.raise_for_status()
        payload = response.content

    features = parse_app_gml_payload(
        payload,
        source_url=source_url,
        teryt_gmina=teryt_gmina,
        source_srid=source_srid,
        fixed_designation=fixed_designation,
        plan_type=plan_type,
        swap_xy=swap_xy,
        description_prefix=description_prefix,
    )
    report.features_fetched = len(features)
    valid = [feature for feature in features if feature.is_valid_bounds()]
    report.features_valid = len(valid)
    report.features_skipped_bounds = len(features) - len(valid)

    client = WFSClient()
    async with AsyncSessionLocal() as db:
        report.features_upserted = await client.ingest_planning_zones(db, valid)
    report.features_failed = report.features_valid - report.features_upserted
    report.duration_s = round(asyncio.get_event_loop().time() - started, 2)

    logger.info(
        "[APPGML] teryt=%s fetched=%d upserted=%d failed=%d",
        teryt_gmina,
        report.features_fetched,
        report.features_upserted,
        report.features_failed,
    )
    return report
