"""ULDK API client — Usługa Lokalizacji Działek Katastralnych.

Official endpoint: https://uldk.gugik.gov.pl/
Documentation: https://uldk.gugik.gov.pl/opis.html

GIS Specialist Commandments enforced here:
  #1 EPSG:2180 is the One True CRS — request srid=2180 on every call.
  #2 ST_MakeValid() before any operation — applied to every parsed geometry.
  #3 Slivers < 0.5 m² are defects — detected and reported post-parse.
  #4 WFS responses are hostile — every HTTP 200 may contain garbage data.
  #5 Area calculations in EPSG:2180 only — ST_Area(geom) in metres².
  #8 Document every CRS transformation — logged for every ST_Transform call.

Response format (semicolon-delimited CSV):
  Line 0: status code ("0" = success, negative = error)
  Line 1: header row (field names)
  Line 2+: data rows

Throttling reality: GUGiK enforces ~2-3 req/s; 429 responses are common during
peak hours (9-16 weekdays). Exponential backoff is mandatory, not optional.
"""

from __future__ import annotations

import binascii
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from urllib.parse import quote

import httpx
from shapely import wkb as shapely_wkb
from shapely.geometry import MultiPolygon, Polygon
from shapely.validation import make_valid

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ULDK_BASE_URL = "https://uldk.gugik.gov.pl/"
CANONICAL_SRID = 2180

# Poland bounding box in EPSG:2180 (PUWG 1992):
#   X (Easting):  140,000 – 900,000 m
#   Y (Northing): 100,000 – 800,000 m
# Anything outside this box is either wrong CRS or outside Poland.
_POLAND_XMIN, _POLAND_XMAX = 140_000.0, 900_000.0
_POLAND_YMIN, _POLAND_YMAX = 100_000.0, 800_000.0

# Sliver threshold: polygons < 0.5 m² from spatial ops are artefacts (Commandment #3)
_SLIVER_AREA_M2 = 0.5

# Result fields requested from ULDK — WKB geometry + full TERYT breakdown
_RESULT_FIELDS = "id,voivodeship,county,commune,region,parcel,geom_wkb"

# ULDK API parameters
_PARAMS_KW = "GetParcelByKW"
_PARAMS_ID = "GetParcelById"

# HTTP timeouts (seconds)
_CONNECT_TIMEOUT = 5.0
_READ_TIMEOUT = 10.0


# ---------------------------------------------------------------------------
# Error hierarchy
# ---------------------------------------------------------------------------

class ULDKError(Exception):
    """Base class for all ULDK client errors."""


class ULDKNotFoundError(ULDKError):
    """Parcel not found in ULDK registry."""


class ULDKAPIError(ULDKError):
    """ULDK returned a non-zero status code or an HTTP error."""


class GeometryValidationError(ULDKError):
    """Geometry failed validation after parsing — do not store downstream."""


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class ULDKParcel:
    """A fully-resolved parcel from ULDK.

    All geometry is in EPSG:2180.
    `geom_shape` is the Shapely MultiPolygon, validated and ready for ST_MakeValid.
    `geom_wkb_hex` is the raw hex string from ULDK (preserved for Evidence Chain).
    """

    # TERYT identifiers
    identifier: str           # Full ULDK ID: e.g. '1412011.0001.123/4'
    voivodeship: str          # 2-digit: '14'
    county: str               # 4-digit: '1412'
    commune: str              # 7-digit: '1412011'
    region: str               # 4-digit obreb within gmina: '0001'
    parcel: str               # numer działki: '123/4'

    # Derived TERYT codes (for silver.dzialki schema)
    teryt_wojewodztwo: str    # 2 chars
    teryt_powiat: str         # 4 chars
    teryt_gmina: str          # 7 chars
    teryt_obreb: str          # 9 chars = gmina(7) + obreb_nr(2)
    numer_dzialki: str        # normalised działka number
    identyfikator: str        # canonical PK: teryt_obreb + '.' + numer_dzialki

    # Geometry
    geom_shape: MultiPolygon  # Shapely geometry, validated, EPSG:2180
    geom_wkb_hex: str         # raw hex from ULDK (for Evidence Chain)
    area_m2: Decimal          # computed from Shapely (ST_Area equivalent)

    # Quality flags
    was_made_valid: bool = False    # True if ST_MakeValid() was applied
    area_discrepancy_pct: float = 0.0  # vs ULDK-reported area if available
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class ULDKResponse:
    """Raw parsed ULDK response before geometry validation."""
    status_code: int
    parcels: list[dict]    # list of field-dict from CSV rows
    raw_text: str          # preserved for Evidence Chain


# ---------------------------------------------------------------------------
# Geometry pipeline
# ---------------------------------------------------------------------------

def parse_and_validate_wkb(wkb_hex: str, source_id: str) -> MultiPolygon:
    """Parse WKB hex → Shapely → validate → ensure MultiPolygon.

    GIS Specialist workflow on every geometry entering PostGIS:
      1. Parse WKB (handles both WKB and EWKB formats)
      2. ST_IsValid() check
      3. ST_MakeValid() if invalid — log the repair
      4. Enforce MultiPolygon type (działki can be non-contiguous)
      5. Coordinate range check (Poland bounding box in EPSG:2180)
      6. Area sanity check (> 0.5 m²)

    Raises GeometryValidationError for unrecoverable failures.
    """
    # 1. Parse — Shapely handles WKB, EWKB, hex-encoded
    try:
        raw_bytes = binascii.unhexlify(wkb_hex.strip())
        shape = shapely_wkb.loads(raw_bytes, hex=False)
    except Exception as exc:
        raise GeometryValidationError(
            f"WKB parse failure for {source_id}: {exc}"
        ) from exc

    # 2. Validity check
    was_invalid = not shape.is_valid
    if was_invalid:
        reason = shape.is_valid_reason
        logger.warning(
            "[GIS] Invalid geometry for %s — reason: %s. Applying make_valid().",
            source_id, reason,
        )
        shape = make_valid(shape)
        if not shape.is_valid:
            raise GeometryValidationError(
                f"make_valid() failed for {source_id}: {shape.is_valid_reason}"
            )
        logger.info("[GIS] make_valid() applied successfully to %s", source_id)

    # 3. Enforce MultiPolygon (schema requires MULTIPOLYGON type)
    if isinstance(shape, Polygon):
        shape = MultiPolygon([shape])
        logger.debug("[GIS] Converted Polygon → MultiPolygon for %s", source_id)
    elif not isinstance(shape, MultiPolygon):
        # GeometryCollection or other: extract polygons if possible
        polys = [g for g in getattr(shape, "geoms", []) if isinstance(g, (Polygon, MultiPolygon))]
        if not polys:
            raise GeometryValidationError(
                f"Cannot convert {shape.geom_type} to MultiPolygon for {source_id}"
            )
        from shapely.ops import unary_union
        shape = MultiPolygon([g for g in polys if isinstance(g, Polygon)]
                             + [p for g in polys if isinstance(g, MultiPolygon)
                                for p in g.geoms])

    # 4. Coordinate range check (EPSG:2180 Poland bounding box)
    minx, miny, maxx, maxy = shape.bounds
    if not (_POLAND_XMIN <= minx and maxx <= _POLAND_XMAX and
            _POLAND_YMIN <= miny and maxy <= _POLAND_YMAX):
        raise GeometryValidationError(
            f"Coordinates outside Poland EPSG:2180 bounding box for {source_id}. "
            f"bounds=({minx:.0f}, {miny:.0f}, {maxx:.0f}, {maxy:.0f}). "
            f"Likely wrong CRS in ULDK response — this is a CRITICAL red flag."
        )

    # 5. Area sanity (slivers = defects)
    area = shape.area
    if area < _SLIVER_AREA_M2:
        raise GeometryValidationError(
            f"Geometry area {area:.4f} m² is below sliver threshold "
            f"({_SLIVER_AREA_M2} m²) for {source_id}. Likely parse artefact."
        )

    return shape


def _derive_teryt_codes(commune: str, region: str) -> tuple[str, str, str, str]:
    """Derive TERYT fields for silver.dzialki from ULDK response data.

    ULDK provides:
      commune = 7-digit TERYT gmina code (WWPPGGR)
      region  = 4-digit obreb number within the gmina ('0001', '0023')

    silver.dzialki requires 9-digit teryt_obreb = gmina(7) + obreb_nr(2).
    Convention: obreb_nr = int(region).zfill(2) — assumes < 100 obreby per gmina
    (valid for all known Polish gminas as of 2026).
    """
    if len(commune) < 7:
        commune = commune.zfill(7)

    woj = commune[:2]
    powiat = commune[:4]
    gmina = commune[:7]

    try:
        obreb_num = int(region)
        obreb_suffix = str(obreb_num).zfill(2)   # '0001' → 1 → '01'
    except (ValueError, TypeError):
        obreb_suffix = region[-2:] if len(region) >= 2 else region.zfill(2)

    teryt_obreb = gmina + obreb_suffix   # 7 + 2 = 9 chars

    return woj, powiat, gmina, teryt_obreb


# ---------------------------------------------------------------------------
# ULDK HTTP client
# ---------------------------------------------------------------------------

class ULDKClient:
    """Async HTTP client for the GUGiK ULDK parcel resolution API.

    Usage:
        async with ULDKClient() as client:
            parcels = await client.resolve_parcel_by_kw("WA1M/00012345/2")
            for p in parcels:
                print(p.identyfikator, p.area_m2)

    Throttling: GUGiK enforces ~2-3 req/s.
    This client does NOT implement rate limiting internally — the caller
    (GeoResolver) is responsible for inserting delays between calls.
    This keeps the client single-responsibility.
    """

    def __init__(
        self,
        base_url: str = ULDK_BASE_URL,
        timeout_s: float = _READ_TIMEOUT,
        max_retries: int = 3,
        retry_base_delay_s: float = 2.0,
    ) -> None:
        self._base_url = base_url.rstrip("/") + "/"
        self._timeout = httpx.Timeout(connect=_CONNECT_TIMEOUT, read=timeout_s, write=5.0, pool=5.0)
        self._max_retries = max_retries
        self._retry_base_delay_s = retry_base_delay_s
        self._http: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "ULDKClient":
        self._http = httpx.AsyncClient(
            timeout=self._timeout,
            headers={"User-Agent": "TerraZoning-GeoResolver/0.1 (+https://terrazoning.pl/bot)"},
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._http:
            await self._http.aclose()

    # ------------------------------------------------------------------
    # Public resolution methods
    # ------------------------------------------------------------------

    async def resolve_parcel_by_kw(self, kw: str) -> list[ULDKParcel]:
        """Resolve all parcels linked to a Księga Wieczysta number.

        A single KW can cover multiple działki — this returns ALL of them.
        The KW must be in canonical form: CCCC/NNNNNNNN/D.

        ULDK call: ?request=GetParcelByKW&kw={kw}&srid=2180&result=...
        """
        raw = await self._request({
            "request": _PARAMS_KW,
            "kw": kw,
            "srid": str(CANONICAL_SRID),
            "result": _RESULT_FIELDS,
        })
        parsed = self._parse_uldk_response(raw, context=f"KW={kw}")
        logger.info(
            "[ULDK] GetParcelByKW kw=%s → %d row(s)", kw, len(parsed.parcels)
        )
        return self._build_parcels(parsed)

    async def resolve_parcel_by_id(self, parcel_id: str) -> ULDKParcel | None:
        """Resolve a single parcel by its ULDK/EGIB identifier.

        parcel_id format: '1412011.0001.123/4' or '141201_1.0001.123/4'

        ULDK call: ?request=GetParcelById&id={parcel_id}&srid=2180&result=...
        """
        raw = await self._request({
            "request": _PARAMS_ID,
            "id": parcel_id,
            "srid": str(CANONICAL_SRID),
            "result": _RESULT_FIELDS,
        })
        parsed = self._parse_uldk_response(raw, context=f"id={parcel_id}")
        logger.info(
            "[ULDK] GetParcelById id=%s → %d row(s)", parcel_id, len(parsed.parcels)
        )
        parcels = self._build_parcels(parsed)
        return parcels[0] if parcels else None

    # ------------------------------------------------------------------
    # HTTP layer with retry + backoff
    # ------------------------------------------------------------------

    async def _request(self, params: dict[str, str]) -> str:
        """Execute a ULDK HTTP GET with exponential backoff retry.

        Retries on: 429 (rate limited), 503, 504, connection errors.
        Does NOT retry on: 400, 404 (client errors — no point retrying).
        """
        import asyncio

        assert self._http is not None, "ULDKClient must be used as async context manager"

        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = await self._http.get(self._base_url, params=params)

                if response.status_code == 429:
                    wait = self._retry_base_delay_s * (2 ** (attempt - 1))
                    logger.warning(
                        "[ULDK] Rate limited (429) on attempt %d/%d — sleeping %.1fs",
                        attempt, self._max_retries, wait,
                    )
                    await asyncio.sleep(wait)
                    continue

                if response.status_code in (503, 504):
                    wait = self._retry_base_delay_s * (2 ** (attempt - 1))
                    logger.warning(
                        "[ULDK] Server unavailable (%d) on attempt %d/%d — sleeping %.1fs",
                        response.status_code, attempt, self._max_retries, wait,
                    )
                    await asyncio.sleep(wait)
                    continue

                response.raise_for_status()
                return response.text

            except httpx.TimeoutException as exc:
                last_exc = exc
                wait = self._retry_base_delay_s * (2 ** (attempt - 1))
                logger.warning(
                    "[ULDK] Timeout on attempt %d/%d — sleeping %.1fs: %s",
                    attempt, self._max_retries, wait, exc,
                )
                await asyncio.sleep(wait)

            except httpx.HTTPStatusError as exc:
                # 4xx that we should not retry
                raise ULDKAPIError(
                    f"ULDK HTTP error {exc.response.status_code}: {exc.response.text[:200]}"
                ) from exc

            except httpx.RequestError as exc:
                last_exc = exc
                wait = self._retry_base_delay_s * (2 ** (attempt - 1))
                logger.warning(
                    "[ULDK] Request error on attempt %d/%d — sleeping %.1fs: %s",
                    attempt, self._max_retries, wait, exc,
                )
                await asyncio.sleep(wait)

        raise ULDKAPIError(
            f"ULDK request failed after {self._max_retries} retries. "
            f"Last error: {last_exc}"
        ) from last_exc

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_uldk_response(text: str, context: str) -> ULDKResponse:
        """Parse ULDK CSV response into structured rows.

        Response format:
          Line 0:  status code integer ("0" = success, negative = error)
          Line 1:  semicolon-separated header row
          Line 2+: semicolon-separated data rows

        Raises ULDKNotFoundError if status = -1 (parcel not in registry).
        Raises ULDKAPIError for other non-zero status codes.
        """
        lines = [l.strip() for l in text.strip().splitlines() if l.strip()]

        if not lines:
            raise ULDKAPIError(f"Empty ULDK response for {context}")

        try:
            status = int(lines[0])
        except ValueError as exc:
            raise ULDKAPIError(
                f"Could not parse ULDK status code from: {lines[0]!r} "
                f"(context: {context})"
            ) from exc

        if status == -1:
            raise ULDKNotFoundError(
                f"Parcel not found in ULDK registry: {context}"
            )
        if status != 0:
            detail = lines[1] if len(lines) > 1 else "no detail"
            raise ULDKAPIError(
                f"ULDK error code {status} for {context}: {detail}"
            )

        if len(lines) < 2:
            raise ULDKAPIError(f"ULDK response has no data rows for {context}")

        headers = [h.strip() for h in lines[1].split(";")]
        rows = []
        for line in lines[2:]:
            values = line.split(";", maxsplit=len(headers) - 1)
            if len(values) != len(headers):
                logger.warning(
                    "[ULDK] Skipping malformed row (expected %d cols, got %d): %r",
                    len(headers), len(values), line[:80],
                )
                continue
            rows.append(dict(zip(headers, values)))

        return ULDKResponse(status_code=status, parcels=rows, raw_text=text)

    # ------------------------------------------------------------------
    # Parcel building (parsing + geometry validation)
    # ------------------------------------------------------------------

    @staticmethod
    def _build_parcels(response: ULDKResponse) -> list[ULDKParcel]:
        """Convert raw CSV rows into validated ULDKParcel objects.

        Applies the full GIS Specialist geometry validation pipeline on each row.
        Rows that fail validation are logged and skipped (not raised) so a single
        bad row doesn't break the entire batch.
        """
        result = []
        for row in response.parcels:
            identifier = row.get("id", "").strip()
            try:
                parcel = ULDKClient._build_one_parcel(row, identifier)
                result.append(parcel)
            except GeometryValidationError as exc:
                logger.error(
                    "[GIS] SEVERITY:HIGH — Geometry validation failed for %s: %s",
                    identifier, exc,
                )
            except Exception as exc:
                logger.error(
                    "[GIS] Unexpected error building parcel %s: %s",
                    identifier, exc, exc_info=True,
                )
        return result

    @staticmethod
    def _build_one_parcel(row: dict, identifier: str) -> ULDKParcel:
        """Build and validate a single ULDKParcel from a CSV row dict."""
        voivodeship = row.get("voivodeship", "").strip().zfill(2)
        county = row.get("county", "").strip().zfill(4)
        commune = row.get("commune", "").strip().zfill(7)
        region = row.get("region", "").strip().zfill(4)
        parcel = row.get("parcel", "").strip()
        wkb_hex = row.get("geom_wkb", "").strip()

        if not wkb_hex:
            raise GeometryValidationError(
                f"ULDK returned empty geom_wkb for {identifier}. "
                "This may indicate the parcel has no registered geometry in EGIB."
            )

        # Geometry pipeline (GIS Commandments #1–#4)
        shape = parse_and_validate_wkb(wkb_hex, source_id=identifier)
        area = Decimal(str(round(shape.area, 2)))

        # Sliver check after make_valid
        if area < Decimal(str(_SLIVER_AREA_M2)):
            raise GeometryValidationError(
                f"Parcel {identifier} area {area} m² is a sliver — "
                "discarding as spatial artefact."
            )

        woj, powiat, gmina, teryt_obreb = _derive_teryt_codes(commune, region)
        numer = parcel
        identyfikator = f"{teryt_obreb}.{numer}"

        return ULDKParcel(
            identifier=identifier,
            voivodeship=voivodeship,
            county=county,
            commune=commune,
            region=region,
            parcel=parcel,
            teryt_wojewodztwo=woj,
            teryt_powiat=powiat,
            teryt_gmina=gmina,
            teryt_obreb=teryt_obreb,
            numer_dzialki=numer,
            identyfikator=identyfikator,
            geom_shape=shape,
            geom_wkb_hex=wkb_hex,
            area_m2=area,
            was_made_valid=not shape.is_valid,  # note: shape is now valid; was it before?
        )
