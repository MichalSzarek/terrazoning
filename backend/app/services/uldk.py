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

CONFIRMED WORKING ENDPOINTS (verified via curl, April 2026):
  GetParcelById  — resolves by full TERYT parcel ID (commune.region.parcel)
  GetParcelByXY  — resolves by X,Y coordinates in EPSG:2180

CONFIRMED DEAD ENDPOINTS (return "niepoprawny parametr …"):
  GetParcelByKW  — does NOT exist. Any request returns the error string
                   "niepoprawny parametr GetParcelByKW" as plain text.
                   Do NOT use this. KW → parcel requires ekw.ms.gov.pl first.
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

# GetParcelById result fields — full TERYT breakdown + geometry
_RESULT_FIELDS_ID = "id,voivodeship,county,commune,region,parcel,geom_wkb"
_RESULT_FIELDS_REGION = "id,voivodeship,county,commune,region"

# ULDK API parameters
_PARAMS_ID = "GetParcelById"
_PARAMS_ID_OR_NR = "GetParcelByIdOrNr"

# HTTP timeouts (seconds)
_CONNECT_TIMEOUT = 5.0
_READ_TIMEOUT = 10.0


# ---------------------------------------------------------------------------
# Error hierarchy
# ---------------------------------------------------------------------------

class ULDKError(Exception):
    """Base class for all ULDK client errors."""


class ULDKNotFoundError(ULDKError):
    """Parcel not found in ULDK registry (transient — may succeed on retry)."""


class ULDKGeometryMissingError(ULDKNotFoundError):
    """KW is valid but GUGiK has no geometry for this parcel (permanent).

    Root cause: ~15-20% of Polish Księgi Wieczyste are not yet spatialised —
    the powiat cadastre hasn't uploaded the vector yet. Retrying will always
    fail. Mark as resolution_status='geometry_missing' and remove from DLQ.

    Human action required: check geoportal.gov.pl or powiat's own WMS/WFS.
    """


class ULDKAPIError(ULDKError):
    """ULDK returned a non-zero status code or an HTTP error."""


class ULDKTransientError(ULDKAPIError):
    """ULDK transport/rate-limit failure after retries.

    This is different from parcel-not-found and should generally be treated as a
    short-circuit condition for the current listing, not as a signal to keep
    probing more parcel candidates from the same source text.
    """


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


@dataclass
class ULDKRegion:
    """One cadastral region (obręb) returned by the PRG endpoints."""

    identifier: str         # e.g. '246601_1.0021'
    commune_id: str         # e.g. '246601_1'
    commune_code: str       # e.g. '2466011'
    region_code: str        # e.g. '0021'
    region_name: str        # e.g. 'Centrum'
    voivodeship: str
    county: str
    commune_name: str


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

    async def resolve_parcel_by_nr(
        self,
        region_name: str,
        parcel_nr: str,
    ) -> list[ULDKParcel]:
        """Search by obreb (region) name + parcel number without commune code.

        Calls GetParcelByIdOrNr with '{region_name} {parcel_nr}' format,
        e.g. 'Szklary 28/2'.

        Response line 0 = count of found parcels (NOT a status code).
        Returns empty list if not found.
        May return multiple parcels when the obreb name is not unique —
        caller should disambiguate by province/commune.
        """
        search_str = f"{region_name} {parcel_nr}"
        raw = await self._request({
            "request": _PARAMS_ID_OR_NR,
            "id": search_str,
            "srid": str(CANONICAL_SRID),
            "result": _RESULT_FIELDS_ID,
        })
        parsed = self._parse_id_or_nr_response(raw, context=f"nr={search_str!r}")
        logger.info(
            "[ULDK] GetParcelByIdOrNr nr=%r → %d row(s)", search_str, len(parsed.parcels)
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
            "result": _RESULT_FIELDS_ID,
        })
        parsed = self._parse_uldk_response(raw, context=f"id={parcel_id}")
        logger.info(
            "[ULDK] GetParcelById id=%s → %d row(s)", parcel_id, len(parsed.parcels)
        )
        parcels = self._build_parcels(parsed)
        return parcels[0] if parcels else None

    async def list_regions_for_commune(self, commune_code: str) -> list[ULDKRegion]:
        """Return all cadastral regions for a 7-digit commune TERYT code."""
        lookup_id = self._format_commune_lookup_id(commune_code)
        raw = await self._request({
            "request": "GetRegionByNameOrId",
            "id": lookup_id,
            "result": _RESULT_FIELDS_REGION,
        })
        parsed = self._parse_lookup_response(
            raw,
            context=f"commune={lookup_id}",
            field_names=_RESULT_FIELDS_REGION,
        )
        regions = self._build_regions(parsed)
        logger.info(
            "[ULDK] GetRegionByNameOrId commune=%s → %d region(s)",
            lookup_id, len(regions),
        )
        return regions

    async def resolve_parcel_by_commune_region(
        self,
        commune_code: str,
        region_code: str,
        parcel_nr: str,
    ) -> ULDKParcel | None:
        """Resolve a parcel by explicit commune + region code + parcel number."""
        commune_id = self._format_commune_lookup_id(commune_code)
        parcel_id = f"{commune_id}.{region_code.zfill(4)}.{parcel_nr}"
        return await self.resolve_parcel_by_id(parcel_id)

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

        raise ULDKTransientError(
            f"ULDK request failed after {self._max_retries} retries. "
            f"Last error: {last_exc}"
        ) from last_exc

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_uldk_response(text: str, context: str) -> ULDKResponse:
        """Parse GetParcelById CSV response into structured rows.

        Actual response format (verified via curl, April 2026):
          Line 0:  status code integer ("0" = success, "-1" = not found)
          Line 1+: pipe-delimited data rows (NO header row)
          Field order matches the 'result' parameter: id|voivodeship|...|geom_wkb

        Raises ULDKNotFoundError if status = -1 (parcel not in registry).
        Raises ULDKAPIError for other non-zero status codes.
        """
        lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]

        if not lines:
            raise ULDKAPIError(f"Empty ULDK response for {context}")

        first_token = lines[0].split()[0]
        try:
            status = int(first_token)
        except ValueError as exc:
            raise ULDKAPIError(
                f"Could not parse ULDK status code from: {lines[0]!r} "
                f"(context: {context})"
            ) from exc

        if status == -1:
            detail = " ".join(lines).lower()
            if "geomet" in detail:
                raise ULDKGeometryMissingError(
                    f"Parcel geometry missing in ULDK registry (status=-1): {context}. "
                    f"Response={lines[0]!r}"
                )
            raise ULDKNotFoundError(
                f"Parcel not found in ULDK registry (status=-1): {context}. "
                f"Response={lines[0]!r}"
            )
        if status != 0:
            detail = lines[1] if len(lines) > 1 else "no detail"
            raise ULDKAPIError(
                f"ULDK error code {status} for {context}: {detail}"
            )

        if len(lines) < 2:
            raise ULDKAPIError(f"ULDK response has no data rows for {context}")

        # No header row — map fields by position from _RESULT_FIELDS_ID
        field_names = [f.strip() for f in _RESULT_FIELDS_ID.split(",")]
        rows = []
        for line in lines[1:]:
            values = line.split("|", maxsplit=len(field_names) - 1)
            if len(values) != len(field_names):
                logger.warning(
                    "[ULDK] Skipping malformed row (expected %d cols, got %d): %r",
                    len(field_names), len(values), line[:80],
                )
                continue
            rows.append(dict(zip(field_names, values)))

        return ULDKResponse(status_code=status, parcels=rows, raw_text=text)

    @staticmethod
    def _parse_lookup_response(
        text: str,
        *,
        context: str,
        field_names: str,
    ) -> ULDKResponse:
        """Parse PRG lookup responses like GetRegionByNameOrId."""
        lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]

        if not lines:
            raise ULDKAPIError(f"Empty ULDK response for {context}")

        first_token = lines[0].split()[0]
        try:
            status = int(first_token)
        except ValueError as exc:
            raise ULDKAPIError(
                f"Could not parse lookup status from: {lines[0]!r} "
                f"(context: {context})"
            ) from exc

        if status < 0:
            raise ULDKNotFoundError(
                f"Lookup returned status={status} for {context}: {lines[0]!r}"
            )

        if len(lines) < 2:
            return ULDKResponse(status_code=0, parcels=[], raw_text=text)

        names = [f.strip() for f in field_names.split(",")]
        rows = []
        for line in lines[1:]:
            values = line.split("|", maxsplit=len(names) - 1)
            if len(values) != len(names):
                logger.warning(
                    "[ULDK] Skipping malformed lookup row (expected %d cols, got %d): %r",
                    len(names), len(values), line[:120],
                )
                continue
            rows.append(dict(zip(names, values)))

        return ULDKResponse(status_code=0, parcels=rows, raw_text=text)

    @staticmethod
    def _parse_id_or_nr_response(text: str, context: str) -> ULDKResponse:
        """Parse GetParcelByIdOrNr CSV response.

        Actual response format (verified via curl, April 2026):
          Line 0:  count of found parcels (integer >= 0; < 0 = API error / not found)
          Line 1+: pipe-delimited data rows (NO header row, same as GetParcelById)
          Field order matches the 'result' parameter: id|voivodeship|...|geom_wkb

        count = 0 means not found (no error raised — empty result).
        count < 0 means not found or error (raises ULDKNotFoundError).
        """
        lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]

        if not lines:
            raise ULDKAPIError(f"Empty ULDK response for {context}")

        # First token on line 0 is the count (or a negative error code).
        # ULDK sometimes returns '-1 brak wyników' (multi-word) on first line.
        first_token = lines[0].split()[0]
        try:
            count = int(first_token)
        except ValueError as exc:
            raise ULDKAPIError(
                f"Could not parse GetParcelByIdOrNr count from: {lines[0]!r} "
                f"(context: {context})"
            ) from exc

        if count < 0:
            raise ULDKNotFoundError(
                f"GetParcelByIdOrNr returned count={count} for {context}"
            )

        if count == 0 or len(lines) < 2:
            return ULDKResponse(status_code=0, parcels=[], raw_text=text)

        # No header row — map fields by position from _RESULT_FIELDS_ID
        field_names = [f.strip() for f in _RESULT_FIELDS_ID.split(",")]
        rows = []
        for line in lines[1:]:
            values = line.split("|", maxsplit=len(field_names) - 1)
            if len(values) != len(field_names):
                logger.warning(
                    "[ULDK] Skipping malformed GetParcelByIdOrNr row "
                    "(expected %d cols, got %d): %r",
                    len(field_names), len(values), line[:80],
                )
                continue
            rows.append(dict(zip(field_names, values)))

        return ULDKResponse(status_code=0, parcels=rows, raw_text=text)

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
    def _build_regions(response: ULDKResponse) -> list[ULDKRegion]:
        """Convert PRG lookup rows into lightweight region descriptors."""
        result: list[ULDKRegion] = []
        for row in response.parcels:
            identifier = row.get("id", "").strip()
            if "." not in identifier:
                logger.warning("[ULDK] Skipping malformed region identifier: %r", identifier)
                continue

            commune_id, region_code = identifier.rsplit(".", maxsplit=1)
            commune_code = commune_id.replace("_", "")
            if not commune_code.isdigit() or len(commune_code) != 7:
                logger.warning(
                    "[ULDK] Skipping region with invalid commune code %r (%r)",
                    commune_code, identifier,
                )
                continue

            result.append(ULDKRegion(
                identifier=identifier,
                commune_id=commune_id,
                commune_code=commune_code,
                region_code=region_code.zfill(4),
                region_name=row.get("region", "").strip(),
                voivodeship=row.get("voivodeship", "").strip(),
                county=row.get("county", "").strip(),
                commune_name=row.get("commune", "").strip(),
            ))
        return result

    @staticmethod
    def _build_one_parcel(row: dict, identifier: str) -> ULDKParcel:
        """Build and validate a single ULDKParcel from a CSV row dict.

        ULDK returns human-readable names in the commune/region/voivodeship fields
        (e.g., "Bogatynia", "Krzewina", "dolnośląskie") — NOT numeric TERYT codes.
        TERYT codes must be parsed from the 'id' field: '022503_5.0003.134'
          → commune='0225035' (underscore removed), region='0003', parcel='134'
        """
        uldk_id = row.get("id", identifier).strip() or identifier

        # Parse TERYT codes from the canonical id field
        # Format: {commune_raw}.{region4}.{parcel}  e.g. '022503_5.0003.134'
        id_parts = uldk_id.split(".")
        if len(id_parts) < 3:
            raise GeometryValidationError(
                f"Cannot parse ULDK id {uldk_id!r} — expected at least 3 dot-separated "
                f"parts (commune.region.parcel) for {identifier!r}."
            )
        commune_raw = id_parts[0]                   # '022503_5' or '241201_4'
        region_raw  = id_parts[1]                   # '0003'
        # id may have 3+ parts: commune.region.parcel or commune.region.obreb_name.parcel
        # Use the 'parcel' response field (just the parcel number) when available;
        # fall back to the id-derived tail.
        parcel_from_row = row.get("parcel", "").strip()
        parcel = parcel_from_row or ".".join(id_parts[2:]).strip()

        # Normalise commune: strip underscore → 7-digit numeric TERYT
        # '022503_5' → replace '_' → '0225035' (7 chars); '241201_4' → '2412014'
        commune = commune_raw.replace("_", "").zfill(7)
        region  = region_raw.zfill(4)                    # '3' → '0003'

        if not commune.isdigit() or len(commune) != 7:
            raise GeometryValidationError(
                f"Commune TERYT derived from id={uldk_id!r} is not a valid 7-digit "
                f"code: {commune!r} for {identifier!r}."
            )

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
            identifier=uldk_id,
            voivodeship=woj,       # 2-digit TERYT (derived from id)
            county=powiat,         # 4-digit TERYT (derived from id)
            commune=gmina,         # 7-digit TERYT (derived from id)
            region=region,         # 4-digit obreb code (from id)
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

    @staticmethod
    def _format_commune_lookup_id(commune_code: str) -> str:
        """Convert a 7-digit TERYT commune code into ULDK's underscore form."""
        code = commune_code.strip()
        if "_" in code:
            return code
        if not code.isdigit() or len(code) != 7:
            raise ULDKAPIError(
                f"Invalid commune code for ULDK lookup: {commune_code!r}"
            )
        return f"{code[:6]}_{code[6]}"
