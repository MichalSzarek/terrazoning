"""Raster-backed GISON/iGeoMap MPZP helpers and parcel-centric ingest.

This module started as a set of reusable helpers for:
  - parsing public legend documents,
  - extracting representative swatch colors from legend PNGs,
  - capturing GetMap/GetFeatureInfo request context,
  - classifying sampled raster colors against parsed legend entries.

It now also exposes a parcel-centric ingest path for municipalities that do not
publish vector zoning polygons but do expose usable WMS + legend assets.
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import struct
import urllib.parse
import zlib
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from html import unescape
from io import BytesIO
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import httpx
from geoalchemy2.shape import to_shape
from lxml import html as lxml_html
from PIL import Image
from PIL import UnidentifiedImageError
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

_BROWSER_HEADERS = {
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

_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_PNG_COLOR_TYPE_GRAYSCALE = 0
_PNG_COLOR_TYPE_RGB = 2
_PNG_COLOR_TYPE_INDEXED = 3
_PNG_COLOR_TYPE_RGBA = 6
_PNG_SUPPORTED_COLOR_TYPES = {
    _PNG_COLOR_TYPE_GRAYSCALE,
    _PNG_COLOR_TYPE_RGB,
    _PNG_COLOR_TYPE_INDEXED,
    _PNG_COLOR_TYPE_RGBA,
}

_RE_MULTI_SPACE = re.compile(r"\s+")
_RE_SHEET_HREF = re.compile(r"""href=["'](?P<href>[^"']*sheet\d+\.html?)["']""", re.IGNORECASE)
_RE_DESIGNATION_TOKEN = re.compile(
    r"\b(?P<code>\d{0,3}[A-Z]{1,6}(?:[-/.][A-Z0-9]{1,6})*)\b"
)
_RE_KEY_VALUE = re.compile(
    r"(?P<key>[A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż0-9 _./-]{2,})\s*[:=]\s*(?P<value>[^;\n\r]+)"
)
_DESIGNATION_HINT_KEYS = ("oznacz", "symbol", "przezn", "stref")
_PLAN_HINT_KEYS = ("plan", "uchwal", "uchwał", "numerplanu")
_IGNORE_ROW_PREFIXES = ("LEGENDA", "dla ")
_IGNORED_DESIGNATION_TOKENS = {
    "GETFEATUREINFO",
    "SPATIALPLAN",
    "INSPIREID",
    "EXTENT",
    "OFFICIALTITLE",
    "LEGALSTATUS",
    "BEGINLIFESPANVERSION",
    "ALTERNATIVETITLE",
}
_DEFAULT_SAMPLE_GRID = 5
_DEFAULT_HALFSPAN_M = 6.0
_DEFAULT_PIXEL_RADIUS = 6
_GISON_TIMEOUT_S = 30.0
_IGNORED_SAMPLE_COLORS = {
    "#000000",
    "#FFFFFF",
}
_MANUAL_GISON_LEGEND_OVERRIDES: dict[str, tuple[tuple[str, str, str | None], ...]] = {
    # Human-read override from the public JPG legend. This unlocks a safe pilot
    # for parcels in Zubrzyca Górna / Jabłonka before we build generic OCR or
    # GeoTIFF-driven legend extraction.
    "https://mpzp.igeomap.pl/doc/nowytarg/jablonka/002.jpg": (
        ("#D6A15A", "MNU1", "MNU"),
        ("#E2B9BD", "MNU2", "MNU"),
        ("#B96B18", "MNU3", "MNU"),
        ("#E48C7B", "UMC", "MU"),
        ("#FFA3A7", "UP", "U"),
        ("#FF373C", "UC", "U"),
        ("#B8B0F0", "PU", "U"),
        ("#F8B9AF", "UT1", "U"),
        ("#F4BBB2", "UT2", "U"),
        ("#F2B7AE", "UT3", "U"),
        ("#F6BEB7", "UT4", "U"),
        ("#F3C0BA", "UT5", "U"),
        ("#D9C8B5", "RU", "RU"),
        ("#C8CC00", "GR", "GR"),
        ("#FAF3B7", "R", "R"),
        ("#8EE7EC", "WS", "WS"),
        ("#6ECBC5", "ZL", "ZL"),
        ("#7FB86B", "L", "L"),
        ("#E8A44F", "MN-U", "MN/U"),
    ),
    # Human-read override from the public PDF legend for Jeleśnia plan 009.
    # The live parcel sample for 241704201.2724/11 lands on the pale cream
    # agricultural fill; the dark turquoise color sampled nearby is linework,
    # not the zone fill. We keep a broad swatch catalog so nearest-color
    # matching stays safe without relying on PDF parsing.
    "https://mpzp.igeomap.pl/doc/zywiec/jelesnia/009_legenda.pdf": (
        ("#FCD2A5", "MM", "MM"),
        ("#D9AF8D", "MU", "MU"),
        ("#DAB0B3", "MP", "MP"),
        ("#FCD690", "ML", "ML"),
        ("#FF8F95", "UP", "U"),
        ("#FD8D90", "U", "U"),
        ("#FE999B", "UK", "UK"),
        ("#E2FFB5", "UT", "UT"),
        ("#B79AF3", "PU", "PU"),
        ("#FFFED0", "R", "R"),
        ("#E2FFE2", "ZL", "ZL"),
        ("#CCFF8F", "ZW", "ZW"),
        ("#DFFF80", "ZE", "ZE"),
        ("#98EF94", "ZC", "ZC"),
        ("#A5F5D0", "Z", "Z"),
        ("#C8C8C8", "Ks", "KS"),
        ("#B0B0B0", "Kp", "KP"),
        ("#FF9B9E", "TK,U", "TK/U"),
        ("#D8B193", "TK,MN", "TK/MN"),
        ("#D0D0D0", "W", "W"),
        ("#D0D0D0", "E", "E"),
        ("#D0D0D0", "K", "K"),
        ("#D0D0D0", "O", "O"),
    ),
    # The public notice for parcels 2998/61, 2998/63, 2998/70, 2998/72 points to
    # "Teren usług handlu". The GeoTIFF palette uses pale pink (#EEDDDD) as the
    # dominant fill over those parcels; black and white are linework/background.
    "https://mpzp.igeomap.pl/doc/wadowice/andrychow/06.pdf": (
        ("#EEDDDD", "UU", "U"),
        ("#000000", "linework", None),
        ("#FFFFFF", "background", None),
    ),
    # Human-read override from the public JPG legend for Mykanów plan 082.
    # The uncovered parcel 240411208.10/6 samples a solid purple tone closest
    # to the PEF swatch, while the legend exposes other buildable/useful zones
    # for future parcels in the same plan area.
    "https://mpzp.igeomap.pl/doc/czestochowa/mykanow/082.jpg": (
        ("#C58466", "MN-U", "MN/U"),
        ("#DD6D52", "U", "U"),
        ("#9E7593", "U-P", "U/P"),
        ("#8E799C", "PEF", "PEF"),
        ("#BFBFBF", "KR", "KR"),
        ("#A0C57A", "Z", "Z"),
    ),
    # Human-read partial override from the public JPG legend for Żegocina plan
    # 001. The active parcel in Bełdno samples a stable orange swatch that the
    # legend uses for both MN and MNU, so we keep a conservative normalized
    # designation of MN/U while preserving the mixed label in evidence.
    "https://mpzp.igeomap.pl/doc/bochnia/zegocina/001.jpg": (
        ("#DD6F00", "MN/MNU", "MN/U"),
        ("#B75C00", "MW", "MW"),
        ("#DCA500", "MU", "MU"),
    ),
    # Human-read partial override for Bochnia plan 003. The active parcel in
    # Chełm samples a pale yellow fill that sits in the agricultural family of
    # the rendered PDF legend; we normalize that conservatively to R while
    # keeping the buildable MN swatches available for future parcels in the
    # same plan.
    "https://mpzp.igeomap.pl/doc/bochnia/bochnia/003_legenda.pdf": (
        ("#F0FAB2", "R/RR/RU", "R"),
        ("#D8BF73", "MN1", "MN"),
        ("#E0C8A4", "MN2", "MN"),
    ),
    # Human-read override from the public JPG legend for Zawoja plan Z01.
    # The current uncovered parcels sample:
    # - pale salmon residential fill closest to MN1,
    # - very light mint natural greenery closest to ZR.
    # We keep the rest of the palette conservative but available for future
    # parcels in the same plan area.
    "https://rastry.gison.pl/mpzp-public/zawoja/legendy/Z01_2019_84_X_legenda.jpg": (
        ("#DCB7B1", "MN1", "MN"),
        ("#ECD3BD", "MN2", "MN"),
        ("#FFE3E0", "MU", "MU"),
        ("#FF6137", "U", "U"),
        ("#D65A34", "UP", "U"),
        ("#AB4F40", "UK", "U"),
        ("#E5613A", "UZ", "U"),
        ("#FDF68C", "RU", "RU"),
        ("#00AF07", "ZU", "ZU"),
        ("#E0FBE6", "ZR", "ZR"),
        ("#FAFAD8", "R", "R"),
        ("#AED9BD", "ZL", "ZL"),
        ("#AED0BF", "ZL1", "ZL"),
        ("#3C8C8D", "WS", "WS"),
    ),
    # Conservative manual override for Kamienica Polska plan 003. The active
    # parcel sample lands on a very pale lilac swatch whose nearest legend cell
    # is KD-d (gmina road/dojazdowa). This reduces uncovered backlog safely
    # without manufacturing buildable coverage.
    "https://mpzp.igeomap.pl/doc/czestochowa/kamienicapolska/003.jpg": (
        ("#E5E4EE", "KD-d", "KD-D"),
        ("#D9D7E0", "MN,U", "MN/U"),
        ("#D8D6E0", "R/MN,U", "R/MN/U"),
        ("#D5D3DC", "R", "R"),
    ),
    # Conservative manual override for Pawłowice plan 011. The active parcel in
    # Warszowice samples a muted salmon swatch (#CC746E) that is visibly closer
    # to the `U` legend cell than to the neighboring `US` sports/recreation
    # swatch. We keep adjacent tones available so future parcels in the same
    # plan do not collapse onto a single class.
    "https://mpzp.igeomap.pl/doc/pszczyna/pawlowice/011.jpg": (
        ("#CC746E", "U", "U"),
        ("#C38280", "US", "US"),
        ("#E0B55D", "MN", "MN"),
        ("#ECE7C8", "RP", "RP"),
        ("#A3C969", "ZL", "ZL"),
        ("#93B5E4", "WS", "WS"),
        ("#B9A8E0", "P", "P"),
    ),
    # Human-read override from the public Żarki legend for plan Z03 covering
    # Przybynów. The live parcel sample is a uniform pale beige fill closest to
    # the ML swatch ("tereny zabudowy letniskowej"). We keep adjacent classes
    # available for future parcels in the same plan area.
    "https://rastry.gison.pl/mpzp-public/zarki/legendy/Z03_2014_280_XLI_legenda.png": (
        ("#C9873A", "MN", "MN"),
        ("#EECCA4", "ML", "ML"),
        ("#E89A8B", "MU", "MU"),
        ("#F06B53", "U", "U"),
        ("#F6F08A", "R", "R"),
        ("#92C76A", "ZL", "ZL"),
        ("#7DBA87", "ZR", "ZR"),
        ("#BACD6E", "ZC", "ZC"),
        ("#93B9DF", "WS", "WS"),
    ),
}


def has_manual_gison_legend_override(legend_url: str | None) -> bool:
    if not legend_url:
        return False
    return legend_url in _MANUAL_GISON_LEGEND_OVERRIDES


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    return _RE_MULTI_SPACE.sub(" ", unescape(value).replace("\xa0", " ")).strip()


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _normalize_designation(value: str | None) -> str | None:
    if not value:
        return None
    text = _clean_text(value).upper()
    if not text:
        return None
    text = text.strip(".,;:()[]{}")
    text = re.sub(r"^\d+", "", text)
    if text in _IGNORED_DESIGNATION_TOKENS:
        return None
    return text or None


def _build_url_with_params(url: str, params: Mapping[str, str]) -> str:
    parsed = urllib.parse.urlsplit(url)
    existing = {
        key: values[-1]
        for key, values in urllib.parse.parse_qs(parsed.query, keep_blank_values=True).items()
        if values
    }
    merged = {**existing, **dict(params)}
    query = urllib.parse.urlencode(merged, quote_via=urllib.parse.quote)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment))


def _rgb_from_hex(value: str) -> RGBColor:
    cleaned = value.strip().lstrip("#")
    if len(cleaned) != 6:
        raise ValueError(f"Expected 6-digit RGB hex, got: {value}")
    return RGBColor(
        int(cleaned[0:2], 16),
        int(cleaned[2:4], 16),
        int(cleaned[4:6], 16),
    )


@dataclass(frozen=True)
class RGBColor:
    r: int
    g: int
    b: int

    def __post_init__(self) -> None:
        for channel in (self.r, self.g, self.b):
            if channel < 0 or channel > 255:
                raise ValueError(f"RGB channel out of range: {channel}")

    @classmethod
    def coerce(cls, value: RGBColor | tuple[int, int, int]) -> RGBColor:
        if isinstance(value, cls):
            return value
        return cls(*value)

    def distance(self, other: RGBColor | tuple[int, int, int]) -> float:
        rhs = self.coerce(other)
        return math.sqrt(
            (self.r - rhs.r) ** 2 +
            (self.g - rhs.g) ** 2 +
            (self.b - rhs.b) ** 2
        )

    def as_hex(self) -> str:
        return f"#{self.r:02X}{self.g:02X}{self.b:02X}"


@dataclass(frozen=True)
class WMSGetMapContext:
    wms_url: str
    layers: str
    bbox: tuple[float, float, float, float]
    width: int
    height: int
    crs: str = "EPSG:2180"
    version: str = "1.3.0"
    image_format: str = "image/png"
    styles: str = ""
    transparent: bool = True
    extra_params: Mapping[str, str] = field(default_factory=dict)

    def build_params(self) -> dict[str, str]:
        crs_param = "CRS" if self.version == "1.3.0" else "SRS"
        params = {
            "SERVICE": "WMS",
            "VERSION": self.version,
            "REQUEST": "GetMap",
            "LAYERS": self.layers,
            crs_param: self.crs,
            "BBOX": ",".join(str(value) for value in self.bbox),
            "WIDTH": str(self.width),
            "HEIGHT": str(self.height),
            "STYLES": self.styles,
            "FORMAT": self.image_format,
            "TRANSPARENT": str(self.transparent).upper(),
        }
        params.update(dict(self.extra_params))
        return params


@dataclass(frozen=True)
class WMSGetFeatureInfoContext:
    pixel_x: int | None = None
    pixel_y: int | None = None
    raw_text: str | None = None
    attributes: Mapping[str, str] = field(default_factory=dict)
    info_format: str = "text/plain"
    query_layers: str | None = None
    extra_params: Mapping[str, str] = field(default_factory=dict)

    def build_params(self, map_context: WMSGetMapContext) -> dict[str, str]:
        if self.pixel_x is None or self.pixel_y is None:
            raise ValueError("pixel_x and pixel_y are required to build GetFeatureInfo params")

        params = map_context.build_params()
        params.update(
            {
                "REQUEST": "GetFeatureInfo",
                "QUERY_LAYERS": self.query_layers or map_context.layers,
                "INFO_FORMAT": self.info_format,
                "I": str(self.pixel_x),
                "J": str(self.pixel_y),
            }
        )
        params.update(dict(self.extra_params))
        return params


@dataclass(frozen=True)
class FeatureInfoHints:
    designation_hint: str | None = None
    plan_name_hint: str | None = None


@dataclass(frozen=True)
class GisonLegendEntry:
    label: str
    color: RGBColor
    image_url: str
    sheet_url: str
    designation: str | None = None


@dataclass(frozen=True)
class GisonLegendCatalog:
    legend_url: str
    title: str | None
    entries: tuple[GisonLegendEntry, ...]


@dataclass(frozen=True)
class GisonRasterClassification:
    designation: str | None
    label: str
    color: RGBColor
    sampled_color: RGBColor
    image_url: str
    sheet_url: str
    distance: float
    confidence: float
    matched_by: str


@dataclass(frozen=True)
class GisonRasterProbeReport:
    wms_url: str
    layer_name: str
    legend_url: str
    sampled_color_hex: str
    legend_entries: int
    designation: str | None
    label: str | None
    confidence: float | None
    matched_by: str | None
    plan_name_hint: str | None
    raw_feature_info_excerpt: str | None
    error: str | None = None


@dataclass(frozen=True)
class _PendingLegendEntry:
    label: str
    image_url: str
    sheet_url: str
    designation: str | None


@dataclass(frozen=True)
class GisonPlanSource:
    plan_code: str
    name: str
    legend_url: str | None
    app_url: str | None
    geotiff_url: str | None
    geoportal_url: str | None
    wms_url: str | None
    bbox_2180: tuple[float, float, float, float] | None


@dataclass(frozen=True)
class GisonRasterConfig:
    wms_url: str
    layer_name: str
    legend_url: str
    teryt_gmina: str
    plan_type: str = "mpzp"
    source_srid: int = 2180
    version: str = "1.1.1"
    styles: str = "default"
    info_format: str = "text/plain"
    sample_grid: int = _DEFAULT_SAMPLE_GRID
    point_halfspan_m: float = _DEFAULT_HALFSPAN_M
    min_confidence: float = 0.35
    plan_name: str | None = None
    swap_bbox_axes: bool = False
    geotiff_url: str | None = None
    sample_bbox_2180: tuple[float, float, float, float] | None = None


@dataclass(frozen=True)
class _GisonRasterHit:
    designation: str
    label: str
    plan_name: str
    matched_by: str
    confidence: float
    image_url: str
    sheet_url: str


def _normalized_sampling_bbox(
    bbox: tuple[float, float, float, float],
    *,
    swap_bbox_axes: bool,
) -> tuple[float, float, float, float]:
    if not swap_bbox_axes:
        return bbox
    return (bbox[1], bbox[0], bbox[3], bbox[2])


def _pixel_from_bbox(
    point: Point,
    *,
    bbox: tuple[float, float, float, float],
    width: int,
    height: int,
) -> tuple[int, int]:
    minx, miny, maxx, maxy = bbox
    if maxx <= minx or maxy <= miny:
        raise ValueError(f"Invalid bbox for raster sampling: {bbox}")

    px = (point.x - minx) / (maxx - minx) * (width - 1)
    py = (maxy - point.y) / (maxy - miny) * (height - 1)

    ix = max(0, min(width - 1, round(px)))
    iy = max(0, min(height - 1, round(py)))
    return ix, iy


def _palette_index_to_rgb(image: Image.Image, index: int) -> RGBColor:
    palette = image.getpalette()
    if palette is None:
        raise ValueError("Paletted raster is missing a palette")
    base = index * 3
    rgb = tuple(palette[base:base + 3])
    if len(rgb) != 3:
        raise ValueError(f"Palette index {index} is incomplete")
    return RGBColor(*rgb)


def _sample_paletted_pixel_neighborhood(
    image: Image.Image,
    *,
    center: tuple[int, int],
    radius: int = _DEFAULT_PIXEL_RADIUS,
) -> RGBColor:
    if image.mode != "P":
        sampled = image.convert("RGB").getpixel(center)
        return RGBColor(*sampled)

    cx, cy = center
    counts: Counter[int] = Counter()
    for dx in range(-radius, radius + 1):
        for dy in range(-radius, radius + 1):
            ix = max(0, min(image.width - 1, cx + dx))
            iy = max(0, min(image.height - 1, cy + dy))
            counts[int(image.getpixel((ix, iy)))] += 1

    for palette_index, _count in counts.most_common():
        rgb = _palette_index_to_rgb(image, palette_index)
        if rgb.as_hex() not in _IGNORED_SAMPLE_COLORS:
            return rgb

    palette_index = counts.most_common(1)[0][0]
    return _palette_index_to_rgb(image, palette_index)


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


def extract_feature_info_hints(
    context: WMSGetFeatureInfoContext | None,
) -> FeatureInfoHints:
    if context is None:
        return FeatureInfoHints()

    designation_hint: str | None = None
    plan_name_hint: str | None = None

    for key, value in context.attributes.items():
        normalized_key = _normalize_key(key)
        cleaned_value = _clean_text(value)
        if not cleaned_value:
            continue
        if designation_hint is None and any(token in normalized_key for token in _DESIGNATION_HINT_KEYS):
            designation_hint = _normalize_designation(cleaned_value)
        if plan_name_hint is None and any(token in normalized_key for token in _PLAN_HINT_KEYS):
            plan_name_hint = cleaned_value

    raw_text = _clean_text(context.raw_text)
    if raw_text:
        for match in _RE_KEY_VALUE.finditer(raw_text):
            normalized_key = _normalize_key(match.group("key"))
            cleaned_value = _clean_text(match.group("value"))
            if designation_hint is None and any(token in normalized_key for token in _DESIGNATION_HINT_KEYS):
                designation_hint = _normalize_designation(cleaned_value)
            if plan_name_hint is None and any(token in normalized_key for token in _PLAN_HINT_KEYS):
                plan_name_hint = cleaned_value
        if designation_hint is None:
            token_match = _RE_DESIGNATION_TOKEN.search(raw_text.upper())
            if token_match:
                designation_hint = _normalize_designation(token_match.group("code"))

    return FeatureInfoHints(
        designation_hint=designation_hint,
        plan_name_hint=plan_name_hint,
    )


def extract_gison_sheet_urls(document_html: str, base_url: str) -> list[str]:
    text = document_html or ""
    try:
        doc = lxml_html.fromstring(text)
    except Exception:
        doc = None

    urls: list[str] = []
    if doc is not None:
        urls.extend(
            urljoin(base_url, href)
            for href in doc.xpath("//link[@href]/@href")
            if isinstance(href, str) and "sheet" in href.lower()
        )

    if not urls:
        urls.extend(urljoin(base_url, match.group("href")) for match in _RE_SHEET_HREF.finditer(text))

    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if url not in seen:
            seen.add(url)
            deduped.append(url)
    return deduped


def _extract_legend_title(doc: lxml_html.HtmlElement) -> str | None:
    for row in doc.xpath("//tr")[:6]:
        text = _clean_text(" ".join(row.xpath(".//text()")))
        if not text:
            continue
        if text.upper() == "LEGENDA":
            continue
        if text.startswith("dla "):
            return text[4:].strip()
        return text
    return None


def _label_from_row(row: lxml_html.HtmlElement) -> str:
    texts: list[str] = []
    for cell in row.xpath("./td"):
        text = _clean_text(" ".join(cell.xpath(".//text()")))
        if not text:
            continue
        if any(text.startswith(prefix) for prefix in _IGNORE_ROW_PREFIXES):
            continue
        texts.append(text)
    if not texts:
        return ""
    return max(texts, key=len)


def _designation_from_label(label: str) -> str | None:
    match = _RE_DESIGNATION_TOKEN.search(label.upper())
    if not match:
        return None
    return _normalize_designation(match.group("code"))


def parse_gison_sheet_legend(
    sheet_html: str,
    sheet_url: str,
) -> tuple[str | None, list[_PendingLegendEntry]]:
    doc = lxml_html.fromstring(sheet_html)
    title = _extract_legend_title(doc)
    entries: list[_PendingLegendEntry] = []
    seen: set[tuple[str, str]] = set()

    for row in doc.xpath("//tr"):
        image_sources = [
            urljoin(sheet_url, src)
            for src in row.xpath(".//img[@src]/@src")
            if isinstance(src, str) and src.strip()
        ]
        if not image_sources:
            continue

        label = _label_from_row(row)
        if not label:
            continue

        for image_url in image_sources:
            key = (image_url, label)
            if key in seen:
                continue
            seen.add(key)
            entries.append(
                _PendingLegendEntry(
                    label=label,
                    image_url=image_url,
                    sheet_url=sheet_url,
                    designation=_designation_from_label(label),
                )
            )

    return title, entries


def _extract_teryt6_from_wms_url(wms_url: str) -> str | None:
    match = re.search(r"/plany/(\d{6})/(?:studium|\d+)", wms_url)
    if match:
        return match.group(1)
    match = re.search(r"/cgi-bin/(\d{6})", wms_url)
    if match:
        return match.group(1)
    return None


def _extract_bbox_and_wms_from_geoportal_url(
    geoportal_url: str | None,
) -> tuple[tuple[float, float, float, float] | None, str | None]:
    if not geoportal_url:
        return None, None
    parsed = urlparse(geoportal_url)
    query = parse_qs(parsed.query)

    bbox_raw = query.get("bbox", [None])[0]
    bbox: tuple[float, float, float, float] | None = None
    if bbox_raw:
        parts = [part.strip() for part in bbox_raw.split(",")]
        if len(parts) == 4:
            try:
                bbox = tuple(float(part) for part in parts)  # type: ignore[assignment]
            except ValueError:
                bbox = None

    resources = query.get("resources", [None])[0]
    if resources and "map:wms@" in resources:
        return bbox, unquote(resources.split("map:wms@", 1)[1])
    return bbox, None


def parse_gison_wykazplanow(
    page_html: str,
    page_url: str,
) -> list[GisonPlanSource]:
    doc = lxml_html.fromstring(page_html)
    plans: list[GisonPlanSource] = []

    for row in doc.xpath("//tr[contains(@class, 'mpzp')]"):
        cells = row.xpath("./td")
        if len(cells) < 13:
            continue

        plan_code = _clean_text(" ".join(cells[0].xpath(".//text()")))
        plan_name = _clean_text(" ".join(cells[1].xpath(".//text()")))
        if not plan_code:
            continue

        legend_url = next(iter(cells[7].xpath(".//a/@href")), None)
        app_url = next(iter(cells[8].xpath(".//a/@href")), None)
        geotiff_url = next(iter(cells[10].xpath(".//a/@href")), None)
        geoportal_url = next(iter(cells[12].xpath(".//a/@href")), None)

        legend_url = urljoin(page_url, legend_url) if legend_url else None
        app_url = urljoin(page_url, app_url) if app_url else None
        geotiff_url = urljoin(page_url, geotiff_url) if geotiff_url else None
        geoportal_url = urljoin(page_url, geoportal_url) if geoportal_url else None
        bbox_2180, wms_url = _extract_bbox_and_wms_from_geoportal_url(geoportal_url)

        plans.append(
            GisonPlanSource(
                plan_code=plan_code,
                name=plan_name,
                legend_url=legend_url,
                app_url=app_url,
                geotiff_url=geotiff_url,
                geoportal_url=geoportal_url,
                wms_url=wms_url,
                bbox_2180=bbox_2180,
            )
        )

    return plans


async def fetch_gison_wykazplanow(page_url: str) -> list[GisonPlanSource]:
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0),
        follow_redirects=True,
        headers=_BROWSER_HEADERS,
    ) as http:
        response = await http.get(page_url)
        response.raise_for_status()
        return parse_gison_wykazplanow(response.text, page_url)


def _parse_png_chunks(payload: bytes) -> tuple[tuple[int, int, int, int, int], bytes, bytes | None, bytes | None]:
    if not payload.startswith(_PNG_SIGNATURE):
        raise ValueError("Unsupported legend image format: expected PNG signature")

    offset = len(_PNG_SIGNATURE)
    ihdr: tuple[int, int, int, int, int] | None = None
    plte: bytes | None = None
    trns: bytes | None = None
    idat = bytearray()

    while offset < len(payload):
        if offset + 8 > len(payload):
            raise ValueError("Corrupt PNG stream")
        length = struct.unpack(">I", payload[offset:offset + 4])[0]
        offset += 4
        chunk_type = payload[offset:offset + 4]
        offset += 4
        chunk_data = payload[offset:offset + length]
        offset += length + 4  # skip CRC

        if chunk_type == b"IHDR":
            width, height, bit_depth, color_type, compression, filter_method, interlace = struct.unpack(
                ">IIBBBBB",
                chunk_data,
            )
            if compression != 0 or filter_method != 0 or interlace != 0:
                raise ValueError("Unsupported PNG compression/filter/interlace mode")
            ihdr = (width, height, bit_depth, color_type, interlace)
        elif chunk_type == b"PLTE":
            plte = chunk_data
        elif chunk_type == b"tRNS":
            trns = chunk_data
        elif chunk_type == b"IDAT":
            idat.extend(chunk_data)
        elif chunk_type == b"IEND":
            break

    if ihdr is None:
        raise ValueError("PNG missing IHDR chunk")
    return ihdr, bytes(idat), plte, trns


def _channels_for_color_type(color_type: int) -> int:
    if color_type == _PNG_COLOR_TYPE_GRAYSCALE:
        return 1
    if color_type == _PNG_COLOR_TYPE_RGB:
        return 3
    if color_type == _PNG_COLOR_TYPE_INDEXED:
        return 1
    if color_type == _PNG_COLOR_TYPE_RGBA:
        return 4
    raise ValueError(f"Unsupported PNG color type: {color_type}")


def _paeth_predictor(a: int, b: int, c: int) -> int:
    p = a + b - c
    pa = abs(p - a)
    pb = abs(p - b)
    pc = abs(p - c)
    if pa <= pb and pa <= pc:
        return a
    if pb <= pc:
        return b
    return c


def _unfilter_scanlines(data: bytes, row_bytes: int, bytes_per_pixel: int) -> list[bytes]:
    rows: list[bytes] = []
    offset = 0
    previous = bytearray(row_bytes)

    while offset < len(data):
        filter_type = data[offset]
        offset += 1
        row = bytearray(data[offset:offset + row_bytes])
        offset += row_bytes
        if len(row) != row_bytes:
            raise ValueError("Corrupt PNG scanline payload")

        if filter_type == 1:
            for idx in range(row_bytes):
                left = row[idx - bytes_per_pixel] if idx >= bytes_per_pixel else 0
                row[idx] = (row[idx] + left) & 0xFF
        elif filter_type == 2:
            for idx in range(row_bytes):
                row[idx] = (row[idx] + previous[idx]) & 0xFF
        elif filter_type == 3:
            for idx in range(row_bytes):
                left = row[idx - bytes_per_pixel] if idx >= bytes_per_pixel else 0
                row[idx] = (row[idx] + ((left + previous[idx]) // 2)) & 0xFF
        elif filter_type == 4:
            for idx in range(row_bytes):
                left = row[idx - bytes_per_pixel] if idx >= bytes_per_pixel else 0
                upper_left = previous[idx - bytes_per_pixel] if idx >= bytes_per_pixel else 0
                row[idx] = (row[idx] + _paeth_predictor(left, previous[idx], upper_left)) & 0xFF
        elif filter_type != 0:
            raise ValueError(f"Unsupported PNG filter type: {filter_type}")

        rows.append(bytes(row))
        previous = row

    return rows


def _unpack_packed_samples(row: bytes, bit_depth: int, width: int) -> list[int]:
    if bit_depth == 8:
        return list(row[:width])

    mask = (1 << bit_depth) - 1
    samples: list[int] = []
    for byte in row:
        remaining = 8
        while remaining >= bit_depth and len(samples) < width:
            remaining -= bit_depth
            samples.append((byte >> remaining) & mask)
    return samples


def decode_png_rgba(payload: bytes) -> list[tuple[int, int, int, int]]:
    (width, height, bit_depth, color_type, _), idat, plte, trns = _parse_png_chunks(payload)
    if color_type not in _PNG_SUPPORTED_COLOR_TYPES:
        raise ValueError(f"Unsupported PNG color type: {color_type}")
    if color_type in {_PNG_COLOR_TYPE_RGB, _PNG_COLOR_TYPE_RGBA} and bit_depth != 8:
        raise ValueError(f"Unsupported PNG bit depth: {bit_depth}")
    if color_type in {_PNG_COLOR_TYPE_GRAYSCALE, _PNG_COLOR_TYPE_INDEXED} and bit_depth not in {1, 2, 4, 8}:
        raise ValueError(f"Unsupported PNG bit depth: {bit_depth}")

    channels = _channels_for_color_type(color_type)
    bits_per_row = width * channels * bit_depth
    row_bytes = math.ceil(bits_per_row / 8)
    raw = zlib.decompress(idat)
    bytes_per_pixel = max(1, math.ceil((channels * bit_depth) / 8))
    rows = _unfilter_scanlines(raw, row_bytes=row_bytes, bytes_per_pixel=bytes_per_pixel)

    pixels: list[tuple[int, int, int, int]] = []
    for row in rows[:height]:
        if color_type == _PNG_COLOR_TYPE_GRAYSCALE:
            scale = 255 / ((1 << bit_depth) - 1)
            for gray in _unpack_packed_samples(row, bit_depth, width):
                gray = round(gray * scale)
                pixels.append((gray, gray, gray, 255))
        elif color_type == _PNG_COLOR_TYPE_RGB:
            for idx in range(0, len(row), 3):
                pixels.append((row[idx], row[idx + 1], row[idx + 2], 255))
        elif color_type == _PNG_COLOR_TYPE_RGBA:
            for idx in range(0, len(row), 4):
                pixels.append((row[idx], row[idx + 1], row[idx + 2], row[idx + 3]))
        elif color_type == _PNG_COLOR_TYPE_INDEXED:
            if plte is None:
                raise ValueError("Indexed PNG missing palette")
            palette = [tuple(plte[idx:idx + 3]) for idx in range(0, len(plte), 3)]
            alpha = list(trns) if trns is not None else []
            for palette_idx in _unpack_packed_samples(row, bit_depth, width):
                r, g, b = palette[palette_idx]
                a = alpha[palette_idx] if palette_idx < len(alpha) else 255
                pixels.append((r, g, b, a))

    return pixels


def _decode_raster_rgba(payload: bytes) -> list[tuple[int, int, int, int]]:
    if payload.startswith(_PNG_SIGNATURE):
        return decode_png_rgba(payload)

    try:
        image = Image.open(BytesIO(payload)).convert("RGBA")
    except UnidentifiedImageError as exc:
        raise ValueError("Raster payload is not a decodable image") from exc
    return [
        image.getpixel((x, y))
        for y in range(image.height)
        for x in range(image.width)
    ]


def extract_representative_png_color(payload: bytes) -> RGBColor:
    opaque = [
        (r, g, b)
        for r, g, b, a in _decode_raster_rgba(payload)
        if a >= 200
    ]
    if not opaque:
        raise ValueError("Legend swatch has no opaque pixels")

    histogram = Counter(opaque)
    filtered = Counter(
        color for color in opaque
        if not (
            max(color) - min(color) < 12 and (max(color) > 245 or max(color) < 12)
        )
    )
    source = filtered or histogram
    color, _ = max(
        source.items(),
        key=lambda item: (item[1], max(item[0]) - min(item[0]), sum(item[0])),
    )
    return RGBColor(*color)


def classify_against_legend(
    *,
    sampled_color: RGBColor | tuple[int, int, int],
    legend: GisonLegendCatalog,
    getfeatureinfo_context: WMSGetFeatureInfoContext | None = None,
) -> GisonRasterClassification | None:
    if not legend.entries:
        return None

    sample = RGBColor.coerce(sampled_color)
    hints = extract_feature_info_hints(getfeatureinfo_context)
    entries = list(legend.entries)
    matched_by = "nearest_color"

    if hints.designation_hint:
        exact = [
            entry for entry in entries
            if entry.designation and entry.designation == hints.designation_hint
        ]
        if exact:
            entries = exact
            matched_by = "designation_hint"
        else:
            hinted = [
                entry for entry in entries
                if hints.designation_hint in entry.label.upper()
            ]
            if hinted:
                entries = hinted
                matched_by = "designation_hint+color"

    best = min(entries, key=lambda entry: sample.distance(entry.color))
    distance = sample.distance(best.color)
    confidence = max(0.0, 1.0 - min(distance / math.sqrt(255 ** 2 * 3), 1.0))
    designation = best.designation or hints.designation_hint

    return GisonRasterClassification(
        designation=designation,
        label=best.label,
        color=best.color,
        sampled_color=sample,
        image_url=best.image_url,
        sheet_url=best.sheet_url,
        distance=distance,
        confidence=round(confidence, 4),
        matched_by=matched_by,
    )


class GisonRasterIngestor:
    """Loads GISON legends and classifies sampled raster colors."""

    def __init__(self, http_client: httpx.AsyncClient | None = None) -> None:
        self._http = http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            follow_redirects=True,
            headers=_BROWSER_HEADERS,
        )
        self._owns_client = http_client is None
        self._legend_cache: dict[str, GisonLegendCatalog] = {}

    async def aclose(self) -> None:
        if self._owns_client:
            await self._http.aclose()

    async def fetch_legend_catalog(self, legend_url: str) -> GisonLegendCatalog:
        cached = self._legend_cache.get(legend_url)
        if cached is not None:
            return cached

        manual_entries = _MANUAL_GISON_LEGEND_OVERRIDES.get(legend_url)
        if manual_entries:
            catalog = GisonLegendCatalog(
                legend_url=legend_url,
                title="manual override",
                entries=tuple(
                    GisonLegendEntry(
                        label=label,
                        designation=_normalize_designation(designation) if designation else None,
                        color=_rgb_from_hex(color_hex),
                        image_url=legend_url,
                        sheet_url=legend_url,
                    )
                    for color_hex, label, designation in manual_entries
                ),
            )
            self._legend_cache[legend_url] = catalog
            return catalog

        response = await self._http.get(legend_url)
        response.raise_for_status()
        document_bytes = response.content
        document_text = response.text

        if _looks_like_png(document_bytes):
            image_color = extract_representative_png_color(document_bytes)
            catalog = GisonLegendCatalog(
                legend_url=legend_url,
                title=None,
                entries=(
                    GisonLegendEntry(
                        label=legend_url.rsplit("/", 1)[-1],
                        designation=None,
                        color=image_color,
                        image_url=legend_url,
                        sheet_url=legend_url,
                    ),
                ),
            )
            self._legend_cache[legend_url] = catalog
            return catalog

        sheet_urls = extract_gison_sheet_urls(document_text, legend_url) or [legend_url]
        pending_entries: list[_PendingLegendEntry] = []
        title: str | None = None

        for sheet_url in sheet_urls:
            sheet_response = response if sheet_url == legend_url else await self._http.get(sheet_url)
            if sheet_response is not response:
                sheet_response.raise_for_status()
            sheet_title, sheet_entries = parse_gison_sheet_legend(sheet_response.text, sheet_url)
            title = title or sheet_title
            pending_entries.extend(sheet_entries)

        unique_image_urls = {entry.image_url for entry in pending_entries}
        colors_by_url: dict[str, RGBColor] = {}
        for image_url in unique_image_urls:
            image_response = await self._http.get(image_url)
            image_response.raise_for_status()
            colors_by_url[image_url] = extract_representative_png_color(image_response.content)

        entries = tuple(
            GisonLegendEntry(
                label=entry.label,
                designation=entry.designation,
                color=colors_by_url[entry.image_url],
                image_url=entry.image_url,
                sheet_url=entry.sheet_url,
            )
            for entry in pending_entries
            if entry.image_url in colors_by_url
        )
        catalog = GisonLegendCatalog(legend_url=legend_url, title=title, entries=entries)
        self._legend_cache[legend_url] = catalog
        return catalog

    async def classify_sample(
        self,
        *,
        legend_url: str,
        sampled_color: RGBColor | tuple[int, int, int],
        getmap_context: WMSGetMapContext | None = None,
        getfeatureinfo_context: WMSGetFeatureInfoContext | None = None,
    ) -> GisonRasterClassification | None:
        # getmap_context is accepted now so callers can keep request metadata
        # alongside classification, even though the first-pass classifier only
        # needs the color sample plus feature-info hints.
        _ = getmap_context
        legend = await self.fetch_legend_catalog(legend_url)
        return classify_against_legend(
            sampled_color=sampled_color,
            legend=legend,
            getfeatureinfo_context=getfeatureinfo_context,
        )


class GisonRasterParcelIngestor:
    """Parcel-centric ingest for raster-only MPZP sources."""

    def __init__(self, config: GisonRasterConfig) -> None:
        self.config = config
        self._verify = True
        self._http = self._build_client()
        self._classifier = GisonRasterIngestor(self._http)
        self._geotiff_image: Image.Image | None = None
        self._parcel_to_source = (
            None
            if self.config.source_srid == 2180
            else Transformer.from_crs(2180, self.config.source_srid, always_xy=True)
        )

    def _build_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=httpx.Timeout(_GISON_TIMEOUT_S),
            follow_redirects=True,
            verify=self._verify,
            headers=_BROWSER_HEADERS,
        )

    async def aclose(self) -> None:
        await self._classifier.aclose()

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

        hits_by_designation: dict[tuple[str, str, str, str], list[BaseGeometry]] = {}

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

                hit = await self._classify_point(center)
                if hit is None:
                    continue

                key = (
                    hit.designation,
                    hit.label,
                    hit.plan_name,
                    hit.matched_by,
                )
                hits_by_designation.setdefault(key, []).append(cell)

        features: list[WFSFeature] = []
        for (designation, label, plan_name, matched_by), cells in hits_by_designation.items():
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
                    uchwala_nr=None,
                    przeznaczenie_opis=(
                        f"Raster sampled for parcel {identyfikator}: "
                        f"{label} ({matched_by})"
                    ),
                    plan_effective_date=None,
                    source_wfs_url=(
                        f"{self.config.wms_url}"
                        f"?layer={self.config.layer_name}"
                        f"&mode=gison-raster"
                        f"&parcel={urllib.parse.quote(identyfikator)}"
                    ),
                )
            )
        return features

    async def _classify_point(self, point: Point) -> _GisonRasterHit | None:
        if self.config.geotiff_url and self.config.sample_bbox_2180:
            return await self._classify_point_from_geotiff(point)

        legend = await self._classifier.fetch_legend_catalog(self.config.legend_url)
        source_point = point
        if self._parcel_to_source is not None:
            sx, sy = self._parcel_to_source.transform(point.x, point.y)
            source_point = Point(sx, sy)
        span = self.config.point_halfspan_m
        if self.config.swap_bbox_axes:
            bbox = (
                source_point.y - span,
                source_point.x - span,
                source_point.y + span,
                source_point.x + span,
            )
        else:
            bbox = (
                source_point.x - span,
                source_point.y - span,
                source_point.x + span,
                source_point.y + span,
            )
        map_context = WMSGetMapContext(
            wms_url=self.config.wms_url,
            layers=self.config.layer_name,
            bbox=bbox,
            width=101,
            height=101,
            crs=f"EPSG:{self.config.source_srid}",
            version=self.config.version,
            styles=self.config.styles,
        )
        getmap_response = await self._request(self.config.wms_url, map_context.build_params())
        sampled_color = extract_representative_png_color(getmap_response.content)

        feature_info_context: WMSGetFeatureInfoContext | None = None
        try:
            raw_feature_info = (
                await self._request(
                    self.config.wms_url,
                    WMSGetFeatureInfoContext(
                        pixel_x=50,
                        pixel_y=50,
                        info_format=self.config.info_format,
                        query_layers=self.config.layer_name,
                    ).build_params(map_context),
                )
            ).text
        except Exception:
            raw_feature_info = None

        if raw_feature_info:
            feature_info_context = WMSGetFeatureInfoContext(
                pixel_x=50,
                pixel_y=50,
                raw_text=raw_feature_info,
                info_format=self.config.info_format,
                query_layers=self.config.layer_name,
            )

        classification = classify_against_legend(
            sampled_color=sampled_color,
            legend=legend,
            getfeatureinfo_context=feature_info_context,
        )
        if classification is None or not classification.designation:
            return None
        if len(legend.entries) <= 1 and classification.matched_by == "nearest_color":
            return None
        if (
            classification.matched_by == "nearest_color"
            and classification.confidence < self.config.min_confidence
        ):
            return None

        hints = extract_feature_info_hints(feature_info_context)
        plan_name = (
            hints.plan_name_hint
            or self.config.plan_name
            or legend.title
            or self.config.layer_name
        )
        return _GisonRasterHit(
            designation=classification.designation,
            label=classification.label,
            plan_name=plan_name,
            matched_by=classification.matched_by,
            confidence=classification.confidence,
            image_url=classification.image_url,
            sheet_url=classification.sheet_url,
        )

    async def _classify_point_from_geotiff(self, point: Point) -> _GisonRasterHit | None:
        legend = await self._classifier.fetch_legend_catalog(self.config.legend_url)
        image = await self._load_geotiff()
        bbox = _normalized_sampling_bbox(
            self.config.sample_bbox_2180,
            swap_bbox_axes=self.config.swap_bbox_axes,
        )
        pixel = _pixel_from_bbox(
            point,
            bbox=bbox,
            width=image.width,
            height=image.height,
        )
        sampled_color = _sample_paletted_pixel_neighborhood(image, center=pixel)
        classification = classify_against_legend(
            sampled_color=sampled_color,
            legend=legend,
            getfeatureinfo_context=None,
        )
        if classification is None or not classification.designation:
            return None
        if (
            classification.matched_by == "nearest_color"
            and classification.confidence < self.config.min_confidence
        ):
            return None

        plan_name = self.config.plan_name or legend.title or self.config.layer_name
        return _GisonRasterHit(
            designation=classification.designation,
            label=classification.label,
            plan_name=plan_name,
            matched_by=f"geotiff_{classification.matched_by}",
            confidence=classification.confidence,
            image_url=classification.image_url,
            sheet_url=classification.sheet_url,
        )

    async def _load_geotiff(self) -> Image.Image:
        if self._geotiff_image is not None:
            return self._geotiff_image
        if not self.config.geotiff_url:
            raise ValueError("GeoTIFF URL is required for GeoTIFF-backed sampling")
        response = await self._http.get(self.config.geotiff_url)
        response.raise_for_status()
        image = Image.open(BytesIO(response.content))
        image.load()
        self._geotiff_image = image
        return image

    async def _request(
        self,
        url: str,
        params: Mapping[str, str],
    ) -> httpx.Response:
        request_url = _build_url_with_params(url, params)
        try:
            response = await self._http.get(request_url)
        except httpx.ConnectError as exc:
            if self._verify:
                logger.warning(
                    "[GisonRaster] TLS/connect error for %s; retrying with verify=False: %s",
                    url,
                    exc,
                )
                self._verify = False
                await self._classifier.aclose()
                self._http = self._build_client()
                self._classifier = GisonRasterIngestor(self._http)
                response = await self._http.get(request_url)
            else:
                raise
        response.raise_for_status()
        return response


async def run_gison_raster_ingest(
    *,
    wms_url: str,
    layer_name: str,
    legend_url: str,
    teryt_gmina: str,
    plan_type: str = "mpzp",
    source_srid: int = 2180,
    version: str = "1.1.1",
    styles: str = "default",
    info_format: str = "text/plain",
    sample_grid: int = _DEFAULT_SAMPLE_GRID,
    point_halfspan_m: float = _DEFAULT_HALFSPAN_M,
    min_confidence: float = 0.35,
    plan_name: str | None = None,
    swap_bbox_axes: bool = False,
    geotiff_url: str | None = None,
    sample_bbox_2180: tuple[float, float, float, float] | None = None,
) -> WFSIngestReport:
    ingestor = GisonRasterParcelIngestor(
        GisonRasterConfig(
            wms_url=wms_url,
            layer_name=layer_name,
            legend_url=legend_url,
            teryt_gmina=teryt_gmina,
            plan_type=plan_type,
            source_srid=source_srid,
            version=version,
            styles=styles,
            info_format=info_format,
            sample_grid=sample_grid,
            point_halfspan_m=point_halfspan_m,
            min_confidence=min_confidence,
            plan_name=plan_name,
            swap_bbox_axes=swap_bbox_axes,
            geotiff_url=geotiff_url,
            sample_bbox_2180=sample_bbox_2180,
        )
    )
    try:
        return await ingestor.run()
    finally:
        await ingestor.aclose()


def _looks_like_png(payload: bytes) -> bool:
    return payload.startswith(_PNG_SIGNATURE)


async def probe_gison_raster_source(
    *,
    wms_url: str,
    layer_name: str,
    legend_url: str,
    bbox: tuple[float, float, float, float],
    source_srid: int = 2180,
    version: str = "1.1.1",
    styles: str = "default",
    info_format: str = "text/plain",
    swap_bbox_axes: bool = False,
) -> GisonRasterProbeReport:
    """Probe one raster-backed WMS source at a single bbox center.

    This is an operational helper for municipalities that expose MPZP only
    through WMS + legend assets. It does not ingest data into the DB; it
    verifies whether a source is classifiable enough to justify wiring a full
    parcel-centric raster ingest path.
    """
    if swap_bbox_axes:
        bbox = (bbox[1], bbox[0], bbox[3], bbox[2])

    map_context = WMSGetMapContext(
        wms_url=wms_url,
        layers=layer_name,
        bbox=bbox,
        width=101,
        height=101,
        crs=f"EPSG:{source_srid}",
        version=version,
        image_format="image/png",
        styles=styles,
    )
    feature_info_context = WMSGetFeatureInfoContext(
        pixel_x=50,
        pixel_y=50,
        info_format=info_format,
        query_layers=layer_name,
    )

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0),
        follow_redirects=True,
    ) as http:
        getmap_response = await http.get(_build_url_with_params(wms_url, map_context.build_params()))
        getmap_response.raise_for_status()
        try:
            sampled_color = extract_representative_png_color(getmap_response.content)
        except ValueError as exc:
            return GisonRasterProbeReport(
                wms_url=wms_url,
                layer_name=layer_name,
                legend_url=legend_url,
                sampled_color_hex="-",
                legend_entries=0,
                designation=None,
                label=None,
                confidence=None,
                matched_by=None,
                plan_name_hint=None,
                raw_feature_info_excerpt=None,
                error=str(exc),
            )

        feature_info_response = await http.get(
            _build_url_with_params(wms_url, feature_info_context.build_params(map_context)),
        )
        feature_info_response.raise_for_status()
        raw_feature_info = _clean_text(feature_info_response.text)

        ingestor = GisonRasterIngestor(http)
        legend = await ingestor.fetch_legend_catalog(legend_url)
        classification = await ingestor.classify_sample(
            legend_url=legend_url,
            sampled_color=sampled_color,
            getmap_context=map_context,
            getfeatureinfo_context=WMSGetFeatureInfoContext(
                pixel_x=50,
                pixel_y=50,
                raw_text=feature_info_response.text,
                info_format=info_format,
                query_layers=layer_name,
            ),
        )

    hints = extract_feature_info_hints(
        WMSGetFeatureInfoContext(
            pixel_x=50,
            pixel_y=50,
            raw_text=raw_feature_info,
            info_format=info_format,
            query_layers=layer_name,
        )
    )

    return GisonRasterProbeReport(
        wms_url=wms_url,
        layer_name=layer_name,
        legend_url=legend_url,
        sampled_color_hex=sampled_color.as_hex(),
        legend_entries=len(legend.entries),
        designation=classification.designation if classification else None,
        label=classification.label if classification else None,
        confidence=classification.confidence if classification else None,
        matched_by=classification.matched_by if classification else None,
        plan_name_hint=hints.plan_name_hint,
        raw_feature_info_excerpt=raw_feature_info[:300] if raw_feature_info else None,
    )
