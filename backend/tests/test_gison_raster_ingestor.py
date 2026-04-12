from __future__ import annotations

import struct
import zlib
from io import BytesIO
from unittest.mock import patch

import httpx
from shapely.geometry import Point

from app.services.gison_raster_ingestor import (
    GisonLegendCatalog,
    GisonLegendEntry,
    GisonRasterIngestor,
    RGBColor,
    WMSGetFeatureInfoContext,
    WMSGetMapContext,
    _build_url_with_params,
    _pixel_from_bbox,
    _sample_paletted_pixel_neighborhood,
    classify_against_legend,
    extract_feature_info_hints,
    extract_gison_sheet_urls,
    extract_representative_png_color,
    probe_gison_raster_source,
    parse_gison_sheet_legend,
)
from PIL import Image


def _png_chunk(chunk_type: bytes, data: bytes) -> bytes:
    crc = zlib.crc32(chunk_type + data) & 0xFFFFFFFF
    return struct.pack(">I", len(data)) + chunk_type + data + struct.pack(">I", crc)


def _make_png(color: tuple[int, int, int], *, width: int = 4, height: int = 4) -> bytes:
    row = bytes(color) * width
    raw = b"".join(b"\x00" + row for _ in range(height))
    return (
        b"\x89PNG\r\n\x1a\n"
        + _png_chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + _png_chunk(b"IDAT", zlib.compress(raw))
        + _png_chunk(b"IEND", b"")
    )


def test_extract_gison_sheet_urls_from_frameset_html() -> None:
    html = """
    <html>
      <head>
        <link id="shLink" href="legenda_pliki/sheet001.html">
        <link id="shLink" href="legenda_pliki/sheet002.html">
      </head>
    </html>
    """

    urls = extract_gison_sheet_urls(html, "https://example.test/legenda.html")

    assert urls == [
        "https://example.test/legenda_pliki/sheet001.html",
        "https://example.test/legenda_pliki/sheet002.html",
    ]


def test_parse_gison_sheet_legend_extracts_label_and_image() -> None:
    sheet_html = """
    <html>
      <body>
        <table>
          <tr><td>LEGENDA</td></tr>
          <tr><td colspan="2">dla Miejscowy plan zagospodarowania przestrzennego Knurów</td></tr>
          <tr>
            <td><img src="image001.png"></td>
            <td>1MN - teren zabudowy mieszkaniowej jednorodzinnej</td>
          </tr>
          <tr>
            <td><img src="image002.png"></td>
            <td>2U - teren zabudowy usługowej</td>
          </tr>
        </table>
      </body>
    </html>
    """

    title, entries = parse_gison_sheet_legend(
        sheet_html,
        "https://example.test/legendy/sheet001.html",
    )

    assert title == "Miejscowy plan zagospodarowania przestrzennego Knurów"
    assert len(entries) == 2
    assert entries[0].image_url == "https://example.test/legendy/image001.png"
    assert entries[0].label == "1MN - teren zabudowy mieszkaniowej jednorodzinnej"
    assert entries[0].designation == "MN"
    assert entries[1].designation == "U"


def test_extract_representative_png_color_reads_truecolor_png() -> None:
    color = extract_representative_png_color(_make_png((34, 139, 34)))

    assert color == RGBColor(34, 139, 34)


def test_extract_representative_png_color_reads_jpeg_payload() -> None:
    image = Image.new("RGB", (6, 6), (204, 116, 110))
    buffer = BytesIO()
    image.save(buffer, format="JPEG", quality=95)

    color = extract_representative_png_color(buffer.getvalue())

    assert abs(color.r - 204) <= 6
    assert abs(color.g - 116) <= 6
    assert abs(color.b - 110) <= 6


def test_extract_representative_png_color_rejects_non_image_payload() -> None:
    try:
        extract_representative_png_color(b"<html>not an image</html>")
    except ValueError as exc:
        assert "not a decodable image" in str(exc)
    else:
        raise AssertionError("expected ValueError for non-image payload")


def test_extract_feature_info_hints_uses_attributes_and_raw_text() -> None:
    hints = extract_feature_info_hints(
        WMSGetFeatureInfoContext(
            raw_text="numerplanu=KN_12; warstwa=mpzp;",
            attributes={
                "oznaczenieterenuzrastra": "16MNU",
            },
        )
    )

    assert hints.designation_hint == "MNU"
    assert hints.plan_name_hint == "KN_12"


def test_wms_context_builders_generate_request_params() -> None:
    getmap = WMSGetMapContext(
        wms_url="https://example.test/wms",
        layers="mpzp",
        bbox=(1.0, 2.0, 3.0, 4.0),
        width=256,
        height=128,
    )
    feature_info = WMSGetFeatureInfoContext(pixel_x=20, pixel_y=30, query_layers="mpzp")

    map_params = getmap.build_params()
    info_params = feature_info.build_params(getmap)

    assert map_params["REQUEST"] == "GetMap"
    assert map_params["BBOX"] == "1.0,2.0,3.0,4.0"
    assert info_params["REQUEST"] == "GetFeatureInfo"
    assert info_params["QUERY_LAYERS"] == "mpzp"
    assert info_params["I"] == "20"
    assert info_params["J"] == "30"


def test_build_url_with_params_preserves_existing_query_items() -> None:
    url = _build_url_with_params(
        "https://example.test/cgi-bin/mapserv?map=/srv/mpzp/chybie.map",
        {
            "SERVICE": "WMS",
            "REQUEST": "GetMap",
        },
    )

    assert "map=%2Fsrv%2Fmpzp%2Fchybie.map" in url
    assert "SERVICE=WMS" in url
    assert "REQUEST=GetMap" in url


def test_classify_against_legend_prefers_designation_hint_when_available() -> None:
    legend = GisonLegendCatalog(
        legend_url="https://example.test/legend.html",
        title="Knurów",
        entries=(
            GisonLegendEntry(
                label="1MN - teren zabudowy mieszkaniowej jednorodzinnej",
                designation="MN",
                color=RGBColor(220, 60, 60),
                image_url="https://example.test/mn.png",
                sheet_url="https://example.test/sheet001.html",
            ),
            GisonLegendEntry(
                label="2U - teren zabudowy usługowej",
                designation="U",
                color=RGBColor(20, 180, 60),
                image_url="https://example.test/u.png",
                sheet_url="https://example.test/sheet001.html",
            ),
        ),
    )

    result = classify_against_legend(
        sampled_color=(210, 70, 70),
        legend=legend,
        getfeatureinfo_context=WMSGetFeatureInfoContext(
            raw_text="oznaczenie=1MN; numerplanu=KN_12"
        ),
    )

    assert result is not None
    assert result.designation == "MN"
    assert result.label.startswith("1MN")
    assert result.matched_by == "designation_hint"


async def test_gison_raster_ingestor_fetches_frameset_sheet_and_png_swatches() -> None:
    frameset_html = """
    <html>
      <head>
        <link id="shLink" href="legenda_pliki/sheet001.html">
      </head>
    </html>
    """
    sheet_html = """
    <html>
      <body>
        <table>
          <tr><td>LEGENDA</td></tr>
          <tr><td colspan="2">dla Knurów test legendy</td></tr>
          <tr>
            <td><img src="image001.png"></td>
            <td>1MN - teren zabudowy mieszkaniowej jednorodzinnej</td>
          </tr>
          <tr>
            <td><img src="image002.png"></td>
            <td>2U - teren zabudowy usługowej</td>
          </tr>
        </table>
      </body>
    </html>
    """
    assets = {
        "https://example.test/legenda.html": (frameset_html.encode("utf-8"), "text/html"),
        "https://example.test/legenda_pliki/sheet001.html": (sheet_html.encode("utf-8"), "text/html"),
        "https://example.test/legenda_pliki/image001.png": (_make_png((200, 50, 50)), "image/png"),
        "https://example.test/legenda_pliki/image002.png": (_make_png((50, 160, 60)), "image/png"),
    }

    def handler(request: httpx.Request) -> httpx.Response:
        payload, content_type = assets[str(request.url)]
        return httpx.Response(200, content=payload, headers={"content-type": content_type})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        ingestor = GisonRasterIngestor(http_client=client)
        legend = await ingestor.fetch_legend_catalog("https://example.test/legenda.html")
        result = await ingestor.classify_sample(
            legend_url="https://example.test/legenda.html",
            sampled_color=(48, 158, 58),
            getfeatureinfo_context=WMSGetFeatureInfoContext(
                raw_text="symbol=2U; numerplanu=KN_12"
            ),
        )

    assert legend.title == "Knurów test legendy"
    assert len(legend.entries) == 2
    assert result is not None
    assert result.designation == "U"
    assert result.label.startswith("2U")
    assert result.color == RGBColor(50, 160, 60)


async def test_probe_gison_raster_source_returns_error_for_invalid_wms_payload() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("REQUEST") == "GetMap":
            return httpx.Response(
                200,
                content=b"<html>gateway response</html>",
                headers={"content-type": "text/html"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    class _PatchedAsyncClient(httpx.AsyncClient):
        def __init__(self, *args, **kwargs):
            super().__init__(transport=httpx.MockTransport(handler), *args, **kwargs)

    with patch("app.services.gison_raster_ingestor.httpx.AsyncClient", _PatchedAsyncClient):
        report = await probe_gison_raster_source(
            wms_url="https://example.test/wms",
            layer_name="mpzp",
            legend_url="https://example.test/legend.png",
            bbox=(1.0, 2.0, 3.0, 4.0),
        )

    assert report.sampled_color_hex == "-"
    assert report.error == "Raster payload is not a decodable image"


async def test_fetch_legend_catalog_uses_manual_override_for_jelesnia_pdf() -> None:
    legend_url = "https://mpzp.igeomap.pl/doc/zywiec/jelesnia/009_legenda.pdf"

    ingestor = GisonRasterIngestor()
    try:
        legend = await ingestor.fetch_legend_catalog(legend_url)
    finally:
        await ingestor.aclose()

    designations = {entry.designation for entry in legend.entries}

    assert legend.title == "manual override"
    assert len(legend.entries) >= 10
    assert "R" in designations
    assert "ZL" in designations
    assert "U" in designations


async def test_fetch_legend_catalog_uses_manual_override_for_andrychow_pdf() -> None:
    legend_url = "https://mpzp.igeomap.pl/doc/wadowice/andrychow/06.pdf"

    ingestor = GisonRasterIngestor()
    try:
        legend = await ingestor.fetch_legend_catalog(legend_url)
    finally:
        await ingestor.aclose()

    by_hex = {entry.color.as_hex(): entry for entry in legend.entries}

    assert legend.title == "manual override"
    assert "#EEDDDD" in by_hex
    assert by_hex["#EEDDDD"].label == "UU"
    assert by_hex["#EEDDDD"].designation == "U"


async def test_fetch_legend_catalog_uses_manual_override_for_mykanow_jpg() -> None:
    legend_url = "https://mpzp.igeomap.pl/doc/czestochowa/mykanow/082.jpg"

    ingestor = GisonRasterIngestor()
    try:
        legend = await ingestor.fetch_legend_catalog(legend_url)
    finally:
        await ingestor.aclose()

    by_hex = {entry.color.as_hex(): entry for entry in legend.entries}

    assert legend.title == "manual override"
    assert "#8E799C" in by_hex
    assert by_hex["#8E799C"].label == "PEF"
    assert by_hex["#8E799C"].designation == "PEF"
    assert "#C58466" in by_hex
    assert by_hex["#C58466"].designation == "MN/U"


async def test_fetch_legend_catalog_uses_manual_override_for_zegocina_jpg() -> None:
    legend_url = "https://mpzp.igeomap.pl/doc/bochnia/zegocina/001.jpg"

    ingestor = GisonRasterIngestor()
    try:
        legend = await ingestor.fetch_legend_catalog(legend_url)
    finally:
        await ingestor.aclose()

    by_hex = {entry.color.as_hex(): entry for entry in legend.entries}

    assert legend.title == "manual override"
    assert "#DD6F00" in by_hex
    assert by_hex["#DD6F00"].label == "MN/MNU"
    assert by_hex["#DD6F00"].designation == "MN/U"
    assert "#B75C00" in by_hex
    assert by_hex["#B75C00"].designation == "MW"


async def test_fetch_legend_catalog_uses_manual_override_for_bochnia_pdf() -> None:
    legend_url = "https://mpzp.igeomap.pl/doc/bochnia/bochnia/003_legenda.pdf"

    ingestor = GisonRasterIngestor()
    try:
        legend = await ingestor.fetch_legend_catalog(legend_url)
    finally:
        await ingestor.aclose()

    by_hex = {entry.color.as_hex(): entry for entry in legend.entries}

    assert legend.title == "manual override"
    assert "#F0FAB2" in by_hex
    assert by_hex["#F0FAB2"].designation == "R"
    assert "#D8BF73" in by_hex
    assert by_hex["#D8BF73"].designation == "MN"


async def test_fetch_legend_catalog_uses_manual_override_for_kamienica_jpg() -> None:
    legend_url = "https://mpzp.igeomap.pl/doc/czestochowa/kamienicapolska/003.jpg"

    ingestor = GisonRasterIngestor()
    try:
        legend = await ingestor.fetch_legend_catalog(legend_url)
    finally:
        await ingestor.aclose()

    by_hex = {entry.color.as_hex(): entry for entry in legend.entries}

    assert legend.title == "manual override"
    assert "#E5E4EE" in by_hex
    assert by_hex["#E5E4EE"].designation == "KD-D"
    assert "#D9D7E0" in by_hex
    assert by_hex["#D9D7E0"].designation == "MN/U"


async def test_fetch_legend_catalog_uses_manual_override_for_zawoja_jpg() -> None:
    legend_url = "https://rastry.gison.pl/mpzp-public/zawoja/legendy/Z01_2019_84_X_legenda.jpg"

    ingestor = GisonRasterIngestor()
    try:
        legend = await ingestor.fetch_legend_catalog(legend_url)
    finally:
        await ingestor.aclose()

    by_hex = {entry.color.as_hex(): entry for entry in legend.entries}

    assert legend.title == "manual override"
    assert "#DCB7B1" in by_hex
    assert by_hex["#DCB7B1"].designation == "MN"
    assert "#E0FBE6" in by_hex
    assert by_hex["#E0FBE6"].designation == "ZR"
    assert "#3C8C8D" in by_hex
    assert by_hex["#3C8C8D"].designation == "WS"


async def test_fetch_legend_catalog_uses_manual_override_for_pawlowice_jpg() -> None:
    legend_url = "https://mpzp.igeomap.pl/doc/pszczyna/pawlowice/011.jpg"

    ingestor = GisonRasterIngestor()
    try:
        legend = await ingestor.fetch_legend_catalog(legend_url)
    finally:
        await ingestor.aclose()

    by_hex = {entry.color.as_hex(): entry for entry in legend.entries}

    assert legend.title == "manual override"
    assert "#CC746E" in by_hex
    assert by_hex["#CC746E"].designation == "U"
    assert "#C38280" in by_hex
    assert by_hex["#C38280"].designation == "US"
    assert "#E0B55D" in by_hex
    assert by_hex["#E0B55D"].designation == "MN"


async def test_fetch_legend_catalog_uses_manual_override_for_zarki_png() -> None:
    legend_url = "https://rastry.gison.pl/mpzp-public/zarki/legendy/Z03_2014_280_XLI_legenda.png"

    ingestor = GisonRasterIngestor()
    try:
        legend = await ingestor.fetch_legend_catalog(legend_url)
    finally:
        await ingestor.aclose()

    by_hex = {entry.color.as_hex(): entry for entry in legend.entries}

    assert legend.title == "manual override"
    assert "#EECCA4" in by_hex
    assert by_hex["#EECCA4"].designation == "ML"
    assert "#C9873A" in by_hex
    assert by_hex["#C9873A"].designation == "MN"


def test_sample_paletted_pixel_neighborhood_prefers_fill_over_black_linework() -> None:
    image = Image.new("P", (9, 9))
    palette = [0] * (256 * 3)
    palette[0:3] = [0, 0, 0]
    palette[3:6] = [255, 255, 255]
    palette[6:9] = [238, 221, 221]  # #EEDDDD
    image.putpalette(palette)

    for x in range(9):
        for y in range(9):
            image.putpixel((x, y), 2)
    for y in range(9):
        image.putpixel((4, y), 0)

    sampled = _sample_paletted_pixel_neighborhood(image, center=(4, 4), radius=4)

    assert sampled == RGBColor(238, 221, 221)


def test_pixel_from_bbox_maps_center_point_into_raster_extent() -> None:
    pixel = _pixel_from_bbox(
        Point(15, 15),
        bbox=(10, 10, 20, 20),
        width=101,
        height=101,
    )

    assert pixel == (50, 50)
