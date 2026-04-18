from __future__ import annotations

import io
import zipfile

from app.services.app_gml_ingestor import parse_app_gml_payload


def _make_zip_with_text(filename: str, content: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as zf:
        zf.writestr(filename, content)
    return buffer.getvalue()


def test_parse_app_gml_payload_maps_project_extent_to_non_buildable_zone() -> None:
    body = """<?xml version="1.0" encoding="UTF-8"?>
    <wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0"
        xmlns:app="https://www.gov.pl/static/zagospodarowanieprzestrzenne/schemas/app/1.0"
        xmlns:gml="http://www.opengis.net/gml/3.2">
      <wfs:member>
        <app:AktPlanowaniaPrzestrzennego gml:id="app.1">
          <gml:identifier>https://example.test/app/180804-MPZP/6.2024_akt</gml:identifier>
          <app:tytul>Przystąpienia do sporządzenia miejscowego planu zagospodarowania przestrzennego terenu wsi Giedlarowa</app:tytul>
          <app:obowiazujeOd>2024-01-29</app:obowiazujeOd>
          <app:zasiegPrzestrzenny>
            <gml:MultiSurface srsName="http://www.opengis.net/def/crs/EPSG/0/2178">
              <gml:surfaceMember>
                <gml:Polygon>
                  <gml:exterior>
                    <gml:LinearRing>
                      <gml:posList>5560000 7580000 5560100 7580000 5560100 7580100 5560000 7580100 5560000 7580000</gml:posList>
                    </gml:LinearRing>
                  </gml:exterior>
                </gml:Polygon>
              </gml:surfaceMember>
            </gml:MultiSurface>
          </app:zasiegPrzestrzenny>
        </app:AktPlanowaniaPrzestrzennego>
      </wfs:member>
    </wfs:FeatureCollection>
    """

    payload = _make_zip_with_text("giedlarowa/APP 6.2024.gml", body)
    features = parse_app_gml_payload(
        payload,
        source_url="https://example.test/giedlarowa.zip",
        teryt_gmina="1808042",
        source_srid=2178,
        fixed_designation="MPZP_PROJ",
        plan_type="mpzp",
        swap_xy=True,
        description_prefix="APP project extent",
    )

    assert len(features) == 1
    feature = features[0]
    assert feature.plan_type == "mpzp"
    assert feature.przeznaczenie == "MPZP_PROJ"
    assert feature.plan_name.startswith("Przystąpienia do sporządzenia")
    assert feature.przeznaczenie_opis.startswith("APP project extent:")
    assert round(feature.geom.centroid.x) > 700000
    assert round(feature.geom.centroid.y) > 200000
