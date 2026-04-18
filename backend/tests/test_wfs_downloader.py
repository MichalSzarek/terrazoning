from app.services.wfs_downloader import WFSClient, WFSFieldMapping


def test_parse_gml_feature_members_container_keeps_all_features() -> None:
    body = """<?xml version="1.0" encoding="UTF-8"?>
    <wfs:FeatureCollection
        xmlns:wfs="http://www.opengis.net/wfs"
        xmlns:gml="http://www.opengis.net/gml"
        xmlns:test="urn:test">
      <gml:featureMembers>
        <test:zone>
          <test:kod>MN1</test:kod>
          <gml:Polygon srsName="EPSG:2180" srsDimension="2">
            <gml:exterior>
              <gml:LinearRing>
                <gml:posList>700000 250000 700100 250000 700100 250100 700000 250100 700000 250000</gml:posList>
              </gml:LinearRing>
            </gml:exterior>
          </gml:Polygon>
        </test:zone>
        <test:zone>
          <test:kod>MN2</test:kod>
          <gml:Polygon srsName="EPSG:2180" srsDimension="2">
            <gml:exterior>
              <gml:LinearRing>
                <gml:posList>700200 250200 700300 250200 700300 250300 700200 250300 700200 250200</gml:posList>
              </gml:LinearRing>
            </gml:exterior>
          </gml:Polygon>
        </test:zone>
      </gml:featureMembers>
    </wfs:FeatureCollection>
    """

    client = WFSClient()
    features = client._parse_gml_response(body)

    assert len(features) == 2
    assert features[0]["properties"]["kod"] == "MN1"
    assert features[1]["properties"]["kod"] == "MN2"


def test_parse_gml_feature_members_ignores_non_element_children() -> None:
    body = """<?xml version="1.0" encoding="UTF-8"?>
    <wfs:FeatureCollection
        xmlns:wfs="http://www.opengis.net/wfs"
        xmlns:gml="http://www.opengis.net/gml"
        xmlns:test="urn:test">
      <gml:featureMembers>
        <!-- comment node emitted by some APP feeds -->
        <test:zone>
          <test:kod>MN1</test:kod>
          <gml:Polygon srsName="EPSG:2180" srsDimension="2">
            <gml:exterior>
              <gml:LinearRing>
                <gml:posList>700000 250000 700100 250000 700100 250100 700000 250100 700000 250000</gml:posList>
              </gml:LinearRing>
            </gml:exterior>
          </gml:Polygon>
        </test:zone>
      </gml:featureMembers>
    </wfs:FeatureCollection>
    """

    client = WFSClient()
    features = client._parse_gml_response(body)

    assert len(features) == 1
    assert features[0]["properties"]["kod"] == "MN1"


def test_parse_feature_applies_swap_xy_even_for_epsg_2180() -> None:
    client = WFSClient()
    raw = {
        "type": "Feature",
        "properties": {"kod": "MN1"},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [250000.0, 730000.0],
                [250100.0, 730000.0],
                [250100.0, 730100.0],
                [250000.0, 730100.0],
                [250000.0, 730000.0],
            ]],
        },
    }

    feature = client._parse_feature(
        raw,
        plan_type="mpzp",
        teryt_gmina="1810011",
        source_srid=2180,
        mapping=WFSFieldMapping(przeznaczenie="kod"),
        wfs_url="http://example.test/wfs",
        swap_xy=True,
    )

    assert feature is not None
    assert round(feature.geom.centroid.x) == 730050
    assert round(feature.geom.centroid.y) == 250050
