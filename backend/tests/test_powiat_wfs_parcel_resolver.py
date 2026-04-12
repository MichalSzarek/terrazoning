from app.services.powiat_wfs_parcel_resolver import PowiatWfsParcelResolver


_ZAKOPANE_XML = """<?xml version='1.0' encoding='UTF-8'?>
<wfs:FeatureCollection xmlns:ms="http://mapserver.gis.umn.edu/mapserver"
    xmlns:gml="http://www.opengis.net/gml/3.2"
    xmlns:wfs="http://www.opengis.net/wfs/2.0">
  <wfs:member>
    <ms:dzialki>
      <ms:geometry>
        <gml:Polygon srsName="urn:ogc:def:crs:EPSG::2180">
          <gml:exterior>
            <gml:LinearRing>
              <gml:posList>158648 567852 158752 567852 158752 567898 158648 567898 158648 567852</gml:posList>
            </gml:LinearRing>
          </gml:exterior>
        </gml:Polygon>
      </ms:geometry>
      <ms:ID_DZIALKI>121701_1.0005.344/1</ms:ID_DZIALKI>
      <ms:NUMER_DZIALKI>344/1</ms:NUMER_DZIALKI>
      <ms:NAZWA_OBREBU>005</ms:NAZWA_OBREBU>
      <ms:NUMER_OBREBU>0005</ms:NUMER_OBREBU>
      <ms:NAZWA_GMINY>Zakopane</ms:NAZWA_GMINY>
    </ms:dzialki>
  </wfs:member>
  <wfs:member>
    <ms:dzialki>
      <ms:geometry>
        <gml:Polygon srsName="urn:ogc:def:crs:EPSG::2180">
          <gml:exterior>
            <gml:LinearRing>
              <gml:posList>158600 567800 158700 567800 158700 567860 158600 567860 158600 567800</gml:posList>
            </gml:LinearRing>
          </gml:exterior>
        </gml:Polygon>
      </ms:geometry>
      <ms:ID_DZIALKI>121701_1.0007.344/1</ms:ID_DZIALKI>
      <ms:NUMER_DZIALKI>344/1</ms:NUMER_DZIALKI>
      <ms:NAZWA_OBREBU>007</ms:NAZWA_OBREBU>
      <ms:NUMER_OBREBU>0007</ms:NUMER_OBREBU>
      <ms:NAZWA_GMINY>Zakopane</ms:NAZWA_GMINY>
    </ms:dzialki>
  </wfs:member>
</wfs:FeatureCollection>
"""

_ANDRYCHOW_XML = """<?xml version='1.0' encoding='UTF-8'?>
<wfs:FeatureCollection xmlns:ms="http://mapserver.gis.umn.edu/mapserver"
    xmlns:gml="http://www.opengis.net/gml"
    xmlns:wfs="http://www.opengis.net/wfs">
  <gml:featureMember>
    <ms:dzialki>
      <ms:geometry>
        <gml:Polygon srsName="EPSG:2180">
          <gml:exterior>
            <gml:LinearRing>
              <gml:posList>222053 523785 222215 523785 222215 523930 222053 523930 222053 523785</gml:posList>
            </gml:LinearRing>
          </gml:exterior>
        </gml:Polygon>
      </ms:geometry>
      <ms:ID_DZIALKI>121801_4.0001.2998/61</ms:ID_DZIALKI>
      <ms:NUMER_DZIALKI>2998/61</ms:NUMER_DZIALKI>
      <ms:NAZWA_OBREBU>Andrychow miasto</ms:NAZWA_OBREBU>
      <ms:NUMER_OBREBU>0001</ms:NUMER_OBREBU>
      <ms:NAZWA_GMINY>Andrychow - miasto</ms:NAZWA_GMINY>
    </ms:dzialki>
  </gml:featureMember>
</wfs:FeatureCollection>
"""

_ZUBRZYCA_XML = """<?xml version='1.0' encoding='UTF-8'?>
<wfs:FeatureCollection xmlns:ewns="http://xsd.geoportal2.pl/ewns"
    xmlns:gml="http://www.opengis.net/gml/3.2"
    xmlns:wfs="http://www.opengis.net/wfs/2.0">
  <wfs:member>
    <ewns:dzialki>
      <ewns:geometria>
        <gml:Polygon srsName="urn:ogc:def:crs:EPSG::2180">
          <gml:exterior>
            <gml:LinearRing>
              <gml:posList>188532 547506 188560 547506 188560 547514 188532 547514 188532 547506</gml:posList>
            </gml:LinearRing>
          </gml:exterior>
        </gml:Polygon>
      </ewns:geometria>
      <ewns:ID_DZIALKI>121105_2.0007.4474</ewns:ID_DZIALKI>
      <ewns:NUMER_DZIALKI>4474</ewns:NUMER_DZIALKI>
      <ewns:NUMER_OBREBU>0007</ewns:NUMER_OBREBU>
      <ewns:NAZWA_OBREBU>ZUBRZYCA GÓRNA</ewns:NAZWA_OBREBU>
      <ewns:NAZWA_GMINY>JABŁONKA</ewns:NAZWA_GMINY>
    </ewns:dzialki>
  </wfs:member>
</wfs:FeatureCollection>
"""

_CHRZANOW_XML = """<?xml version='1.0' encoding='UTF-8'?>
<wfs:FeatureCollection xmlns:ms="http://mapserver.gis.umn.edu/mapserver"
    xmlns:gml="http://www.opengis.net/gml"
    xmlns:wfs="http://www.opengis.net/wfs">
  <gml:featureMember>
    <ms:dzialki>
      <ms:geometry>
        <gml:Polygon srsName="EPSG:2180">
          <gml:exterior>
            <gml:LinearRing>
              <gml:posList>250392 526031 250651 526031 250651 526600 250392 526600 250392 526031</gml:posList>
            </gml:LinearRing>
          </gml:exterior>
        </gml:Polygon>
      </ms:geometry>
      <ms:ID_DZIALKI>120303_4.0001.1155/17</ms:ID_DZIALKI>
      <ms:NUMER_DZIALKI>1155/17</ms:NUMER_DZIALKI>
      <ms:NAZWA_OBREBU>Chrzanów</ms:NAZWA_OBREBU>
      <ms:NUMER_OBREBU>0001</ms:NUMER_OBREBU>
      <ms:NAZWA_GMINY>Chrzanów - miasto</ms:NAZWA_GMINY>
    </ms:dzialki>
  </gml:featureMember>
</wfs:FeatureCollection>
"""


class _Resolver(PowiatWfsParcelResolver):
    def __init__(self) -> None:
        super().__init__()
        self.calls: list[tuple[str, str]] = []

    async def _fetch(self, url: str, *, params: dict[str, str]) -> str:
        self.calls.append((url, params.get("filter", "")))
        if "tatrzanski-wms" in url:
            return _ZAKOPANE_XML
        if "nowotarski.geoportal2" in url:
            return _ZUBRZYCA_XML
        if "chrzanowski.webewid" in url:
            return _CHRZANOW_XML
        return _ANDRYCHOW_XML


class _ZakopaneInferenceResolver(PowiatWfsParcelResolver):
    async def _query_zakopane(self, parcel_number: str):
        fixtures = {
            "101/2": [
                self._feature("121701_1.0011.101/2", "101/2", "011", "0011", "Zakopane"),
                self._feature("121701_1.0106.101/2", "101/2", "106", "0106", "Zakopane"),
            ],
            "106/2": [
                self._feature("121701_1.0011.106/2", "106/2", "011", "0011", "Zakopane"),
                self._feature("121701_1.0058.106/2", "106/2", "058", "0058", "Zakopane"),
            ],
            "106/3": [
                self._feature("121701_1.0011.106/3", "106/3", "011", "0011", "Zakopane"),
                self._feature("121701_1.0071.106/3", "106/3", "071", "0071", "Zakopane"),
            ],
        }
        return fixtures.get(parcel_number, [])

    @staticmethod
    def _feature(identifier: str, parcel_number: str, region_name: str, region_code: str, commune_name: str):
        from shapely.geometry import MultiPolygon, Polygon
        from app.services.powiat_wfs_parcel_resolver import PowiatFeature

        return PowiatFeature(
            identifier=identifier,
            parcel_number=parcel_number,
            region_name=region_name,
            region_code=region_code,
            commune_name=commune_name,
            shape=MultiPolygon([Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])]),
        )


async def test_resolve_zakopane_filters_by_explicit_region_code() -> None:
    resolver = _Resolver()

    parcels = await resolver.resolve(
        raw_obreb="Zakopane",
        raw_gmina="Zakopane",
        plain_text="Nieruchomosc polozona w Zakopanem, obreb nr 5, dzialka 344/1.",
        parcel_numbers=("344/1",),
    )

    assert [parcel.identyfikator for parcel in parcels] == ["121701105.344/1"]


async def test_resolve_andrychow_recovers_city_parcel_from_powiat_wfs() -> None:
    resolver = _Resolver()

    parcels = await resolver.resolve(
        raw_obreb="Andrychow",
        raw_gmina=None,
        plain_text="Dzialki nr 2998/61 sa polozone w Andrychowie.",
        parcel_numbers=("2998/61",),
    )

    assert [parcel.identyfikator for parcel in parcels] == ["121801401.2998/61"]


async def test_resolve_zubrzyca_gorna_uses_nowotarski_wfs() -> None:
    resolver = _Resolver()

    parcels = await resolver.resolve(
        raw_obreb="Zubrzyca Górna",
        raw_gmina="Jabłonka",
        plain_text="Nieruchomość położona jest w Zubrzycy Górnej, gmina Jabłonka.",
        parcel_numbers=("4474",),
    )

    assert [parcel.identyfikator for parcel in parcels] == ["121105207.4474"]


async def test_resolve_kroczymiech_normalizes_to_chrzanow_city_wfs() -> None:
    resolver = _Resolver()

    parcels = await resolver.resolve(
        raw_obreb="Kroczymiech",
        raw_gmina=None,
        plain_text="Nieruchomość położona przy ul. Kroczymiech w Chrzanowie.",
        parcel_numbers=("1155/17",),
    )

    assert [parcel.identyfikator for parcel in parcels] == ["120303401.1155/17"]


async def test_resolve_zakopane_infers_shared_region_from_multiple_notice_parcels() -> None:
    resolver = _ZakopaneInferenceResolver()

    parcels = await resolver.resolve(
        raw_obreb="Zakopanem",
        raw_gmina="Zakopane",
        plain_text="Nieruchomość położona w Zakopanem bez jawnego numeru obrębu.",
        parcel_numbers=("101/2", "106/2", "106/3"),
    )

    assert [parcel.identyfikator for parcel in parcels] == [
        "121701111.101/2",
        "121701111.106/2",
        "121701111.106/3",
    ]
