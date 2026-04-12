from app.services.wfs_downloader import _parse_pos_list


def test_parse_pos_list_supports_3d_gml_coordinates() -> None:
    coords = _parse_pos_list(
        "5570744.236 6572761.853 0 5570663.367 6572990.790 0 5570462.733 6572971.464 0",
        dimension=3,
    )

    assert coords == [
        (5570744.236, 6572761.853),
        (5570663.367, 6572990.79),
        (5570462.733, 6572971.464),
    ]
