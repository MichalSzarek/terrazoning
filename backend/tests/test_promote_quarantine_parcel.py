from promote_quarantine_parcel import _score_candidate, parse_args


def test_parse_args_accepts_operator_flags() -> None:
    args = parse_args(
        [
            "--province",
            "podkarpackie",
            "--teryt",
            "1816145",
            "--manual-przeznaczenie",
            "MN",
            "--source-url-contains",
            "warunkami-zabudowy",
            "--auto-pick",
            "--apply",
        ]
    )

    assert args.province == "podkarpackie"
    assert args.teryt == "1816145"
    assert args.manual_przeznaczenie == "MN"
    assert args.source_url_contains == ["warunkami-zabudowy"]
    assert args.auto_pick is True
    assert args.apply is True


def test_score_candidate_prefers_investment_and_wz_signals() -> None:
    strong = _score_candidate(
        area_m2=1164.71,
        current_use="B",
        source_url=(
            "https://licytacje.komornik.pl/.../borek-stary-dzialki-inwestycyjne-"
            "widokowe-z-warunkami-zabudowy"
        ),
    )
    weak = _score_candidate(
        area_m2=1164.71,
        current_use="B",
        source_url="https://licytacje.komornik.pl/.../nieruchomosc-gruntowa-rolna",
    )

    assert strong.score > weak.score
    assert "listing_mentions_wz" in strong.reasons
    assert "listing_mentions_investment" in strong.reasons
    assert "listing_mentions_view" in strong.reasons
    assert "listing_mentions_agricultural" in weak.reasons


def test_score_candidate_rewards_nonbuilt_current_use() -> None:
    nonbuilt = _score_candidate(area_m2=2500.0, current_use="R", source_url=None)
    built = _score_candidate(area_m2=2500.0, current_use="B", source_url=None)

    assert nonbuilt.score > built.score
    assert "current_use_nonbuilt" in nonbuilt.reasons
