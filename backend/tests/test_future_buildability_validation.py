from pathlib import Path

from app.services.future_buildability_validation import (
    ValidationCorpusEntry,
    build_validation_corpus_report,
    load_validation_corpus,
    summarize_validation_corpus,
)
from print_future_buildability_validation_report import parse_args as parse_validation_report_args


def test_validation_corpus_summary_counts_labels_and_provinces() -> None:
    entries = [
        ValidationCorpusEntry(
            dzialka_id="a",
            province="12",
            label="true_positive",
            expected_band="formal",
        ),
        ValidationCorpusEntry(
            dzialka_id="b",
            province="12",
            label="tempting_false_positive",
            expected_band="supported",
        ),
        ValidationCorpusEntry(
            dzialka_id="c",
            province="24",
            label="true_negative",
        ),
    ]

    summary = summarize_validation_corpus(entries)

    assert summary.total == 3
    assert summary.by_label == {
        "true_positive": 1,
        "tempting_false_positive": 1,
        "true_negative": 1,
    }
    assert summary.by_province == {"12": 2, "24": 1}
    assert summary.missing_expected_band == 1


def test_validation_corpus_loader_and_report_accept_json_array(tmp_path: Path) -> None:
    corpus_path = tmp_path / "corpus.json"
    corpus_path.write_text(
        """
        [
          {"dzialka_id": "x", "province": "12", "label": "true_positive", "expected_band": "formal"},
          {"dzialka_id": "y", "province": "24", "label": "true_negative"}
        ]
        """.strip(),
        encoding="utf-8",
    )

    entries = load_validation_corpus(corpus_path)
    report = build_validation_corpus_report(entries)

    assert len(entries) == 2
    assert report["summary"]["total"] == 2
    assert report["summary"]["by_label"]["true_positive"] == 1
    assert report["summary"]["by_province"]["12"] == 1


def test_validation_report_parse_args_accepts_custom_corpus_path() -> None:
    args = parse_validation_report_args(
        [
            "--corpus-path",
            "data/custom_future_buildability_validation.json",
            "--format",
            "json",
        ]
    )

    assert str(args.corpus_path) == "data/custom_future_buildability_validation.json"
    assert args.format == "json"
