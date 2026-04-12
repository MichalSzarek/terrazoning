"""Lightweight validation-corpus scaffolding for the future-buildable release gate."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

VALIDATION_CORPUS_LABELS = ("true_positive", "tempting_false_positive", "true_negative")


@dataclass(slots=True)
class ValidationCorpusEntry:
    dzialka_id: str
    province: str
    label: str
    expected_band: str | None = None
    notes: str | None = None
    source_hint: str | None = None


@dataclass(slots=True)
class ValidationCorpusSummary:
    total: int = 0
    by_label: dict[str, int] = field(default_factory=dict)
    by_province: dict[str, int] = field(default_factory=dict)
    missing_expected_band: int = 0

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_validation_corpus(path: Path | str | None) -> list[ValidationCorpusEntry]:
    if path is None:
        return []

    corpus_path = Path(path)
    if not corpus_path.exists():
        return []

    payload = json.loads(corpus_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Validation corpus must be a JSON array")

    entries: list[ValidationCorpusEntry] = []
    for raw in payload:
        if not isinstance(raw, dict):
            raise ValueError("Validation corpus entries must be JSON objects")
        entry = ValidationCorpusEntry(
            dzialka_id=str(raw.get("dzialka_id") or raw.get("id") or "").strip(),
            province=str(raw.get("province") or "").strip(),
            label=str(raw.get("label") or "").strip(),
            expected_band=str(raw.get("expected_band") or "").strip() or None,
            notes=str(raw.get("notes") or "").strip() or None,
            source_hint=str(raw.get("source_hint") or "").strip() or None,
        )
        if not entry.dzialka_id or not entry.province or not entry.label:
            raise ValueError("Validation corpus entries require dzialka_id, province, and label")
        if entry.label not in VALIDATION_CORPUS_LABELS:
            raise ValueError(f"Unsupported validation corpus label: {entry.label}")
        entries.append(entry)
    return entries


def summarize_validation_corpus(entries: list[ValidationCorpusEntry]) -> ValidationCorpusSummary:
    summary = ValidationCorpusSummary()
    for entry in entries:
        summary.total += 1
        summary.by_label[entry.label] = summary.by_label.get(entry.label, 0) + 1
        summary.by_province[entry.province] = summary.by_province.get(entry.province, 0) + 1
        if not entry.expected_band:
            summary.missing_expected_band += 1
    return summary


def build_validation_corpus_report(entries: list[ValidationCorpusEntry]) -> dict[str, Any]:
    summary = summarize_validation_corpus(entries)
    return {
        "summary": summary.as_dict(),
        "entries": [asdict(entry) for entry in entries],
    }
