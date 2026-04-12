"""Lightweight persistence for manual-only backlog items archived from DLQ."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


_STORE_PATH = Path(__file__).resolve().parents[2] / ".runtime" / "manual_backlog.json"


@dataclass
class ManualBacklogRecord:
    listing_id: str
    province: str | None
    title: str | None
    source_url: str | None
    raw_teryt_input: str | None
    last_error: str | None
    category: str
    next_action: str
    attempt_count: int
    archived_at: str = datetime.now(timezone.utc).isoformat()


def load_manual_backlog() -> list[ManualBacklogRecord]:
    if not _STORE_PATH.exists():
        return []
    try:
        raw = json.loads(_STORE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []
    records: list[ManualBacklogRecord] = []
    for item in raw if isinstance(raw, list) else []:
        try:
            records.append(ManualBacklogRecord(**item))
        except TypeError:
            continue
    return records


def save_manual_backlog(records: list[ManualBacklogRecord]) -> None:
    _STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _STORE_PATH.write_text(
        json.dumps([asdict(record) for record in records], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def upsert_manual_backlog(records: list[ManualBacklogRecord]) -> int:
    if not records:
        return 0
    existing = {record.listing_id: record for record in load_manual_backlog()}
    for record in records:
        existing[record.listing_id] = record
    save_manual_backlog(list(existing.values()))
    return len(records)


def list_manual_backlog(*, province: str | None = None) -> list[ManualBacklogRecord]:
    records = load_manual_backlog()
    if province is None:
        return records
    return [record for record in records if record.province == province]
