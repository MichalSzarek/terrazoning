"""Lightweight server-side persistence for investor watchlist criteria."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


_STORE_PATH = Path(__file__).resolve().parents[2] / ".runtime" / "investor_watchlist.json"


@dataclass
class InvestorWatchlistRecord:
    max_price_per_m2: float | None = 150.0
    min_coverage_pct: float = 60.0
    min_confidence_pct: float = 80.0
    required_designation: str = ""
    only_reliable_price: bool = True
    acknowledged_at: str | None = None
    updated_at: str = datetime.now(timezone.utc).isoformat()


def _default_record() -> InvestorWatchlistRecord:
    return InvestorWatchlistRecord()


def load_watchlist() -> InvestorWatchlistRecord:
    if not _STORE_PATH.exists():
        return _default_record()

    try:
        raw = json.loads(_STORE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return _default_record()

    record = _default_record()
    for field, value in raw.items():
        if hasattr(record, field):
            setattr(record, field, value)
    return record


def save_watchlist(record: InvestorWatchlistRecord) -> InvestorWatchlistRecord:
    record.updated_at = datetime.now(timezone.utc).isoformat()
    _STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _STORE_PATH.write_text(
        json.dumps(asdict(record), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return record

