from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse
from uuid import UUID, uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.leads import router as leads_router
from app.core.database import get_db


class _FakeResult:
    def __init__(self, *, rows: list[dict] | None = None, scalar: int | None = None) -> None:
        self._rows = rows or []
        self._scalar = scalar

    def mappings(self) -> "_FakeResult":
        return self

    def all(self) -> list[dict]:
        return list(self._rows)

    def first(self) -> dict | None:
        return self._rows[0] if self._rows else None

    def scalar_one(self) -> int:
        if self._scalar is None:
            raise AssertionError("scalar_one() called without a scalar payload")
        return self._scalar


class _FakeSession:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    async def execute(self, stmt, params=None):  # noqa: ANN001
        sql = getattr(stmt, "text", str(stmt))

        if "COUNT(*)" in sql:
            return _FakeResult(scalar=len(self._rows))

        if params and params.get("lead_id") is not None:
            lead_id = params["lead_id"]
            match = next((row for row in self._rows if row["lead_id"] == lead_id), None)
            return _FakeResult(rows=[match] if match else [])

        return _FakeResult(rows=self._rows)


def _make_app(rows: list[dict]) -> FastAPI:
    app = FastAPI()
    app.include_router(leads_router, prefix="/api/v1")

    async def override_get_db():
        yield _FakeSession(rows)

    app.dependency_overrides[get_db] = override_get_db
    return app


def _lead_row(*, lead_id: UUID | None = None, raw_kw: str | None) -> dict:
    return {
        "lead_id": lead_id or uuid4(),
        "confidence_score": 0.92,
        "priority": "high",
        "strategy_type": "current_buildable",
        "confidence_band": None,
        "status": "new",
        "reviewed_at": None,
        "notes": None,
        "max_coverage_pct": 72.5,
        "max_buildable_area_m2": 2505.2,
        "dominant_przeznaczenie": "MN",
        "price_zl": 129000.0,
        "price_per_m2_zl": 178.45,
        "future_signal_score": None,
        "cheapness_score": None,
        "overall_score": None,
        "dominant_future_signal": None,
        "future_signal_count": None,
        "distance_to_nearest_buildable_m": None,
        "adjacent_buildable_pct": None,
        "listing_id": uuid4(),
        "source_url": (
            "https://licytacje.komornik.pl/wyszukiwarka/obwieszczenia-o-licytacji/"
            "34251/licytacja-nieruchomosci"
        ),
        "raw_kw": raw_kw,
        "evidence_chain": [
            {
                "step": "source",
                "ref": str(uuid4()),
                "url": "https://licytacje.komornik.pl/example",
            }
        ],
        "signal_breakdown": [],
        "created_at": datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc),
        "identyfikator": "240609206.60",
        "teryt_gmina": "2406092",
        "area_m2": 18825.0,
        "display_point": {"type": "Point", "coordinates": [19.1, 50.8]},
        "geometry": {"type": "Point", "coordinates": [19.1, 50.8]},
    }


def test_list_leads_includes_kw_fields_when_raw_kw_is_present() -> None:
    app = _make_app([_lead_row(raw_kw="KR1B/00079684/3")])

    with TestClient(app) as client:
        response = client.get("/api/v1/leads")

    assert response.status_code == 200
    body = response.json()
    props = body["features"][0]["properties"]
    parsed = urlparse(props["ekw_search_url"])
    query = parse_qs(parsed.query)

    assert props["kw_number"] == "KR1B/00079684/3"
    assert query["kodEci"] == ["KR1B"]
    assert query["kodWydzialuInput"] == ["KR1B"]
    assert query["numerKW"] == ["00079684"]
    assert query["cyfraKontrolna"] == ["3"]


def test_get_lead_returns_null_kw_fields_when_raw_kw_is_missing() -> None:
    lead_id = uuid4()
    app = _make_app([_lead_row(lead_id=lead_id, raw_kw=None)])

    with TestClient(app) as client:
        response = client.get(f"/api/v1/leads/{lead_id}")

    assert response.status_code == 200
    props = response.json()["properties"]

    assert props["kw_number"] is None
    assert props["ekw_search_url"] is None
