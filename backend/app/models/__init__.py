# SQLAlchemy ORM models — Bronze / Silver / Gold layers
# Import all models here so Alembic's env.py sees them via Base.metadata.
from app.models.base import Base  # noqa: F401
from app.models.bronze import RawDocument, RawListing, ScrapeRun  # noqa: F401
from app.models.silver import DlqParcel, Dzialka, KsiegaWieczysta, ListingParcel  # noqa: F401
from app.models.gold import DeltaResult, InvestmentLead, PlanningZone  # noqa: F401

__all__ = [
    "Base",
    # Bronze
    "ScrapeRun",
    "RawListing",
    "RawDocument",
    # Silver
    "Dzialka",
    "KsiegaWieczysta",
    "ListingParcel",
    "DlqParcel",
    # Gold
    "PlanningZone",
    "DeltaResult",
    "InvestmentLead",
]
