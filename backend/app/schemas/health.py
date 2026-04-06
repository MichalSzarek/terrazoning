from datetime import datetime

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = Field(..., examples=["ok"])
    database: str = Field(..., examples=["connected"])
    version: str = Field(..., examples=["0.1.0"])
    timestamp: datetime = Field(..., examples=["2026-04-04T10:00:00Z"])


class HealthErrorResponse(BaseModel):
    status: str = Field(default="error")
    database: str = Field(..., examples=["connection refused"])
    detail: str
