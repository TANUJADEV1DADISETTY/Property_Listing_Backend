"""
models.py — Pydantic request/response schemas.
"""
from datetime import datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field


class PropertyUpdateRequest(BaseModel):
    """Request body for PUT /:region/properties/:id"""
    price: Decimal = Field(..., gt=0, description="New property price")
    version: int = Field(..., ge=1, description="Current version for optimistic locking")


class PropertyResponse(BaseModel):
    """Response body for a successful property update."""
    id: int
    price: Decimal
    bedrooms: int
    bathrooms: int
    region_origin: str
    version: int
    updated_at: datetime

    class Config:
        from_attributes = True


class ReplicationLagResponse(BaseModel):
    """Response for GET /:region/replication-lag"""
    lag_seconds: float


class HealthResponse(BaseModel):
    """Response for GET /:region/health"""
    status: str
    region: str
    database: str


class ErrorResponse(BaseModel):
    """Standard error response."""
    detail: str
    code: Optional[str] = None


class DuplicateRequestResponse(BaseModel):
    """Response for duplicate X-Request-ID (422)."""
    detail: str
    request_id: str
