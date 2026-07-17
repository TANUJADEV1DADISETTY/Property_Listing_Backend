"""
routes.py — API route handlers for the property listing backend.

Implements:
- GET  /:region/health          — Health check
- GET  /:region/properties      — List properties
- GET  /:region/properties/:id  — Get single property
- PUT  /:region/properties/:id  — Update with optimistic locking + idempotency
- GET  /:region/replication-lag — Replication lag in seconds
"""
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Header, Path, Request
from fastapi.responses import JSONResponse

from app.config import REGION
from app.database import get_pool
from app.kafka_client import publish_property_update, get_last_consumed_at
from app.models import (
    PropertyUpdateRequest,
    PropertyResponse,
    ReplicationLagResponse,
    HealthResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Utility ───────────────────────────────────────────────────────────────────

def _serialize(record) -> dict:
    """Convert asyncpg Record to a JSON-serializable dict."""
    d = dict(record)
    for k, v in d.items():
        if isinstance(v, Decimal):
            d[k] = float(v)
        elif isinstance(v, datetime):
            d[k] = v.isoformat()
    return d


# ── Health Check ──────────────────────────────────────────────────────────────

@router.get("/{region}/health", response_model=HealthResponse)
async def health_check(region: str = Path(...)):
    """
    Health check endpoint.
    Returns 200 OK if the service and database are reachable.
    This endpoint responds regardless of which region it's called with,
    enabling NGINX failover (EU serves /us/ and vice versa).
    """
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        db_status = "ok"
    except Exception as e:
        logger.error(f"DB health check failed: {e}")
        db_status = "error"

    return HealthResponse(
        status="ok",
        region=REGION,
        database=db_status,
    )


# ── List Properties ───────────────────────────────────────────────────────────

@router.get("/{region}/properties")
async def list_properties(region: str, limit: int = 20, offset: int = 0):
    """List properties with pagination."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM properties ORDER BY id LIMIT $1 OFFSET $2",
            limit, offset
        )
    return [_serialize(r) for r in rows]


# ── Get Single Property ───────────────────────────────────────────────────────

@router.get("/{region}/properties/{property_id}")
async def get_property(region: str, property_id: int):
    """Get a single property by ID."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM properties WHERE id = $1", property_id
        )
    if not row:
        raise HTTPException(status_code=404, detail=f"Property {property_id} not found.")
    return _serialize(row)


# ── Update Property (Optimistic Locking + Idempotency) ───────────────────────

@router.put("/{region}/properties/{property_id}")
async def update_property(
    region: str,
    property_id: int,
    body: PropertyUpdateRequest,
    x_request_id: str = Header(None, alias="x-request-id"),
):
    """
    Update a property with optimistic locking and idempotency.

    - Returns 200 OK on success with the updated property.
    - Returns 409 Conflict if the version doesn't match (optimistic lock failure).
    - Returns 422 Unprocessable Entity if the X-Request-ID has already been processed.
    - Returns 404 if property not found.
    """
    pool = await get_pool()

    async with pool.acquire() as conn:
        async with conn.transaction():

            # ── Step 1: Idempotency check ──────────────────────────────────
            if x_request_id:
                existing = await conn.fetchrow(
                    "SELECT response FROM idempotency_keys WHERE request_id = $1",
                    x_request_id,
                )
                if existing:
                    logger.info(f"Duplicate X-Request-ID detected: {x_request_id}")
                    return JSONResponse(
                        status_code=422,
                        content={
                            "detail": "Duplicate request: this X-Request-ID has already been processed.",
                            "request_id": x_request_id,
                        },
                    )

            # ── Step 2: Lock the row and check existence ───────────────────
            current = await conn.fetchrow(
                "SELECT * FROM properties WHERE id = $1 FOR UPDATE",
                property_id,
            )
            if not current:
                raise HTTPException(
                    status_code=404,
                    detail=f"Property {property_id} not found.",
                )

            # ── Step 3: Optimistic locking version check ───────────────────
            if current["version"] != body.version:
                logger.warning(
                    f"Version conflict for property {property_id}: "
                    f"expected {body.version}, found {current['version']}."
                )
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Version conflict: the record has been updated since you last read it. "
                        f"Current version is {current['version']}, but you submitted version {body.version}. "
                        f"Please re-fetch the latest data and retry."
                    ),
                )

            # ── Step 4: Apply the update ───────────────────────────────────
            updated = await conn.fetchrow(
                """
                UPDATE properties
                SET price      = $1,
                    version    = version + 1,
                    updated_at = NOW()
                WHERE id = $2
                RETURNING *
                """,
                body.price,
                property_id,
            )

            # ── Step 5: Store idempotency key ──────────────────────────────
            if x_request_id:
                response_data = _serialize(updated)
                await conn.execute(
                    """
                    INSERT INTO idempotency_keys (request_id, response)
                    VALUES ($1, $2)
                    ON CONFLICT (request_id) DO NOTHING
                    """,
                    x_request_id,
                    json.dumps(response_data),
                )

    # ── Step 6: Publish Kafka event ────────────────────────────────────────────
    try:
        await publish_property_update(dict(updated))
    except Exception as e:
        logger.error(f"Failed to publish Kafka event for property {property_id}: {e}")
        # Don't fail the request — the DB update succeeded

    return _serialize(updated)


# ── Replication Lag ───────────────────────────────────────────────────────────

@router.get("/{region}/replication-lag", response_model=ReplicationLagResponse)
async def replication_lag(region: str):
    """
    Report the replication lag in seconds.
    Calculates the time since the last Kafka message was consumed from another region.
    """
    last_consumed = get_last_consumed_at()
    if last_consumed is None:
        # No messages consumed yet — lag is effectively 0 or unknown
        return ReplicationLagResponse(lag_seconds=0.0)

    now = datetime.now(timezone.utc)
    lag = (now - last_consumed).total_seconds()
    return ReplicationLagResponse(lag_seconds=round(lag, 3))
