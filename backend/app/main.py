"""
main.py — FastAPI application entry point.

Handles startup/shutdown lifecycle for:
- Database connection pool
- Kafka producer
- Kafka consumer (runs as background task)
"""
import asyncio
import logging
import sys

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import REGION, DATABASE_URL, KAFKA_BROKER
from app.database import get_pool, close_pool
from app.kafka_client import get_producer, stop_producer, start_consumer
from app.routes import router

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── FastAPI app ────────────────────────────────────────────────────────────────
app = FastAPI(
    title=f"Property Listing Backend — {REGION.upper()} Region",
    description=(
        "Multi-region property listing backend with NGINX routing, "
        "Kafka async replication, optimistic locking, and idempotency."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

# ── Background consumer task reference ────────────────────────────────────────
_consumer_task: asyncio.Task | None = None


@app.on_event("startup")
async def startup_event():
    """Initialize connections on startup."""
    global _consumer_task

    logger.info(f"Starting backend service for region: {REGION.upper()}")
    logger.info(f"DATABASE_URL: {DATABASE_URL[:30]}...")
    logger.info(f"KAFKA_BROKER: {KAFKA_BROKER}")

    # Initialize DB pool (with retry for slow postgres startup)
    for attempt in range(15):
        try:
            pool = await get_pool()
            # Quick connectivity test
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            logger.info("Database connected successfully.")
            break
        except Exception as e:
            logger.warning(f"DB connection attempt {attempt + 1}/15 failed: {e}. Retrying in 5s...")
            await asyncio.sleep(5)
    else:
        logger.error("Could not connect to database after 15 attempts. Service may be unhealthy.")

    # Start Kafka producer (warm it up)
    try:
        await get_producer()
    except Exception as e:
        logger.warning(f"Kafka producer startup warning: {e}")

    # Start Kafka consumer in background
    try:
        pool = await get_pool()
        _consumer_task = asyncio.create_task(start_consumer(pool))
        logger.info("Kafka consumer background task started.")
    except Exception as e:
        logger.error(f"Failed to start Kafka consumer: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up connections on shutdown."""
    global _consumer_task

    logger.info("Shutting down backend service...")

    if _consumer_task is not None:
        _consumer_task.cancel()
        try:
            await _consumer_task
        except asyncio.CancelledError:
            pass

    await stop_producer()
    await close_pool()

    logger.info("Backend service shut down cleanly.")


@app.get("/")
async def root():
    """Root endpoint — service identification."""
    return {
        "service": "property-listing-backend",
        "region": REGION.upper(),
        "status": "running",
        "docs": "/docs",
    }
