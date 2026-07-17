"""
kafka_client.py — Kafka producer and consumer management for async replication.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from app.config import KAFKA_BROKER, KAFKA_TOPIC, REGION

logger = logging.getLogger(__name__)

# ── Shared state ──────────────────────────────────────────────────────────────
_producer: Optional[AIOKafkaProducer] = None
_last_consumed_at: Optional[datetime] = None  # tracks replication lag


# ── Producer ──────────────────────────────────────────────────────────────────

async def get_producer() -> AIOKafkaProducer:
    """Return (or lazily create) the Kafka producer."""
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="all",
            enable_idempotence=True,
        )
        await _producer.start()
        logger.info("Kafka producer started.")
    return _producer


async def stop_producer() -> None:
    """Stop the Kafka producer on shutdown."""
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None
        logger.info("Kafka producer stopped.")


async def publish_property_update(record: dict) -> None:
    """
    Publish a property update event to the property-updates Kafka topic.

    Args:
        record: dict containing full property state after update.
    """
    producer = await get_producer()
    message = {
        "id": record["id"],
        "price": float(record["price"]),
        "bedrooms": record["bedrooms"],
        "bathrooms": record["bathrooms"],
        "region_origin": record["region_origin"],
        "version": record["version"],
        "updated_at": record["updated_at"].isoformat() if isinstance(record["updated_at"], datetime) else record["updated_at"],
    }
    await producer.send_and_wait(KAFKA_TOPIC, value=message)
    logger.info(f"Published update for property {record['id']} to Kafka topic '{KAFKA_TOPIC}'.")


# ── Consumer ──────────────────────────────────────────────────────────────────

def get_last_consumed_at() -> Optional[datetime]:
    """Return the timestamp of the last consumed message for replication lag."""
    return _last_consumed_at


async def start_consumer(pool) -> None:
    """
    Start the Kafka consumer loop.

    Consumes messages from the property-updates topic.
    Only processes messages originating from OTHER regions (not our own).

    Args:
        pool: asyncpg connection pool for database writes.
    """
    global _last_consumed_at

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=f"property-replicator-{REGION}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    # Retry loop to wait for Kafka readiness
    for attempt in range(10):
        try:
            await consumer.start()
            logger.info(f"Kafka consumer started for region '{REGION}' (group: property-replicator-{REGION}).")
            break
        except KafkaConnectionError as e:
            logger.warning(f"Kafka connection attempt {attempt + 1}/10 failed: {e}. Retrying in 5s...")
            await asyncio.sleep(5)
    else:
        logger.error("Failed to connect to Kafka after 10 attempts. Consumer not started.")
        return

    try:
        async for msg in consumer:
            try:
                data = msg.value
                origin_region = data.get("region_origin", "").lower()

                # ── Idempotency: skip messages from our own region ──
                if origin_region == REGION:
                    logger.debug(f"Skipping own-region message for property {data.get('id')}.")
                    continue

                logger.info(
                    f"Consuming cross-region update from '{origin_region}' for property {data.get('id')}."
                )

                # ── Apply replication to local database ──
                async with pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO properties (id, price, bedrooms, bathrooms, region_origin, version, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (id) DO UPDATE
                            SET price         = EXCLUDED.price,
                                bedrooms      = EXCLUDED.bedrooms,
                                bathrooms     = EXCLUDED.bathrooms,
                                region_origin = EXCLUDED.region_origin,
                                version       = EXCLUDED.version,
                                updated_at    = EXCLUDED.updated_at
                        WHERE properties.version < EXCLUDED.version
                        """,
                        data["id"],
                        data["price"],
                        data["bedrooms"],
                        data["bathrooms"],
                        data["region_origin"],
                        data["version"],
                        datetime.fromisoformat(data["updated_at"].replace("Z", "+00:00")),
                    )

                # Track last consumed time for replication lag
                _last_consumed_at = datetime.now(timezone.utc)
                logger.info(f"Replicated property {data['id']} from '{origin_region}' into '{REGION}' DB.")

            except Exception as e:
                logger.error(f"Error processing Kafka message: {e}", exc_info=True)

    finally:
        await consumer.stop()
        logger.info("Kafka consumer stopped.")
