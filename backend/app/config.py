"""
config.py — Environment-based configuration for the backend service.
"""
import os
from dotenv import load_dotenv

load_dotenv()

REGION: str = os.environ.get("REGION", "us").lower()
DATABASE_URL: str = os.environ.get("DATABASE_URL", "")
KAFKA_BROKER: str = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC: str = "property-updates"
PORT: int = int(os.environ.get("PORT", 8000))
