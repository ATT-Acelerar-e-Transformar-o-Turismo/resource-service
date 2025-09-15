from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import logging
import time
from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONNECTION_STRING = settings.MONGO_URI

if CONNECTION_STRING:
    while True:
        try:
            client = AsyncIOMotorClient(CONNECTION_STRING)
            # Test the connection
            client.admin.command("ping")
            db = client.get_default_database()
            logger.info("Successfully connected to MongoDB")
            break
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            logger.info("Retrying in 5 seconds...")
            time.sleep(5)
else:
    logger.error("Missing MONGO_URI in settings")
