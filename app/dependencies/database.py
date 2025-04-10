from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
import time
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONNECTION_STRING = os.getenv('MONGO_URI')

if CONNECTION_STRING is not None:
    while True:
        try:
            client = AsyncIOMotorClient(CONNECTION_STRING)
            # Test the connection
            client.admin.command('ping')
            db = client.get_default_database()
            logger.info("Successfully connected to MongoDB")
            break
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            logger.info("Retrying in 5 seconds...")
            time.sleep(5)
else:
    logger.error("Missing MONGO_URI environment variable")
