import json
import aio_pika
import logging
from dependencies.rabbitmq import consumer, rabbitmq_client
from services.data_service import create_data_segment, get_data_by_resource_id
from schemas.data_segment import TimePoint
from config import settings
from exceptions import ResourceNotFoundException
from dependencies.database import db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@consumer(settings.COLLECTED_DATA_QUEUE)
async def process_collected_data(message: aio_pika.abc.AbstractIncomingMessage):
    async with message.process():
        try:
            data = json.loads(message.body.decode())
            logger.debug(f"Consumed message: {data}")
            wrapper_id = data.get("wrapper_id")
            points_data = data.get("data")

            if not wrapper_id or not points_data:
                logger.warning("Invalid message format, discarding message.")
                return

            # Find resource by wrapper_id
            resource = await db.resources.find_one(
                    {"wrapper_id": wrapper_id, "deleted": False}
                    )

            if not resource:
                raise ResourceNotFoundException(
                        f"Resource with wrapper_id {wrapper_id} not found"
                        )

            points = [TimePoint(**p) for p in points_data]

            if not points:
                logger.warning("Empty data in message, discarding.")
                return

            data_segment = await create_data_segment(resource["_id"], points)
            if not data_segment:
                logger.warning(f"Data segment creation failed, discarding message: {data}")
                return
            logger.debug(f"Data segment created successfully: {data_segment}")

            await rabbitmq_client.publish(
                    settings.RESOURCE_DATA_QUEUE, json.dumps({
                        "resource_id": str(resource["_id"]),
                        "data": points_data
                        })
                    )

        except json.JSONDecodeError:
            logger.error("Invalid JSON format, discarding message.")
        except ResourceNotFoundException as e:
            logger.warning(e)
        except (ValueError, TypeError) as e:
            logger.error(f"Data validation error: {e}")
            await message.nack(requeue=False)
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"Connection error processing message: {e}", exc_info=True)
            await message.nack(requeue=True)
