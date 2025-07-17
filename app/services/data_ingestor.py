import json
import aio_pika
import logging
from dependencies.rabbitmq import RabbitMQConnection
from services.resource_service import get_resource_by_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def process_message(message: aio_pika.IncomingMessage):
    """Process incoming messages from RabbitMQ"""
    try:
        # Parse message
        data = json.loads(message.body.decode())
        logger.info(f"Processing message: {data}")

        resource_id = data.get('resource')
        points = data.get('data', [])

        if not resource_id or not points:
            logger.warning("Invalid message format - discarding message")
            await message.ack()  # Acknowledge and discard
            return

        # Find resource by ID
        resource = await get_resource_by_id(resource_id)
        if not resource:
            logger.warning(f"Resource {resource_id} not found - discarding message")
            await message.ack()  # Acknowledge and discard
            return

        # TODO: Process the data points and update the resource as needed
        logger.info(f"Processed data for resource {resource_id}")

        await message.ack()  # Acknowledge success
    except Exception as e:
        logger.error(f"Failed to process message: {e}")
        await message.nack()  # Negative acknowledge to retry 