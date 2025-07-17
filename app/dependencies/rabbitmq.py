import aio_pika
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class RabbitMQConnection:
    def __init__(self, url: str, queue_name: str):
        self.url = url
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.queue_name = queue_name

    async def connect(self):
        """Establish connection with RabbitMQ"""
        try:
            self.connection = await aio_pika.connect_robust(self.url)
            self.channel = await self.connection.channel()
            await self.channel.declare_queue(self.queue_name, durable=True)
            logger.info("Successfully connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    async def close(self):
        """Close connection"""
        if self.connection:
            await self.connection.close()

    async def get_queue(self):
        """Get or create queue"""
        if not self.connection:
            await self.connect()
        return await self.channel.declare_queue(self.queue_name, durable=True) 