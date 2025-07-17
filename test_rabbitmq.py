#!/usr/bin/env python3
import asyncio
import aio_pika
import json
import datetime
import random

async def main():
    # Connect to RabbitMQ
    connection = await aio_pika.connect_robust("amqp://guest:guest@indicators-rabbitmq/")
    
    async with connection:
        # Create channel
        channel = await connection.channel()
        
        # Declare queue
        queue = await channel.declare_queue(
            "resource_data",
            durable=True
        )
        
        # Prepare test data
        test_data = {
            "resource": "test-resource-123",
            "data": [
                {
                    "x": datetime.datetime.now().isoformat(),
                    "y": random.uniform(0, 100)
                },
                {
                    "x": (datetime.datetime.now() + datetime.timedelta(hours=1)).isoformat(),
                    "y": random.uniform(0, 100)
                }
            ]
        }
        
        # Convert to JSON string and encode
        message_body = json.dumps(test_data).encode()
        
        # Create and send message
        message = aio_pika.Message(
            body=message_body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
        
        # Publish message
        await channel.default_exchange.publish(
            message,
            routing_key=queue.name
        )
        
        print(f"Message sent: {test_data}")

if __name__ == "__main__":
    asyncio.run(main()) 