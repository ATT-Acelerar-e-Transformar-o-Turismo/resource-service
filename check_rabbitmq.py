#!/usr/bin/env python3
import asyncio
import aio_pika
import json

async def main():
    # Connect to RabbitMQ
    print("Connecting to RabbitMQ...")
    connection = await aio_pika.connect_robust("amqp://guest:guest@indicators-rabbitmq/")
    
    async with connection:
        # Create channel
        channel = await connection.channel()
        
        # Declare queue
        queue = await channel.declare_queue(
            "resource_data",
            durable=True
        )
        
        print(f"Waiting for messages in queue '{queue.name}'...")
        
        # Callback when message is received
        async def process_message(message):
            async with message.process():
                try:
                    # Decode the message body
                    body = message.body.decode()
                    print(f"Received raw message: {body}")
                    
                    # Parse as JSON
                    data = json.loads(body)
                    print(f"Parsed message: {data}")
                    
                    # Process message (in this case, just print it)
                    print(f"Resource ID: {data.get('resource')}")
                    print(f"Data points: {len(data.get('data', []))}")
                    
                except Exception as e:
                    print(f"Error processing message: {e}")
        
        # Start consuming messages
        await queue.consume(process_message)
        
        # Keep the consumer running
        try:
            # Wait indefinitely 
            await asyncio.Future()
        except asyncio.CancelledError:
            print("Consumer was cancelled")

if __name__ == "__main__":
    asyncio.run(main()) 