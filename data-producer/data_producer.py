import pika
import json
import time
from datetime import datetime, timezone, timedelta
import random
import os
import requests
import signal
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

API_URL = os.getenv("API_URL", "http://localhost:8080/resources")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")


class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.kill_now = True


def main():
    killer = GracefulKiller()
    connection = None
    channel = None

    while True:
        try:
            connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            channel = connection.channel()
            channel.queue_declare(queue="collected_data", durable=True)
            logger.info("Connected to RabbitMQ and declared queue.")
            break
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(
                f"Connection to RabbitMQ failed: {e}. Retrying in 5 seconds..."
            )
            time.sleep(5)

    while not killer.kill_now:
        try:
            response = requests.get(API_URL)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            resources = response.json()
            logger.info(f"Successfully fetched {len(resources)} resources from API.")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch resources from API: {e}")
            time.sleep(5)
            continue

        if not resources:
            logger.warning(
                "No resources found from API. Waiting for 10 seconds before retrying."
            )
            time.sleep(10)
            continue

        for resource in resources:
            if killer.kill_now:
                break
            wrapper_id = resource.get("wrapper_id")
            if wrapper_id:
                message = {
                    "wrapper_id": wrapper_id,
                    "data": [
                        {
                            "x": (
                                datetime.now(timezone.utc) - timedelta(seconds=i)
                            ).isoformat(),
                            "y": random.uniform(1, 100),
                        } for i in range(5)
                    ],
                }

                try:
                    channel.basic_publish(
                        exchange="",
                        routing_key="collected_data",
                        body=json.dumps(message, default=str),
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # make message persistent
                        ),
                    )
                    logger.info(f"Sent message for wrapper_id: {wrapper_id}")
                except pika.exceptions.AMQPConnectionError as e:
                    logger.error(
                        f"Failed to publish message to RabbitMQ: {e}. Attempting to reconnect..."
                    )
                    # Attempt to reconnect
                    while True:
                        try:
                            connection = pika.BlockingConnection(
                                pika.URLParameters(RABBITMQ_URL)
                            )
                            channel = connection.channel()
                            channel.queue_declare(queue="collected_data", durable=True)
                            logger.info("Reconnected to RabbitMQ.")
                            break
                        except pika.exceptions.AMQPConnectionError as reconnect_e:
                            logger.error(
                                f"Reconnection to RabbitMQ failed: {reconnect_e}. Retrying in 5 seconds..."
                            )
                            time.sleep(5)
                    # After successful reconnection, try publishing again in the next loop iteration
                    continue
                except Exception as e:
                    logger.error(f"An unexpected error occurred while publishing: {e}")
            else:
                logger.warning(f"Resource missing wrapper_id: {resource}")
        time.sleep(10)

    logger.info("Shutting down gracefully...")
    if connection:
        connection.close()
        logger.info("RabbitMQ connection closed.")


if __name__ == "__main__":
    main()
