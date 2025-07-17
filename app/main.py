from fastapi import FastAPI
from routes.resource_routes import router as resource_router
from routes.health import router as health_router
from dependencies.rabbitmq import RabbitMQConnection
from services.data_ingestor import process_message
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.include_router(resource_router, prefix="/resources", tags=["Resources"])
app.include_router(health_router, tags=["Health"])

# Store the consumer task and connection
consumer_task = None
rabbitmq_connection = None

# RabbitMQ Configuration
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
QUEUE_NAME = "resource_data"


@app.on_event("startup")
async def startup_event():
    global consumer_task, rabbitmq_connection
    rabbitmq_connection = RabbitMQConnection(RABBITMQ_URL, QUEUE_NAME)
    await rabbitmq_connection.connect()
    queue = await rabbitmq_connection.get_queue()
    consumer_task = asyncio.create_task(queue.consume(process_message))


@app.on_event("shutdown")
async def shutdown_event():
    consumer_task.cancel()
    await rabbitmq_connection.close()


@app.get("/")
def read_root():
    return {"message": "Hello from resource service!"}
