from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import router as api_router
from dependencies.rabbitmq import rabbitmq_client
import logging
from contextlib import asynccontextmanager
from config import settings
from services import data_collector
from services.wrapper_service import wrapper_service
from services.wrapper_process_manager import wrapper_process_manager

logger = logging.getLogger(__name__)

app = FastAPI()

# CORS Configuration
origins = settings.ORIGINS.split(",")
logger.info(f"Origins: {origins}")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connect to RabbitMQ
    await rabbitmq_client.connect()
    logger.info("Connected to RabbitMQ")

    # Start RabbitMQ consumers
    await rabbitmq_client.start_consumers()
    logger.info("Started RabbitMQ consumers")

    # Start wrapper process monitoring
    await wrapper_process_manager.start_monitoring()
    logger.info("Started wrapper process monitoring")

    # Restart continuous wrappers that were executing before service restart
    await wrapper_service.restart_executing_wrappers()
    logger.info("Restart of executing wrappers completed")

    try:
        yield
    finally:
        # Graceful shutdown
        try:
            # Stop wrapper process monitoring
            await wrapper_process_manager.stop_monitoring()
            logger.info("Stopped wrapper process monitoring")
        except Exception as e:
            logger.error(f"Error stopping wrapper monitoring: {e}")

        # Close RabbitMQ connection
        if rabbitmq_client:
            await rabbitmq_client.close()
            logger.info("Closed RabbitMQ connection")


app.router.lifespan_context = lifespan
