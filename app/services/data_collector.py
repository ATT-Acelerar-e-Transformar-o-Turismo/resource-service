import json
import aio_pika
import logging
from datetime import datetime
from dependencies.rabbitmq import consumer, rabbitmq_client
from services.data_service import create_data_segment
from schemas.data_segment import TimePoint
from config import settings
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

            # Step 1: Update wrapper checkpoint (always, regardless of resource)
            timestamps = []
            for p in points_data:
                try:
                    timestamps.append(datetime.fromisoformat(p["x"]))
                except (ValueError, KeyError):
                    continue

            set_fields = {"last_data_sent": datetime.utcnow()}
            phase = data.get("phase")
            if phase:
                set_fields["phase"] = phase

            update_ops = {
                "$set": set_fields,
                "$inc": {"data_points_count": len(points_data)},
            }
            if timestamps:
                update_ops["$max"] = {"high_water_mark": max(timestamps)}
                update_ops["$min"] = {"low_water_mark": min(timestamps)}

            await db.generated_wrappers.update_one(
                {"wrapper_id": wrapper_id},
                update_ops,
            )

            # Step 2: Ingest data into resource (only if resource exists)
            resource = await db.resources.find_one(
                {"wrapper_id": wrapper_id, "deleted": False}
            )

            if not resource:
                logger.debug(
                    f"No resource linked to wrapper {wrapper_id}, skipping data ingestion"
                )
                return

            points = [TimePoint(**p) for p in points_data]
            if not points:
                return

            data_segment = await create_data_segment(resource["_id"], points)
            if not data_segment:
                logger.warning(f"Data segment creation failed for wrapper {wrapper_id}")
                return

            await rabbitmq_client.publish(
                settings.RESOURCE_DATA_QUEUE,
                json.dumps({"resource_id": str(resource["_id"]), "data": points_data}),
            )

        except json.JSONDecodeError:
            logger.error("Invalid JSON format, discarding message.")
        except (ValueError, TypeError) as e:
            logger.error(f"Data validation error: {e}")
            await message.nack(requeue=False)
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"Connection error processing message: {e}", exc_info=True)
            await message.nack(requeue=True)
