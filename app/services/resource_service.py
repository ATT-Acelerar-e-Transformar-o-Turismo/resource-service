from typing import List, Optional
from datetime import datetime
import json
from bson.objectid import ObjectId
from bson.errors import InvalidId
from pymongo.errors import DuplicateKeyError, OperationFailure
from dependencies.database import db
from dependencies.rabbitmq import rabbitmq_client
from config import settings
from utils.mongo_utils import serialize, deserialize
from schemas.resource import ResourceCreate, ResourceDelete, ResourceUpdate
from schemas.wrapper import WrapperStatus
from services.wrapper_process_manager import wrapper_process_manager
import logging

logger = logging.getLogger(__name__)


async def get_all_resources(skip: int = 0, limit: int = 10) -> List[dict]:
    try:
        resources = (
            await db.resources.find({"deleted": False})
            .skip(skip)
            .limit(limit)
            .to_list(limit)
        )
        return [serialize(resource) for resource in resources]
    except OperationFailure as e:
        logger.error(f"Database operation failed in get_all_resources: {e}")
        raise


async def get_resource_by_id(resource_id: str) -> Optional[dict]:
    try:
        resource = await db.resources.find_one(
            {"_id": ObjectId(resource_id), "deleted": False}
        )
        return serialize(resource) if resource else None
    except InvalidId as e:
        logger.error(f"Invalid resource ID format: {resource_id}")
        raise ValueError(f"Invalid resource ID: {resource_id}")
    except OperationFailure as e:
        logger.error(f"Database operation failed in get_resource_by_id: {e}")
        raise


async def create_resource(resource_data: ResourceCreate) -> Optional[dict]:
    try:
        wrapper_id = resource_data.wrapper_id

        wrapper = await db.generated_wrappers.find_one({"wrapper_id": wrapper_id})
        if not wrapper:
            logger.error(
                f"Resource creation failed - wrapper_id '{wrapper_id}' does not exist"
            )
            raise ValueError(f"Wrapper with ID '{wrapper_id}' does not exist")

        resource_dict = deserialize(resource_data.dict())
        resource_dict["deleted"] = False
        resource_dict["startPeriod"] = None
        resource_dict["endPeriod"] = None
        result = await db.resources.insert_one(resource_dict)
        if result.inserted_id:
            return await get_resource_by_id(str(result.inserted_id))
        return None
    except DuplicateKeyError as e:
        logger.error(f"Resource creation failed - duplicate key: {e}")
        raise ValueError("Resource with this identifier already exists")
    except OperationFailure as e:
        logger.error(f"Database operation failed in create_resource: {e}")
        raise


async def update_resource(
    resource_id: str, resource_data: ResourceUpdate
) -> Optional[dict]:
    try:
        result = await db.resources.update_one(
            {"_id": ObjectId(resource_id), "deleted": False},
            {"$set": deserialize(resource_data)},
        )
        if result.modified_count > 0:
            return await get_resource_by_id(resource_id)
        return None
    except InvalidId as e:
        logger.error(f"Invalid resource ID format: {resource_id}")
        raise ValueError(f"Invalid resource ID: {resource_id}")
    except OperationFailure as e:
        logger.error(f"Database operation failed in update_resource: {e}")
        raise


async def delete_resource(resource_id: str) -> Optional[ResourceDelete]:
    try:
        object_id = ObjectId(resource_id)
        resource = await db.resources.find_one({"_id": object_id})
        if not resource:
            return None

        result = await db.resources.update_one(
            {"_id": object_id},
            {"$set": {"deleted": True}},
        )
        if result.modified_count > 0:
            wrapper_id = resource.get("wrapper_id")
            if wrapper_id:
                stopped = await wrapper_process_manager.stop_wrapper_process(wrapper_id)
                log_entry = f"Resource {resource_id} deleted at {datetime.utcnow()}"
                if stopped:
                    log_entry += " and wrapper process stopped"

                await db.generated_wrappers.update_one(
                    {"wrapper_id": wrapper_id},
                    {
                        "$set": {
                            "status": WrapperStatus.STOPPED.value,
                            "updated_at": datetime.utcnow(),
                        },
                        "$push": {"execution_log": log_entry},
                    },
                )

            await rabbitmq_client.publish(
                settings.RESOURCE_DELETED_QUEUE,
                json.dumps({"resource_id": resource_id, "wrapper_id": wrapper_id}),
            )

            return ResourceDelete(id=resource_id, deleted=True)
        return None
    except InvalidId as e:
        logger.error(f"Invalid resource ID format: {resource_id}")
        raise ValueError(f"Invalid resource ID: {resource_id}")
    except OperationFailure as e:
        logger.error(f"Database operation failed in delete_resource: {e}")
        raise
