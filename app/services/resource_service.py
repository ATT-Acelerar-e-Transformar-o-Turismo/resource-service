from typing import List, Optional
from bson.objectid import ObjectId
from bson.errors import InvalidId
from pymongo.errors import DuplicateKeyError, OperationFailure
from dependencies.database import db
from utils.mongo_utils import serialize, deserialize
from schemas.resource import ResourceCreate, ResourceDelete, ResourceUpdate
import logging

logger = logging.getLogger(__name__)


async def get_all_resources(skip: int = 0, limit: int = 10) -> List[dict]:
    try:
        resources = await db.resources.find({"deleted": False}).skip(skip).limit(limit).to_list(limit)
        return [serialize(resource) for resource in resources]
    except OperationFailure as e:
        logger.error(f"Database operation failed in get_all_resources: {e}")
        raise


async def get_resource_by_id(resource_id: str) -> Optional[dict]:
    try:
        resource = await db.resources.find_one({"_id": ObjectId(resource_id), "deleted": False})
        return serialize(resource) if resource else None
    except InvalidId as e:
        logger.error(f"Invalid resource ID format: {resource_id}")
        raise ValueError(f"Invalid resource ID: {resource_id}")
    except OperationFailure as e:
        logger.error(f"Database operation failed in get_resource_by_id: {e}")
        raise


async def create_resource(resource_data: ResourceCreate) -> Optional[dict]:
    try:
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


async def update_resource(resource_id: str, resource_data: ResourceUpdate) -> Optional[dict]:
    try:
        result = await db.resources.update_one(
            {"_id": ObjectId(resource_id), "deleted": False},
            {"$set": deserialize(resource_data)}
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
        result = await db.resources.update_one(
            {"_id": ObjectId(resource_id)},
            {"$set": {"deleted": True}}
        )
        if result.modified_count > 0:
            return ResourceDelete(id=resource_id, deleted=True)
        return None
    except InvalidId as e:
        logger.error(f"Invalid resource ID format: {resource_id}")
        raise ValueError(f"Invalid resource ID: {resource_id}")
    except OperationFailure as e:
        logger.error(f"Database operation failed in delete_resource: {e}")
        raise
