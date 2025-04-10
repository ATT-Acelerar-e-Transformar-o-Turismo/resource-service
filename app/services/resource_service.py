from typing import List, Optional
from bson.objectid import ObjectId
from dependencies.database import db
from utils.mongo_utils import serialize, deserialize
from schemas.resource import ResourceCreate, ResourceDelete


async def get_all_resources(skip: int = 0, limit: int = 10) -> List[dict]:
    resources = await db.resources.find({"deleted": False}).skip(skip).limit(limit).to_list(limit)
    return [serialize(resource) for resource in resources]


async def get_resource_by_id(resource_id: str) -> Optional[dict]:
    resource = await db.resources.find_one({"_id": ObjectId(resource_id), "deleted": False})
    return serialize(resource) if resource else None


async def create_resource(resource_data: ResourceCreate) -> Optional[dict]:
    resource_dict = deserialize(resource_data.dict())
    resource_dict["deleted"] = False
    result = await db.resources.insert_one(resource_dict)
    if result.inserted_id:
        return await get_resource_by_id(str(result.inserted_id))
    return None


async def update_resource(resource_id: str, resource_data: dict) -> Optional[dict]:
    result = await db.resources.update_one(
        {"_id": ObjectId(resource_id), "deleted": False},
        {"$set": deserialize(resource_data)}
    )
    if result.modified_count > 0:
        return await get_resource_by_id(resource_id)
    return None


async def delete_resource(resource_id: str) -> Optional[ResourceDelete]:
    result = await db.resources.update_one(
        {"_id": ObjectId(resource_id)},
        {"$set": {"deleted": True}}
    )
    if result.modified_count > 0:
        return ResourceDelete(id=resource_id, deleted=True)
    return None
