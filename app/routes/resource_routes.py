from fastapi import APIRouter, HTTPException, Query
from typing import List
from bson.errors import InvalidId
import logging
from schemas.common import PyObjectId
from services.resource_service import (
    get_all_resources,
    get_resource_by_id,
    create_resource,
    update_resource,
    delete_resource,
)
from schemas.resource import (
    Resource,
    ResourceCreate,
    ResourceUpdate,
    ResourcePatch,
    ResourceDelete,
)

router = APIRouter()
logger = logging.getLogger(__name__)

RESOURCE_NOT_FOUND = "Resource not found"
INVALID_RESOURCE_ID = "Invalid resource ID"


@router.get("/", response_model=List[Resource])
async def get_resources(
    skip: int = Query(0, ge=0), limit: int = Query(10, ge=1, le=50)
):
    return await get_all_resources(skip=skip, limit=limit)


@router.get("/{resource_id}", response_model=Resource)
async def get_resource(resource_id: str):
    try:
        PyObjectId(resource_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_RESOURCE_ID)
    resource = await get_resource_by_id(resource_id)
    if not resource:
        raise HTTPException(status_code=404, detail=RESOURCE_NOT_FOUND)
    return resource


@router.post("/", response_model=Resource)
async def create_resource_route(resource: ResourceCreate):
    created_resource = await create_resource(resource)
    if not created_resource:
        raise HTTPException(status_code=400, detail="Failed to create resource")
    return created_resource


@router.put("/{resource_id}", response_model=Resource)
async def update_resource_route(resource_id: str, resource: ResourceUpdate):
    try:
        PyObjectId(resource_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_RESOURCE_ID)
    updated_resource = await update_resource(resource_id, resource)
    if not updated_resource:
        raise HTTPException(status_code=404, detail=RESOURCE_NOT_FOUND)
    return updated_resource


@router.patch("/{resource_id}", response_model=Resource)
async def patch_resource_route(resource_id: str, resource: ResourcePatch):
    try:
        PyObjectId(resource_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_RESOURCE_ID)
    updated_resource = await update_resource(
        resource_id, resource.dict(exclude_unset=True)
    )
    if not updated_resource:
        raise HTTPException(status_code=404, detail=RESOURCE_NOT_FOUND)
    return updated_resource


@router.delete("/{resource_id}", response_model=ResourceDelete)
async def delete_resource_route(resource_id: str):
    # Check for common invalid values
    if not resource_id or resource_id in ["undefined", "null", "None"]:
        logger.error(f"Invalid or missing resource ID in delete request: '{resource_id}'")
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid resource ID: '{resource_id}'. Please provide a valid resource ID."
        )
    
    try:
        PyObjectId(resource_id)
    except (InvalidId, ValueError) as e:
        logger.error(f"Invalid resource ID format in delete request: {resource_id}, error: {e}")
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid resource ID format: '{resource_id}'. Expected a valid MongoDB ObjectId (24 character hex string)."
        )
    
    try:
        deleted_resource = await delete_resource(resource_id)
        if not deleted_resource:
            raise HTTPException(status_code=404, detail=RESOURCE_NOT_FOUND)
        return deleted_resource
    except ValueError as e:
        # This handles the InvalidId error from the service layer
        logger.error(f"Service layer validation failed for resource ID: {resource_id}")
        raise HTTPException(status_code=400, detail=str(e))
