from fastapi import APIRouter, HTTPException, Query
from typing import List
from bson.errors import InvalidId
from bson.objectid import ObjectId
from datetime import datetime
import logging
from schemas.common import PyObjectId
from services.resource_service import (
    get_all_resources,
    get_resource_by_id,
    create_resource,
    update_resource,
    delete_resource,
)
from services.data_service import get_data_by_resource_id
from schemas.resource import (
    Resource,
    ResourceCreate,
    ResourceUpdate,
    ResourcePatch,
    ResourceDelete,
)
from schemas.data_segment import DataPoint

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


@router.get("/{resource_id}/data", response_model=List[DataPoint])
async def get_resource_data(
    resource_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=10000),
    sort: str = Query("asc", regex="^(asc|desc)$"),
    start_date: datetime = Query(None, description="Start date for filtering"),
    end_date: datetime = Query(None, description="End date for filtering"),
):
    """Get data points for a specific resource with optional filtering and pagination"""
    try:
        resource_obj_id = ObjectId(resource_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_RESOURCE_ID)
    
    # Check if resource exists
    resource = await get_resource_by_id(resource_id)
    if not resource:
        raise HTTPException(status_code=404, detail=RESOURCE_NOT_FOUND)
    
    # Get data from the data service
    all_data = []
    async for chunk in get_data_by_resource_id(
        resource_obj_id,
        min_time=start_date,
        max_time=end_date,
        sorted=True
    ):
        all_data.extend(chunk.get("data", []))
    
    # Apply pagination
    if sort == "desc":
        all_data.reverse()
    
    paginated_data = all_data[skip:skip + limit] if limit else all_data[skip:]
    
    return paginated_data
