from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List
from auth import require_admin, require_auth
from bson.errors import InvalidId
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

router = APIRouter()


@router.get("/version")
def get_version():
    return {"service": "resource-service", "version": "1.0.0"}


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
async def create_resource_route(resource: ResourceCreate, _=Depends(require_admin)):
    created_resource = await create_resource(resource)
    if not created_resource:
        raise HTTPException(status_code=400, detail="Failed to create resource")
    return created_resource


@router.put("/{resource_id}", response_model=Resource)
async def update_resource_route(resource_id: str, resource: ResourceUpdate, _=Depends(require_admin)):
    try:
        PyObjectId(resource_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_RESOURCE_ID)
    updated_resource = await update_resource(resource_id, resource)
    if not updated_resource:
        raise HTTPException(status_code=404, detail=RESOURCE_NOT_FOUND)
    return updated_resource


@router.patch("/{resource_id}", response_model=Resource)
async def patch_resource_route(resource_id: str, resource: ResourcePatch, _=Depends(require_admin)):
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


@router.get("/{resource_id}/data")
async def get_resource_data(resource_id: str, limit: int = Query(500, ge=1, le=5000), _=Depends(require_auth)):
    try:
        oid = PyObjectId(resource_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_RESOURCE_ID)
    resource = await get_resource_by_id(resource_id)
    if not resource:
        raise HTTPException(status_code=404, detail=RESOURCE_NOT_FOUND)
    points = []
    async for chunk in get_data_by_resource_id(oid, sorted=True):
        points.extend(chunk["data"])
        if len(points) >= limit:
            break
    return {"resource_id": resource_id, "data": points[:limit]}


@router.delete("/{resource_id}", response_model=ResourceDelete)
async def delete_resource_route(resource_id: str, _=Depends(require_admin)):
    try:
        PyObjectId(resource_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_RESOURCE_ID)
    deleted_resource = await delete_resource(resource_id)
    if not deleted_resource:
        raise HTTPException(status_code=404, detail=RESOURCE_NOT_FOUND)
    return deleted_resource
