from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List
from schemas.common import PyObjectId
from schemas.data_segment import DataPoint


class ResourceBase(BaseModel):
    wrapper_id: str
    name: str
    type: str


class ResourceCreate(ResourceBase):
    pass


class ResourceUpdate(ResourceBase):
    pass


class ResourcePatch(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None


class Resource(ResourceBase):
    id: PyObjectId
    startPeriod: Optional[datetime] = None
    endPeriod: Optional[datetime] = None

    class Config:
        from_attributes = True


class ResourceDelete(BaseModel):
    id: str
    deleted: bool


class ResourceData(BaseModel):
    resource_id: PyObjectId
    data: List[DataPoint]
