from pydantic import BaseModel
from datetime import datetime
from typing import Optional
from schemas.common import PyObjectId


class ResourceBase(BaseModel):
    name: str
    startPeriod: datetime
    endPeriod: datetime


class ResourceCreate(ResourceBase):
    pass


class ResourceUpdate(ResourceBase):
    pass


class ResourcePatch(BaseModel):
    name: Optional[str] = None
    startPeriod: Optional[datetime] = None
    endPeriod: Optional[datetime] = None


class Resource(ResourceBase):
    id: PyObjectId

    class Config:
        orm_mode = True


class ResourceDelete(BaseModel):
    id: str
    deleted: bool
