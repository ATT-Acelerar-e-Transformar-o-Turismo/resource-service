from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List, Dict, Any
from schemas.common import PyObjectId
from schemas.data_segment import DataPoint


class FileMetadata(BaseModel):
    original_filename: Optional[str] = None
    file_count: Optional[int] = None
    rows_count: Optional[int] = None
    columns_count: Optional[int] = None


class ResourceBase(BaseModel):
    wrapper_id: str
    name: str
    type: str


class ResourceCreate(ResourceBase):
    start_period: Optional[str] = None  # Frontend sends string dates
    end_period: Optional[str] = None    # Frontend sends string dates
    indicator: Optional[str] = None
    data: Optional[List[List[Any]]] = None
    headers: Optional[List[str]] = None
    file_metadata: Optional[FileMetadata] = None


class ResourceUpdate(ResourceBase):
    start_period: Optional[str] = None  # Frontend sends string dates
    end_period: Optional[str] = None    # Frontend sends string dates
    indicator: Optional[str] = None
    data: Optional[List[List[Any]]] = None
    headers: Optional[List[str]] = None
    file_metadata: Optional[FileMetadata] = None


class ResourcePatch(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None


class Resource(ResourceBase):
    id: PyObjectId
    startPeriod: Optional[datetime] = None
    endPeriod: Optional[datetime] = None
    indicator: Optional[str] = None
    data: Optional[List[List[Any]]] = None
    headers: Optional[List[str]] = None
    file_metadata: Optional[FileMetadata] = None

    class Config:
        from_attributes = True


class ResourceDelete(BaseModel):
    id: str
    deleted: bool

