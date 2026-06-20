from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List
from schemas.common import PyObjectId
from schemas.data_segment import DataPoint


class ResourceBase(BaseModel):
    wrapper_id: str
    name: str
    type: str
    # Optional admin-curated label shown in the chart legend for this
    # resource's data series. Falls back to the column/file name when unset.
    legend: Optional[str] = None


class ResourceCreate(ResourceBase):
    pass


class ResourceUpdate(ResourceBase):
    pass


class ResourcePatch(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    legend: Optional[str] = None


class Resource(ResourceBase):
    id: PyObjectId
    startPeriod: Optional[datetime] = None
    endPeriod: Optional[datetime] = None
    # Source type of the underlying wrapper ("API" | "CSV" | "XLSX"),
    # denormalised into the response so the UI can classify resources by
    # source without round-tripping to fetch every wrapper. `type` above is
    # the resource category (e.g. "sustainability_indicator") and is not a
    # reliable signal for API-vs-upload classification.
    source_type: Optional[str] = None

    class Config:
        from_attributes = True


class ResourceDelete(BaseModel):
    id: str
    deleted: bool

