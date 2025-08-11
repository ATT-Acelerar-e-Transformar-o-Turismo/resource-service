from pydantic import BaseModel, Field
from typing import List
from datetime import datetime, UTC
from schemas.common import PyObjectId


class DataPoint(BaseModel):
    x: datetime | float
    y: float


class TimePoint(BaseModel):
    x: datetime
    y: float


class DataSegmentBase(BaseModel):
    resource_id: PyObjectId
    points: List[TimePoint]
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class DataSegment(DataSegmentBase):
    class Config:
        from_attributes = True
