from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
from datetime import datetime
from enum import Enum

class SourceType(str, Enum):
    API = "API"
    CSV = "CSV"
    XLSX = "XLSX"

class WrapperStatus(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"

class IndicatorMetadata(BaseModel):
    name: str = Field(..., description="Name of the sustainability indicator")
    domain: str = Field(..., description="Domain (e.g., Environment, Social)")
    subdomain: str = Field(..., description="Subdomain classification")
    description: str = Field(..., description="Detailed description")
    unit: str = Field(..., description="Unit of measurement")
    source: str = Field(..., description="Data source name")
    scale: str = Field(..., description="Scale (e.g., National, Regional)")
    governance_indicator: bool = Field(..., description="Is governance indicator")
    carrying_capacity: Optional[float] = Field(None, description="Maximum capacity")
    periodicity: str = Field(..., description="Data collection frequency")

class DataSourceConfig(BaseModel):
    source_type: SourceType = Field(..., description="Type of data source")
    location: Optional[str] = Field(None, description="URL endpoint for API sources")
    file_id: Optional[str] = Field(None, description="File ID for uploaded CSV/XLSX files")
    auth_config: Dict[str, Any] = Field(default_factory=dict, description="Authentication configuration")
    
    def model_post_init(self, __context):
        """Validate that either location or file_id is provided based on source type"""
        if self.source_type == SourceType.API:
            if not self.location:
                raise ValueError("location is required for API sources")
        elif self.source_type in [SourceType.CSV, SourceType.XLSX]:
            if not self.file_id:
                raise ValueError("file_id is required for CSV/XLSX sources")
        else:
            if not self.location and not self.file_id:
                raise ValueError("Either location or file_id must be provided")

class WrapperGenerationRequest(BaseModel):
    metadata: IndicatorMetadata
    source_config: DataSourceConfig
    auto_create_resource: bool = Field(default=True, description="Automatically create resource")

class GeneratedWrapper(BaseModel):
    wrapper_id: str
    resource_id: Optional[str] = None
    metadata: IndicatorMetadata
    source_config: DataSourceConfig
    generated_code: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    status: WrapperStatus = Field(default=WrapperStatus.CREATED)
    execution_log: list[str] = Field(default_factory=list)

class WrapperExecutionResult(BaseModel):
    wrapper_id: str
    success: bool
    message: str
    data_points_sent: Optional[int] = None
    execution_time: datetime = Field(default_factory=datetime.utcnow)