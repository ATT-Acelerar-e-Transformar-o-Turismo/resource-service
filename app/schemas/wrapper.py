from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
from datetime import datetime
from enum import Enum
import uuid

class SourceType(str, Enum):
    API = "API"
    CSV = "CSV"
    XLSX = "XLSX"

class WrapperStatus(str, Enum):
    PENDING = "pending"
    GENERATING = "generating" 
    CREATING_RESOURCE = "creating_resource"
    EXECUTING = "executing"
    COMPLETED = "completed"
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
    
    # API Authentication
    auth_type: Optional[str] = Field(None, description="Authentication type: 'bearer', 'api_key', 'basic', 'none'")
    api_key: Optional[str] = Field(None, description="API key for authentication")
    api_key_header: Optional[str] = Field("X-API-Key", description="Header name for API key")
    bearer_token: Optional[str] = Field(None, description="Bearer token for authentication")
    username: Optional[str] = Field(None, description="Username for basic auth")
    password: Optional[str] = Field(None, description="Password for basic auth")
    
    # API Configuration
    rate_limit_per_minute: Optional[int] = Field(60, description="Maximum requests per minute")
    timeout_seconds: Optional[int] = Field(30, description="Request timeout in seconds")
    retry_attempts: Optional[int] = Field(3, description="Number of retry attempts on failure")
    date_field: Optional[str] = Field(None, description="Field name for date/timestamp in API response")
    value_field: Optional[str] = Field(None, description="Field name for the indicator value in API response")
    custom_headers: Dict[str, str] = Field(default_factory=dict, description="Additional custom headers")
    query_params: Dict[str, str] = Field(default_factory=dict, description="Default query parameters")
    
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
    generated_code: Optional[str] = None  # Can be None during generation
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    status: WrapperStatus = Field(default=WrapperStatus.PENDING)
    error_message: Optional[str] = None
    execution_log: list[str] = Field(default_factory=list)
    execution_result: Optional["WrapperExecutionResult"] = None

class WrapperExecutionResult(BaseModel):
    wrapper_id: str
    success: bool
    message: str
    data_points_sent: Optional[int] = None
    execution_time: datetime = Field(default_factory=datetime.utcnow)

class AsyncWrapperCreationRequest(BaseModel):
    metadata: IndicatorMetadata
    source_config: DataSourceConfig
    auto_create_resource: bool = Field(default=True, description="Automatically create resource")

class AsyncWrapperCreationResponse(BaseModel):
    wrapper_id: str
    status: WrapperStatus
    message: str