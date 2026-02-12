from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, Union, Annotated
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
    STOPPED = "stopped"
    COMPLETED = "completed"
    ERROR = "error"


class WrapperPhase(str, Enum):
    HISTORICAL = "historical"
    CONTINUOUS = "continuous"


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


# Source-specific configurations
class CSVSourceConfig(BaseModel):
    file_id: str = Field(..., description="File ID for uploaded CSV file")
    location: Optional[str] = Field(
        None, description="Computed file path (populated by backend)"
    )


class XLSXSourceConfig(BaseModel):
    file_id: str = Field(..., description="File ID for uploaded XLSX file")
    location: Optional[str] = Field(
        None, description="Computed file path (populated by backend)"
    )


class APISourceConfig(BaseModel):
    location: str = Field(..., description="API endpoint URL")
    auth_type: str = Field(
        default="none",
        description="Authentication type: 'bearer', 'api_key', 'basic', 'none'",
    )
    api_key: Optional[str] = Field(None, description="API key for authentication")
    api_key_header: Optional[str] = Field(
        default="X-API-Key", description="Header name for API key"
    )
    bearer_token: Optional[str] = Field(
        None, description="Bearer token for authentication"
    )
    username: Optional[str] = Field(None, description="Username for basic auth")
    password: Optional[str] = Field(None, description="Password for basic auth")
    timeout_seconds: int = Field(default=30, description="Request timeout in seconds")
    date_field: Optional[str] = Field(
        None, description="Field name for date/timestamp in API response"
    )
    value_field: Optional[str] = Field(
        None, description="Field name for the indicator value in API response"
    )
    custom_headers: Dict[str, str] = Field(
        default_factory=dict, description="Additional custom headers"
    )
    query_params: Dict[str, str] = Field(
        default_factory=dict, description="Default query parameters"
    )


# Union type for source configs
DataSourceConfig = Union[CSVSourceConfig, XLSXSourceConfig, APISourceConfig]


class WrapperGenerationRequest(BaseModel):
    source_type: SourceType = Field(..., description="Type of data source")
    source_config: DataSourceConfig = Field(
        ..., description="Source-specific configuration"
    )
    metadata: IndicatorMetadata
    auto_create_resource: bool = Field(
        default=True, description="Automatically create resource"
    )


class GeneratedWrapper(BaseModel):
    wrapper_id: str
    resource_id: Optional[str] = None
    metadata: IndicatorMetadata
    source_type: SourceType
    source_config: DataSourceConfig
    generated_code: Optional[str] = None  # Can be None during generation
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    status: WrapperStatus = Field(default=WrapperStatus.PENDING)
    error_message: Optional[str] = None
    execution_log: list[str] = Field(default_factory=list)

    # Monitoring fields
    last_health_check: datetime = Field(default_factory=datetime.utcnow)
    last_data_sent: Optional[datetime] = None
    data_points_count: int = 0
    monitoring_details: Dict[str, Any] = Field(default_factory=dict)

    # Checkpoint fields for resumable execution
    phase: WrapperPhase = Field(default=WrapperPhase.HISTORICAL)
    high_water_mark: Optional[datetime] = Field(
        None, description="Newest data point timestamp ever sent"
    )
    low_water_mark: Optional[datetime] = Field(
        None, description="Oldest data point timestamp ever sent"
    )

    execution_result: Optional["WrapperExecutionResult"] = None


class WrapperExecutionResult(BaseModel):
    wrapper_id: str
    success: bool
    message: str
    data_points_sent: Optional[int] = None
    execution_time: datetime = Field(default_factory=datetime.utcnow)


class AsyncWrapperCreationRequest(BaseModel):
    source_type: SourceType = Field(..., description="Type of data source")
    source_config: DataSourceConfig = Field(
        ..., description="Source-specific configuration"
    )
    metadata: IndicatorMetadata
    auto_create_resource: bool = Field(
        default=True, description="Automatically create resource"
    )


class AsyncWrapperCreationResponse(BaseModel):
    wrapper_id: str
    status: WrapperStatus
    message: str
