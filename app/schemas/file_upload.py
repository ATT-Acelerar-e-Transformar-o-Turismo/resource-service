from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime

class UploadedFile(BaseModel):
    file_id: str = Field(..., description="Unique identifier for the uploaded file")
    filename: str = Field(..., description="Original filename")
    file_size: int = Field(..., description="File size in bytes")
    content_type: str = Field(..., description="MIME type of the file")
    upload_timestamp: datetime = Field(default_factory=datetime.utcnow, description="When the file was uploaded")
    file_path: str = Field(..., description="Internal storage path")
    preview_data: Optional[str] = Field(None, description="Sample of file content for preview")
    validation_status: str = Field(default="pending", description="File validation status")
    validation_errors: Optional[list[str]] = Field(default=None, description="Validation error messages if any")

class FileUploadResponse(BaseModel):
    file_id: str
    filename: str
    file_size: int
    preview_data: Optional[str] = None
    validation_status: str
    validation_errors: Optional[list[str]] = None
    message: str

class FileValidationResult(BaseModel):
    is_valid: bool
    errors: list[str] = []
    preview_data: Optional[str] = None
    detected_columns: Optional[list[str]] = None
    row_count: Optional[int] = None