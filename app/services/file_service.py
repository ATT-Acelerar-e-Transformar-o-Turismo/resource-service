import os
import uuid
import tempfile
from datetime import datetime
from typing import Optional
import pandas as pd
import logging
from fastapi import UploadFile, HTTPException
from dependencies.database import db
from schemas.file_upload import UploadedFile, FileValidationResult
from config import settings

logger = logging.getLogger(__name__)

class FileService:
    def __init__(self):
        # Create uploads directory if it doesn't exist
        self.upload_dir = os.path.join(os.getcwd(), "uploads")
        os.makedirs(self.upload_dir, exist_ok=True)
        
    def _get_file_path(self, file_id: str, filename: str) -> str:
        """Generate secure file path for storage"""
        # Use file_id as directory to avoid conflicts
        file_dir = os.path.join(self.upload_dir, file_id)
        os.makedirs(file_dir, exist_ok=True)
        return os.path.join(file_dir, filename)
    
    def _validate_file_type(self, filename: str, content_type: str) -> None:
        """Validate file type"""
        allowed_extensions = {'.csv', '.xlsx', '.xls'}
        allowed_content_types = {
            'text/csv',
            'application/vnd.ms-excel',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
        
        file_extension = os.path.splitext(filename)[1].lower()
        
        if file_extension not in allowed_extensions:
            raise HTTPException(
                status_code=400,
                detail=f"File type not supported. Allowed: {', '.join(allowed_extensions)}"
            )
        
        # Content type validation is less strict due to browser inconsistencies
        if content_type and not any(ct in content_type for ct in ['csv', 'excel', 'spreadsheet']):
            logger.warning(f"Unexpected content type: {content_type} for file: {filename}")
    
    def _validate_file_content(self, file_path: str) -> FileValidationResult:
        """Validate and analyze file content"""
        try:
            file_extension = os.path.splitext(file_path)[1].lower()
            
            # Read file based on extension
            if file_extension == '.csv':
                df = pd.read_csv(file_path, nrows=10)  # Read first 10 rows for validation
                full_df = pd.read_csv(file_path)  # Get full count
            elif file_extension in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path, nrows=10)
                full_df = pd.read_excel(file_path)
            else:
                return FileValidationResult(
                    is_valid=False,
                    errors=["Unsupported file format"]
                )
            
            errors = []
            
            # Check if file has data
            if df.empty:
                errors.append("File is empty")
            
            # Check if file has at least 2 columns (x, y for time series)
            if len(df.columns) < 2:
                errors.append("File must have at least 2 columns for time series data")
            
            # Generate preview data
            preview_data = df.head(5).to_string(index=False) if not df.empty else None
            
            return FileValidationResult(
                is_valid=len(errors) == 0,
                errors=errors,
                preview_data=preview_data,
                detected_columns=df.columns.tolist() if not df.empty else None,
                row_count=len(full_df) if not df.empty else 0
            )
            
        except Exception as e:
            logger.error(f"Error validating file content: {e}")
            return FileValidationResult(
                is_valid=False,
                errors=[f"Error reading file: {str(e)}"]
            )
    
    async def upload_file(self, file: UploadFile) -> UploadedFile:
        """Upload and validate file"""
        try:
            # Generate unique file ID
            file_id = str(uuid.uuid4())
            
            # Validate file type
            self._validate_file_type(file.filename, file.content_type)
            
            # Read file content
            content = await file.read()
            file_size = len(content)
            
            # Check file size (max 50MB)
            max_size = 50 * 1024 * 1024  # 50MB
            if file_size > max_size:
                raise HTTPException(
                    status_code=400,
                    detail=f"File too large. Maximum size: {max_size/1024/1024}MB"
                )
            
            # Save file to disk
            file_path = self._get_file_path(file_id, file.filename)
            with open(file_path, 'wb') as f:
                f.write(content)
            
            # Validate file content
            validation_result = self._validate_file_content(file_path)
            
            # Create uploaded file record
            uploaded_file = UploadedFile(
                file_id=file_id,
                filename=file.filename,
                file_size=file_size,
                content_type=file.content_type or "application/octet-stream",
                upload_timestamp=datetime.utcnow(),
                file_path=file_path,
                preview_data=validation_result.preview_data,
                validation_status="valid" if validation_result.is_valid else "invalid",
                validation_errors=validation_result.errors if validation_result.errors else None
            )
            
            # Store in database
            await db.uploaded_files.insert_one(uploaded_file.model_dump())
            
            logger.info(f"File uploaded successfully: {file_id} - {file.filename}")
            return uploaded_file
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error uploading file: {e}")
            raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")
    
    async def get_uploaded_file(self, file_id: str) -> Optional[UploadedFile]:
        """Retrieve uploaded file information"""
        try:
            file_doc = await db.uploaded_files.find_one({"file_id": file_id})
            if not file_doc:
                return None
            return UploadedFile(**file_doc)
        except Exception as e:
            logger.error(f"Error retrieving uploaded file {file_id}: {e}")
            raise HTTPException(status_code=500, detail="Error retrieving file")
    
    async def delete_uploaded_file(self, file_id: str) -> bool:
        """Delete uploaded file from disk and database"""
        try:
            # Get file info
            uploaded_file = await self.get_uploaded_file(file_id)
            if not uploaded_file:
                return False
            
            # Delete from disk
            if os.path.exists(uploaded_file.file_path):
                os.remove(uploaded_file.file_path)
                # Also try to remove the directory if empty
                file_dir = os.path.dirname(uploaded_file.file_path)
                try:
                    os.rmdir(file_dir)
                except OSError:
                    pass  # Directory not empty
            
            # Delete from database
            result = await db.uploaded_files.delete_one({"file_id": file_id})
            
            logger.info(f"File deleted: {file_id}")
            return result.deleted_count > 0
            
        except Exception as e:
            logger.error(f"Error deleting file {file_id}: {e}")
            return False

# Global file service instance
file_service = FileService()