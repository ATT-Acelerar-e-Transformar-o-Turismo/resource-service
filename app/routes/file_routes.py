from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from fastapi.responses import JSONResponse
from services.file_service import file_service
from schemas.file_upload import FileUploadResponse
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/upload", response_model=FileUploadResponse)
async def upload_file(file: UploadFile = File(...)):
    """
    Upload a CSV or XLSX file for wrapper generation
    
    - **file**: The file to upload (CSV, XLSX, XLS)
    
    Returns file information including:
    - file_id: Use this ID when generating wrappers
    - preview_data: Sample of the file content
    - validation_status: Whether the file is valid for processing
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")
    
    try:
        uploaded_file = await file_service.upload_file(file)
        
        return FileUploadResponse(
            file_id=uploaded_file.file_id,
            filename=uploaded_file.filename,
            file_size=uploaded_file.file_size,
            preview_data=uploaded_file.preview_data,
            validation_status=uploaded_file.validation_status,
            validation_errors=uploaded_file.validation_errors,
            message="File uploaded successfully" if uploaded_file.validation_status == "valid" else "File uploaded but has validation errors"
        )
        
    except HTTPException:
        raise
    except (IOError, OSError, ValueError, PermissionError) as e:
        logger.error(f"File upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"File upload failed: {str(e)}")

@router.get("/{file_id}")
async def get_file_info(file_id: str):
    """Get information about an uploaded file"""
    uploaded_file = await file_service.get_uploaded_file(file_id)
    
    if not uploaded_file:
        raise HTTPException(status_code=404, detail="File not found")
    
    return FileUploadResponse(
        file_id=uploaded_file.file_id,
        filename=uploaded_file.filename,
        file_size=uploaded_file.file_size,
        preview_data=uploaded_file.preview_data,
        validation_status=uploaded_file.validation_status,
        validation_errors=uploaded_file.validation_errors,
        message="File information retrieved successfully"
    )

@router.delete("/{file_id}")
async def delete_file(file_id: str):
    """Delete an uploaded file"""
    success = await file_service.delete_uploaded_file(file_id)
    
    if not success:
        raise HTTPException(status_code=404, detail="File not found")
    
    return {"message": "File deleted successfully"}

@router.get("/")
async def list_files():
    """List all uploaded files (for debugging)"""
    # This could be enhanced with pagination, filtering, etc.
    try:
        from dependencies.database import db
        files_cursor = db.uploaded_files.find({}, {"file_path": 0})  # Exclude file_path for security
        files = await files_cursor.to_list(100)  # Limit to 100 files
        
        return {
            "files": files,
            "count": len(files)
        }
    except (ConnectionError, TimeoutError) as e:
        logger.error(f"Database error listing files: {e}")
        raise HTTPException(status_code=503, detail="Database unavailable")