from fastapi import APIRouter, HTTPException, BackgroundTasks
from services.wrapper_service import wrapper_service
from services.file_service import file_service
from schemas.wrapper import (
    WrapperGenerationRequest, GeneratedWrapper, WrapperExecutionResult,
    SourceType, CSVSourceConfig, XLSXSourceConfig, WrapperStatus
)
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/generate", response_model=GeneratedWrapper)
async def generate_wrapper(request: WrapperGenerationRequest) -> GeneratedWrapper:
    """Generate a new AI-powered wrapper for sustainability indicators"""
    try:
        # Compute location from file_id for CSV/XLSX sources
        if request.source_type in [SourceType.CSV, SourceType.XLSX]:
            if isinstance(request.source_config, (CSVSourceConfig, XLSXSourceConfig)):
                uploaded_file = await file_service.get_uploaded_file(request.source_config.file_id)
                if not uploaded_file:
                    raise HTTPException(status_code=400, detail=f"File not found: {request.source_config.file_id}")
                # Populate location with computed file path
                request.source_config.location = uploaded_file.file_path
                logger.info(f"Resolved file_id {request.source_config.file_id} to path: {uploaded_file.file_path}")

        wrapper = await wrapper_service.generate_and_store_wrapper(request)
        return wrapper
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Wrapper generation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Wrapper generation failed: {str(e)}")

@router.post("/{wrapper_id}/execute", response_model=WrapperExecutionResult)
async def execute_wrapper(wrapper_id: str, background_tasks: BackgroundTasks) -> WrapperExecutionResult:
    """Execute a generated wrapper to fetch and send data"""
    try:
        result = await wrapper_service.execute_wrapper(wrapper_id)
        return result
    except Exception as e:
        logger.error(f"Wrapper execution failed: {e}")
        raise HTTPException(status_code=500, detail=f"Execution failed: {str(e)}")

@router.get("/{wrapper_id}", response_model=GeneratedWrapper)
async def get_wrapper(wrapper_id: str) -> GeneratedWrapper:
    """Get wrapper details by ID with real-time status validation"""
    wrapper = await wrapper_service.get_wrapper(wrapper_id)
    if not wrapper:
        raise HTTPException(status_code=404, detail="Wrapper not found")

    # Real-time status validation: if wrapper claims to be executing, verify it's actually running
    if wrapper.status == "executing":
        try:
            is_actually_running = await wrapper_service.is_wrapper_actively_executing(wrapper_id)
            if not is_actually_running:
                # Wrapper claims to be executing but isn't actually running - update status
                await wrapper_service._update_wrapper_status(
                    wrapper_id,
                    WrapperStatus.ERROR,
                    "Process not found - wrapper was not actually running"
                )
                # Refresh the wrapper data with updated status
                wrapper = await wrapper_service.get_wrapper(wrapper_id)
                logger.info(f"Updated wrapper {wrapper_id} status from 'executing' to 'error' - process not found")
        except Exception as e:
            logger.warning(f"Failed to validate status for wrapper {wrapper_id}: {e}")

    return wrapper

@router.get("/", response_model=List[GeneratedWrapper])
async def list_wrappers(skip: int = 0, limit: int = 10) -> List[GeneratedWrapper]:
    """List all generated wrappers"""
    try:
        return await wrapper_service.list_wrappers(skip, limit)
    except Exception as e:
        logger.error(f"Failed to list wrappers: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve wrappers")

@router.post("/{wrapper_id}/stop")
async def stop_wrapper(wrapper_id: str) -> dict:
    """Stop a running wrapper (universal endpoint)"""
    try:
        success = await wrapper_service.stop_wrapper_execution(wrapper_id)
        return {
            "wrapper_id": wrapper_id,
            "success": success,
            "message": "Wrapper stopped successfully" if success else "Failed to stop wrapper"
        }
    except Exception as e:
        logger.error(f"Failed to stop wrapper {wrapper_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to stop wrapper: {str(e)}")

@router.get("/{wrapper_id}/health")
async def get_wrapper_health(wrapper_id: str) -> dict:
    """Get wrapper health status (universal endpoint)"""
    try:
        health_status = await wrapper_service.get_wrapper_health_status(wrapper_id)
        is_executing = await wrapper_service.is_wrapper_actively_executing(wrapper_id)

        return {
            "wrapper_id": wrapper_id,
            "health_status": health_status,
            "is_actively_executing": is_executing
        }
    except Exception as e:
        logger.error(f"Failed to get health for wrapper {wrapper_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get health status: {str(e)}")

@router.get("/{wrapper_id}/logs")
async def get_wrapper_logs(wrapper_id: str, limit: int = 200) -> dict:
    """Get wrapper logs from log files (universal endpoint)"""
    try:
        logs = await wrapper_service.get_wrapper_logs(wrapper_id, limit)
        return {
            "wrapper_id": wrapper_id,
            "logs": logs,
            "total_lines": len(logs)
        }
    except Exception as e:
        logger.error(f"Failed to get logs for wrapper {wrapper_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve logs: {str(e)}")

@router.get("/{wrapper_id}/monitoring")
async def get_wrapper_monitoring_details(wrapper_id: str) -> dict:
    """Get type-specific monitoring details (universal endpoint with flexible response)"""
    try:
        monitoring_details = await wrapper_service.get_wrapper_monitoring_details(wrapper_id)
        health_status = await wrapper_service.get_wrapper_health_status(wrapper_id)

        return {
            "wrapper_id": wrapper_id,
            "health_status": health_status,
            "monitoring_details": monitoring_details
        }
    except Exception as e:
        logger.error(f"Failed to get monitoring details for wrapper {wrapper_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get monitoring details: {str(e)}")
