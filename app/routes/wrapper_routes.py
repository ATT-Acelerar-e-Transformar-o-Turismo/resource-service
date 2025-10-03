from fastapi import APIRouter, HTTPException, BackgroundTasks
from services.wrapper_service import WrapperService
from schemas.wrapper import (
    WrapperGenerationRequest, GeneratedWrapper, WrapperExecutionResult
)
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)
router = APIRouter()
wrapper_service = WrapperService()

@router.post("/generate", response_model=GeneratedWrapper)
async def generate_wrapper(request: WrapperGenerationRequest) -> GeneratedWrapper:
    """Generate a new AI-powered wrapper for sustainability indicators"""
    try:
        wrapper = await wrapper_service.generate_and_store_wrapper(request)
        return wrapper
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
    """Get wrapper details by ID"""
    wrapper = await wrapper_service.get_wrapper(wrapper_id)
    if not wrapper:
        raise HTTPException(status_code=404, detail="Wrapper not found")
    return wrapper

@router.get("/", response_model=List[GeneratedWrapper])
async def list_wrappers(skip: int = 0, limit: int = 10) -> List[GeneratedWrapper]:
    """List all generated wrappers"""
    try:
        return await wrapper_service.list_wrappers(skip, limit)
    except Exception as e:
        logger.error(f"Failed to list wrappers: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve wrappers")

