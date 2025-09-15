import uuid
import os
import asyncio
import subprocess
from datetime import datetime
from typing import Optional, Dict, Any
from bson.objectid import ObjectId
from pymongo.errors import OperationFailure
from dependencies.database import db
from services.wrapper_generator import WrapperGenerator, IndicatorMetadata, DataSourceConfig
from services.resource_service import create_resource
from services.async_wrapper_runner import AsyncWrapperRunner
from services.abc.wrapper_runner import WrapperRunner
from schemas.wrapper import (
    WrapperGenerationRequest, GeneratedWrapper, WrapperStatus, 
    WrapperExecutionResult
)
from schemas.resource import ResourceCreate
from config import settings
import logging

logger = logging.getLogger(__name__)

class WrapperService:
    def __init__(self):
        # Type annotation for the runner using the Protocol
        self.runner: WrapperRunner
        self.generator = WrapperGenerator(
            gemini_api_key=settings.GEMINI_API_KEY,
            rabbitmq_url=settings.DATA_RABBITMQ_URL,
            debug_mode=False,
            debug_dir="prompts"
        )
        self.runner = AsyncWrapperRunner()
    
    async def generate_and_store_wrapper(self, request: WrapperGenerationRequest) -> GeneratedWrapper:
        """Generate wrapper code and store in database"""
        try:
            # Generate unique wrapper ID
            wrapper_id = str(uuid.uuid4())
            
            # Resolve file_id to actual file path if needed
            if hasattr(request.source_config, 'file_id') and request.source_config.file_id:
                from services.file_service import file_service
                uploaded_file = await file_service.get_uploaded_file(request.source_config.file_id)
                if not uploaded_file:
                    raise Exception(f"File with ID {request.source_config.file_id} not found")
                if uploaded_file.validation_status != "valid":
                    raise Exception(f"File {request.source_config.file_id} failed validation: {uploaded_file.validation_errors}")
                # Set location to the resolved file path
                request.source_config.location = uploaded_file.file_path

            # Generate wrapper code using AI
            logger.info(f"Generating wrapper {wrapper_id} for indicator {request.metadata.name}")
            wrapper_code = await self.generator.generate_wrapper(
                request.metadata,
                request.source_config,
                wrapper_id
            )
            
            # Create resource if requested
            resource_id = None
            if request.auto_create_resource:
                resource_data = ResourceCreate(
                    name=request.metadata.name,
                    type="sustainability_indicator",
                    wrapper_id=wrapper_id
                )
                resource = await create_resource(resource_data)
                resource_id = resource["id"] if resource else None
            
            # Create wrapper record
            wrapper = GeneratedWrapper(
                wrapper_id=wrapper_id,
                resource_id=resource_id,
                metadata=request.metadata,
                source_config=request.source_config,
                generated_code=wrapper_code,
                status=WrapperStatus.CREATED
            )
            
            # Store in database
            await db.generated_wrappers.insert_one(wrapper.model_dump())
            logger.info(f"Wrapper {wrapper_id} generated and stored successfully")
            
            return wrapper
            
        except Exception as e:
            logger.error(f"Failed to generate wrapper: {e}")
            raise
    
    async def execute_wrapper(self, wrapper_id: str) -> WrapperExecutionResult:
        """Execute a generated wrapper using the wrapper runner"""
        try:
            # Get wrapper from database
            wrapper_doc = await db.generated_wrappers.find_one({"wrapper_id": wrapper_id})
            if not wrapper_doc:
                return WrapperExecutionResult(
                    wrapper_id=wrapper_id,
                    success=False,
                    message="Wrapper not found",
                    data_points_sent=None,
                    execution_time=datetime.utcnow().isoformat()
                )
            
            wrapper = GeneratedWrapper(**wrapper_doc)
            
            # Execute wrapper using the runner adapter
            result = await self.runner.execute_wrapper(wrapper)
            
            # Update database based on execution result
            if result.success:
                await db.generated_wrappers.update_one(
                    {"wrapper_id": wrapper_id},
                    {
                        "$set": {"status": WrapperStatus.RUNNING.value},
                        "$push": {"execution_log": f"Executed successfully at {datetime.utcnow()}"}
                    }
                )
            else:
                await db.generated_wrappers.update_one(
                    {"wrapper_id": wrapper_id},
                    {
                        "$set": {"status": WrapperStatus.ERROR.value},
                        "$push": {"execution_log": f"Error at {datetime.utcnow()}: {result.message}"}
                    }
                )
            
            return result
                
        except Exception as e:
            logger.error(f"Failed to execute wrapper {wrapper_id}: {e}")
            
            # Update error status
            await db.generated_wrappers.update_one(
                {"wrapper_id": wrapper_id},
                {
                    "$set": {"status": WrapperStatus.ERROR.value},
                    "$push": {"execution_log": f"Error at {datetime.utcnow()}: {str(e)}"}
                }
            )
            
            return WrapperExecutionResult(
                wrapper_id=wrapper_id,
                success=False,
                message=f"Execution failed: {str(e)}",
                data_points_sent=None,
                execution_time=datetime.utcnow().isoformat()
            )
    
    async def get_wrapper(self, wrapper_id: str) -> Optional[GeneratedWrapper]:
        """Get wrapper by ID"""
        try:
            wrapper_doc = await db.generated_wrappers.find_one({"wrapper_id": wrapper_id})
            if not wrapper_doc:
                return None
            return GeneratedWrapper(**wrapper_doc)
        except OperationFailure as e:
            logger.error(f"Database operation failed in get_wrapper: {e}")
            raise
    
    async def list_wrappers(self, skip: int = 0, limit: int = 10) -> list[GeneratedWrapper]:
        """List all generated wrappers"""
        try:
            wrappers = await db.generated_wrappers.find({}).skip(skip).limit(limit).to_list(limit)
            return [GeneratedWrapper(**w) for w in wrappers]
        except OperationFailure as e:
            logger.error(f"Database operation failed in list_wrappers: {e}")
            raise