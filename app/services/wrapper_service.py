import uuid
import os
import asyncio
import subprocess
import json
from datetime import datetime
from typing import Optional, Dict, Any
from bson.objectid import ObjectId
from pymongo.errors import OperationFailure
from dependencies.database import db
from dependencies.rabbitmq import consumer, rabbitmq_client
from services.wrapper_generator import WrapperGenerator, IndicatorMetadata, DataSourceConfig
from services.resource_service import create_resource
from services.async_wrapper_runner import AsyncWrapperRunner
from services.abc.wrapper_runner import WrapperRunner
from services.wrapper_process_manager import wrapper_process_manager
from schemas.wrapper import (
    WrapperGenerationRequest, GeneratedWrapper, WrapperStatus,
    WrapperExecutionResult
)
from schemas.resource import ResourceCreate
from config import settings
import aio_pika
import logging
import httpx

logger = logging.getLogger(__name__)

class WrapperService:
    def __init__(self):
        # Type annotation for the runner using the Protocol
        self.runner: WrapperRunner
        self.generator = WrapperGenerator(
            gemini_api_key=settings.GEMINI_API_KEY,
            rabbitmq_url=settings.DATA_RABBITMQ_URL,
            debug_mode=True,
            debug_dir="/app/prompts"
        )
        self.runner = AsyncWrapperRunner()
        self.queue_name = "wrapper_creation_queue"
    

    async def generate_and_store_wrapper(self, request: WrapperGenerationRequest) -> GeneratedWrapper:
        """Queue wrapper creation asynchronously - all wrapper creation goes through queue"""
        try:
            # Generate unique wrapper ID
            wrapper_id = str(uuid.uuid4())
            
            # Create initial wrapper record with pending status
            wrapper = GeneratedWrapper(
                wrapper_id=wrapper_id,
                metadata=request.metadata,
                source_config=request.source_config,
                status=WrapperStatus.PENDING
            )
            
            await db.generated_wrappers.insert_one(wrapper.model_dump())
            logger.info(f"Created wrapper {wrapper_id} for async processing")
            
            # Send message to queue with wrapper_id and auto_create_resource flag
            queue_message = {
                "wrapper_id": wrapper_id,
                "auto_create_resource": request.auto_create_resource
            }
            await rabbitmq_client.publish(self.queue_name, json.dumps(queue_message))
            
            return wrapper
            
        except Exception as e:
            logger.error(f"Failed to queue wrapper creation: {e}")
            if 'wrapper_id' in locals():
                await self._update_wrapper_status(wrapper_id, WrapperStatus.ERROR, error_message=str(e))
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
                        "$set": {"status": WrapperStatus.COMPLETED.value},
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

    async def process_wrapper_creation(self, message_data: dict):
        """Process wrapper creation task - this runs in the consumer"""
        wrapper_id = message_data["wrapper_id"]
        auto_create_resource = message_data["auto_create_resource"]
        
        try:
            logger.info(f"Processing wrapper creation {wrapper_id}")
            
            # Get the wrapper record
            wrapper_doc = await db.generated_wrappers.find_one({"wrapper_id": wrapper_id})
            if not wrapper_doc:
                raise Exception(f"Wrapper {wrapper_id} not found")
            
            wrapper = GeneratedWrapper(**wrapper_doc)
            
            # Step 1: Update status to generating
            await self._update_wrapper_status(wrapper_id, WrapperStatus.GENERATING)
            
            # Step 2: Generate wrapper code
            generated_code = await self.generator.generate_wrapper(
                wrapper.metadata, wrapper.source_config, wrapper_id
            )
            
            # Update wrapper with generated code
            await db.generated_wrappers.update_one(
                {"wrapper_id": wrapper_id},
                {
                    "$set": {
                        "generated_code": generated_code,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            logger.info(f"Generated code for wrapper {wrapper_id}")
            
            # Step 3: Create resource if requested
            resource_id = None
            if auto_create_resource:
                await self._update_wrapper_status(wrapper_id, WrapperStatus.CREATING_RESOURCE)
                
                resource_data = ResourceCreate(
                    name=wrapper.metadata.name,
                    type="sustainability_indicator",
                    wrapper_id=wrapper_id
                )
                resource = await create_resource(resource_data)
                resource_id = resource["id"] if resource else None
                logger.info(f"Created resource {resource_id} for wrapper {wrapper_id}")
                
                # Update wrapper with resource_id
                await db.generated_wrappers.update_one(
                    {"wrapper_id": wrapper_id},
                    {
                        "$set": {
                            "resource_id": resource_id,
                            "updated_at": datetime.utcnow()
                        }
                    }
                )
            
            # Step 4: Execute wrapper
            await self._update_wrapper_status(wrapper_id, WrapperStatus.EXECUTING)
            
            # Create a GeneratedWrapper object for execution
            updated_wrapper = GeneratedWrapper(
                wrapper_id=wrapper_id,
                resource_id=resource_id,
                metadata=wrapper.metadata,
                source_config=wrapper.source_config,
                generated_code=generated_code,
                status=WrapperStatus.EXECUTING
            )
            
            # Handle execution based on source type
            if wrapper.source_config.source_type.value == "API":
                # For API sources: start continuous execution in separate process
                logger.info(f"Starting continuous execution for API wrapper {wrapper_id}")

                # Start wrapper in background process for continuous execution
                success = await wrapper_process_manager.start_wrapper_process(wrapper_id)

                if success:
                    logger.info(f"API wrapper {wrapper_id} started successfully and running continuously")
                else:
                    # If failed to start process, fall back to single execution
                    logger.warning(f"Failed to start process for wrapper {wrapper_id}, falling back to single execution")
                    execution_result = await self.runner.execute_wrapper(updated_wrapper)
                    await db.generated_wrappers.update_one(
                        {"wrapper_id": wrapper_id},
                        {
                            "$set": {
                                "status": WrapperStatus.COMPLETED.value,
                                "execution_result": execution_result.model_dump(),
                                "completed_at": datetime.utcnow(),
                                "updated_at": datetime.utcnow()
                            }
                        }
                    )
            else:
                # For file sources (CSV/XLSX): execute once
                execution_result = await self.runner.execute_wrapper(updated_wrapper)
                logger.info(f"Executed file wrapper {wrapper_id}")

                # Mark as completed
                await db.generated_wrappers.update_one(
                    {"wrapper_id": wrapper_id},
                    {
                        "$set": {
                            "status": WrapperStatus.COMPLETED.value,
                            "execution_result": execution_result.model_dump(),
                            "completed_at": datetime.utcnow(),
                            "updated_at": datetime.utcnow()
                        }
                    }
                )



            logger.info(f"Completed wrapper creation {wrapper_id}")
            
        except Exception as e:
            logger.error(f"Failed to process wrapper creation {wrapper_id}: {e}")
            await self._update_wrapper_status(wrapper_id, WrapperStatus.ERROR, error_message=str(e))
            raise


    async def _update_wrapper_status(self, wrapper_id: str, status: WrapperStatus, error_message: Optional[str] = None):
        """Update wrapper status in database"""
        try:
            update_data = {
                "status": status.value,
                "updated_at": datetime.utcnow()
            }
            
            if error_message:
                update_data["error_message"] = error_message
            
            if status == WrapperStatus.ERROR:
                update_data["completed_at"] = datetime.utcnow()
            
            await db.generated_wrappers.update_one(
                {"wrapper_id": wrapper_id},
                {"$set": update_data}
            )
            
            logger.info(f"Updated wrapper {wrapper_id} status to {status.value}")
            
        except Exception as e:
            logger.error(f"Failed to update wrapper status: {e}")
            raise


# Global instance
wrapper_service = WrapperService()

# Consumer function using the decorator pattern
@consumer("wrapper_creation_queue")
async def process_wrapper_creation_message(message: aio_pika.abc.AbstractIncomingMessage):
    """Process wrapper creation messages from the queue"""
    async with message.process():
        try:
            message_data = json.loads(message.body.decode())
            await wrapper_service.process_wrapper_creation(message_data)
            logger.info(f"Successfully processed wrapper creation task {message_data['wrapper_id']}")
        except Exception as e:
            logger.error(f"Error processing wrapper creation message: {e}")
            raise