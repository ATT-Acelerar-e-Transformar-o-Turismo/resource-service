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
from services.wrapper_generator import (
    WrapperGenerator,
    IndicatorMetadata,
    DataSourceConfig,
)
from services.resource_service import create_resource
from services.abc.wrapper_runner import WrapperRunner
from services.abc.wrapper_monitor import WrapperMonitor
from services.wrapper_process_manager import wrapper_process_manager
from services.process_wrapper_runner import ProcessWrapperRunner
from schemas.wrapper import (
    WrapperGenerationRequest,
    GeneratedWrapper,
    WrapperStatus,
    WrapperExecutionResult,
    SourceType,
)
from schemas.resource import ResourceCreate
from config import settings
import aio_pika
import logging

logger = logging.getLogger(__name__)


class WrapperService:
    def __init__(self, runner: WrapperRunner, generator: WrapperGenerator):
        self._runner = runner
        self.generator = generator
        self.queue_name = settings.WRAPPER_CREATION_QUEUE_NAME

    async def generate_and_store_wrapper(
        self, request: WrapperGenerationRequest
    ) -> GeneratedWrapper:
        """Create wrapper and resource synchronously, then queue wrapper execution"""
        try:
            wrapper_id = str(uuid.uuid4())

            wrapper = GeneratedWrapper(
                wrapper_id=wrapper_id,
                metadata=request.metadata,
                source_type=request.source_type,
                source_config=request.source_config,
                status=WrapperStatus.PENDING,
            )

            insert_result = await db.generated_wrappers.insert_one(wrapper.model_dump())
            logger.info(
                f"Created wrapper {wrapper_id} with ObjectId {insert_result.inserted_id}"
            )

            resource_id = None
            if request.auto_create_resource:
                resource = await create_resource(
                    ResourceCreate(
                        name=request.metadata.name,
                        type="sustainability_indicator",
                        wrapper_id=wrapper_id,
                    )
                )
                resource_id = resource["id"] if resource else None
                logger.info(f"Created resource {resource_id} for wrapper {wrapper_id}")

                await db.generated_wrappers.update_one(
                    {"wrapper_id": wrapper_id},
                    {
                        "$set": {
                            "resource_id": resource_id,
                            "updated_at": datetime.utcnow(),
                        }
                    },
                )

            wrapper.resource_id = resource_id

            await rabbitmq_client.publish(
                self.queue_name, json.dumps({"wrapper_id": wrapper_id})
            )

            return wrapper

        except (OperationFailure, ConnectionError) as e:
            logger.error(f"Database/Connection error queueing wrapper creation: {e}")
            if "wrapper_id" in locals():
                await self._update_wrapper_status(
                    wrapper_id, WrapperStatus.ERROR, error_message=str(e)
                )
            raise
        except (ValueError, KeyError) as e:
            logger.error(f"Invalid wrapper configuration: {e}")
            if "wrapper_id" in locals():
                await self._update_wrapper_status(
                    wrapper_id, WrapperStatus.ERROR, error_message=str(e)
                )
            raise

    async def execute_wrapper(self, wrapper_id: str) -> WrapperExecutionResult:
        """Execute a generated wrapper using the wrapper runner"""
        try:
            wrapper_doc = await db.generated_wrappers.find_one(
                {"wrapper_id": wrapper_id}
            )
            if not wrapper_doc:
                return WrapperExecutionResult(
                    wrapper_id=wrapper_id,
                    success=False,
                    message="Wrapper not found",
                    data_points_sent=None,
                    execution_time=datetime.utcnow().isoformat(),
                )

            wrapper = GeneratedWrapper(**wrapper_doc)

            result = await self._runner.execute_wrapper(wrapper)

            if result.success:
                await db.generated_wrappers.update_one(
                    {"wrapper_id": wrapper_id},
                    {
                        "$set": {"status": WrapperStatus.COMPLETED.value},
                        "$push": {
                            "execution_log": f"Executed successfully at {datetime.utcnow()}"
                        },
                    },
                )
            else:
                await db.generated_wrappers.update_one(
                    {"wrapper_id": wrapper_id},
                    {
                        "$set": {"status": WrapperStatus.ERROR.value},
                        "$push": {
                            "execution_log": f"Error at {datetime.utcnow()}: {result.message}"
                        },
                    },
                )

            return result

        except (OperationFailure, ConnectionError) as e:
            logger.error(
                f"Database/Connection error executing wrapper {wrapper_id}: {e}"
            )

            await db.generated_wrappers.update_one(
                {"wrapper_id": wrapper_id},
                {
                    "$set": {"status": WrapperStatus.ERROR.value},
                    "$push": {
                        "execution_log": f"Error at {datetime.utcnow()}: {str(e)}"
                    },
                },
            )

            return WrapperExecutionResult(
                wrapper_id=wrapper_id,
                success=False,
                message=f"Execution failed: {str(e)}",
                data_points_sent=None,
                execution_time=datetime.utcnow().isoformat(),
            )
        except (ValueError, FileNotFoundError, subprocess.SubprocessError) as e:
            logger.error(f"Execution error for wrapper {wrapper_id}: {e}")

            await db.generated_wrappers.update_one(
                {"wrapper_id": wrapper_id},
                {
                    "$set": {"status": WrapperStatus.ERROR.value},
                    "$push": {
                        "execution_log": f"Error at {datetime.utcnow()}: {str(e)}"
                    },
                },
            )

            return WrapperExecutionResult(
                wrapper_id=wrapper_id,
                success=False,
                message=f"Execution failed: {str(e)}",
                data_points_sent=None,
                execution_time=datetime.utcnow().isoformat(),
            )

    async def get_wrapper(self, wrapper_id: str) -> Optional[GeneratedWrapper]:
        """Get wrapper by ID"""
        try:
            wrapper_doc = await db.generated_wrappers.find_one(
                {"wrapper_id": wrapper_id}
            )
            if not wrapper_doc:
                return None
            return GeneratedWrapper(**wrapper_doc)
        except OperationFailure as e:
            logger.error(f"Database operation failed in get_wrapper: {e}")
            raise

    async def list_wrappers(
        self, skip: int = 0, limit: int = 10
    ) -> list[GeneratedWrapper]:
        """List all generated wrappers"""
        try:
            wrappers = (
                await db.generated_wrappers.find({})
                .skip(skip)
                .limit(limit)
                .to_list(limit)
            )
            return [GeneratedWrapper(**w) for w in wrappers]
        except OperationFailure as e:
            logger.error(f"Database operation failed in list_wrappers: {e}")
            raise

    async def process_wrapper_creation(self, message_data: dict):
        """Process wrapper creation task - this runs in the consumer"""
        wrapper_id = message_data["wrapper_id"]

        try:
            logger.info(f"Processing wrapper creation {wrapper_id}")

            wrapper_doc = await db.generated_wrappers.find_one(
                {"wrapper_id": wrapper_id}
            )
            if not wrapper_doc:
                logger.error(f"Wrapper {wrapper_id} not found in database")
                all_wrappers = await db.generated_wrappers.find({}).to_list(None)
                logger.error(f"Total wrappers in database: {len(all_wrappers)}")
                recent_wrappers = [w.get("wrapper_id") for w in all_wrappers[-5:]]
                logger.error(f"Recent wrapper IDs: {recent_wrappers}")
                raise Exception(f"Wrapper with ID '{wrapper_id}' does not exist")

            wrapper = GeneratedWrapper(**wrapper_doc)

            await self._update_wrapper_status(wrapper_id, WrapperStatus.GENERATING)

            generated_code = await self.generator.generate_wrapper(
                wrapper.metadata,
                wrapper.source_config,
                wrapper.source_type.value,
                wrapper_id,
            )

            await db.generated_wrappers.update_one(
                {"wrapper_id": wrapper_id},
                {
                    "$set": {
                        "generated_code": generated_code,
                        "updated_at": datetime.utcnow(),
                    }
                },
            )
            logger.info(f"Generated code for wrapper {wrapper_id}")

            wrapper_file_path = self.generator.save_wrapper(generated_code, wrapper_id)
            logger.info(f"Saved wrapper code to file: {wrapper_file_path}")

            resource_id = wrapper.resource_id
            logger.info(
                f"Using existing resource {resource_id} for wrapper {wrapper_id}"
            )

            await self._update_wrapper_status(wrapper_id, WrapperStatus.EXECUTING)

            updated_wrapper = GeneratedWrapper(
                wrapper_id=wrapper_id,
                resource_id=resource_id,
                metadata=wrapper.metadata,
                source_type=wrapper.source_type,
                source_config=wrapper.source_config,
                generated_code=generated_code,
                status=WrapperStatus.EXECUTING,
            )

            execution_result = await self._runner.execute_wrapper(updated_wrapper)
            logger.info(
                f"Executed wrapper {wrapper_id} with {type(self._runner).__name__}"
            )

            if execution_result.success:
                # For API wrappers, they run continuously, so mark as EXECUTING
                # For file wrappers, they complete immediately, so mark as COMPLETED
                status = (
                    WrapperStatus.EXECUTING
                    if wrapper.source_type == SourceType.API
                    else WrapperStatus.COMPLETED
                )
                completed_at = (
                    datetime.utcnow() if status == WrapperStatus.COMPLETED else None
                )

                update_data = {
                    "status": status.value,
                    "execution_result": execution_result.model_dump(),
                    "updated_at": datetime.utcnow(),
                }
                if completed_at:
                    update_data["completed_at"] = completed_at

                await db.generated_wrappers.update_one(
                    {"wrapper_id": wrapper_id}, {"$set": update_data}
                )
            else:
                # Mark as error regardless of source type
                await db.generated_wrappers.update_one(
                    {"wrapper_id": wrapper_id},
                    {
                        "$set": {
                            "status": WrapperStatus.ERROR.value,
                            "execution_result": execution_result.model_dump(),
                            "error_message": execution_result.message,
                            "completed_at": datetime.utcnow(),
                            "updated_at": datetime.utcnow(),
                        }
                    },
                )

            logger.info(f"Completed wrapper creation {wrapper_id}")

        except (OperationFailure, ConnectionError, TimeoutError, OSError) as e:
            logger.error(
                f"Database/Connection error processing wrapper {wrapper_id}: {e}"
            )
            await self._update_wrapper_status(
                wrapper_id, WrapperStatus.ERROR, error_message=str(e)
            )
            raise
        except (ValueError, KeyError, FileNotFoundError) as e:
            logger.error(
                f"Configuration/File error processing wrapper {wrapper_id}: {e}"
            )
            await self._update_wrapper_status(
                wrapper_id, WrapperStatus.ERROR, error_message=str(e)
            )
            raise

    async def _update_wrapper_status(
        self,
        wrapper_id: str,
        status: WrapperStatus,
        error_message: Optional[str] = None,
    ):
        """Update wrapper status in database"""
        try:
            update_data = {"status": status.value, "updated_at": datetime.utcnow()}

            if error_message:
                update_data["error_message"] = error_message

            if status == WrapperStatus.ERROR:
                update_data["completed_at"] = datetime.utcnow()

            await db.generated_wrappers.update_one(
                {"wrapper_id": wrapper_id}, {"$set": update_data}
            )

            logger.info(f"Updated wrapper {wrapper_id} status to {status.value}")

        except (OperationFailure, ConnectionError) as e:
            logger.error(f"Database error updating wrapper status: {e}")
            raise

    async def stop_wrapper_execution(self, wrapper_id: str) -> bool:
        """Stop a wrapper (universal method - internal logic varies by type)

        Args:
            wrapper_id: ID of wrapper to stop

        Returns:
            bool: True if wrapper stopped successfully
        """
        try:
            wrapper = await self.get_wrapper(wrapper_id)
            if not wrapper:
                logger.warning(f"Wrapper {wrapper_id} not found for stop operation")
                return False

            monitor = self._runner.get_monitor()
            success = await monitor.stop_wrapper(wrapper_id)

            if success:
                logger.info(f"Successfully stopped wrapper {wrapper_id}")
            else:
                logger.warning(f"Failed to stop wrapper {wrapper_id}")

            return success

        except (ProcessLookupError, PermissionError) as e:
            logger.error(f"Process error stopping wrapper {wrapper_id}: {e}")
            return False
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Connection error stopping wrapper {wrapper_id}: {e}")
            return False

    async def get_wrapper_health_status(self, wrapper_id: str) -> str:
        """Get current health status of wrapper (universal method)

        Args:
            wrapper_id: ID of wrapper to check

        Returns:
            str: Health status (HEALTHY, STALLED, DEGRADED, CRASHED, UNKNOWN)
        """
        try:
            wrapper = await self.get_wrapper(wrapper_id)
            if not wrapper:
                return "UNKNOWN"

            monitor = self._runner.get_monitor()
            return await monitor.get_health_status(wrapper_id)

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                f"Connection error getting health status for wrapper {wrapper_id}: {e}"
            )
            return "UNKNOWN"
        except (ValueError, KeyError) as e:
            logger.error(
                f"Invalid data getting health status for wrapper {wrapper_id}: {e}"
            )
            return "UNKNOWN"

    async def get_wrapper_monitoring_details(self, wrapper_id: str) -> dict:
        try:
            wrapper = await self.get_wrapper(wrapper_id)
            if not wrapper:
                return {"error": "Wrapper not found"}

            monitor = self._runner.get_monitor()
            return await monitor.get_monitoring_details(wrapper_id)

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                f"Connection error getting monitoring details for wrapper {wrapper_id}: {e}"
            )
            return {"error": "Service unavailable"}
        except (ValueError, KeyError) as e:
            logger.error(
                f"Invalid data getting monitoring details for wrapper {wrapper_id}: {e}"
            )
            return {"error": "Invalid data"}

    async def get_wrapper_logs(self, wrapper_id: str, limit: int = 100) -> list:
        """Get wrapper logs from log files (universal method)

        Args:
            wrapper_id: ID of wrapper to get logs for
            limit: Maximum number of log lines to return

        Returns:
            list: Log lines from log files
        """
        try:
            wrapper = await self.get_wrapper(wrapper_id)
            if not wrapper:
                return ["Error: Wrapper not found"]

            monitor = self._runner.get_monitor()
            return await monitor.get_logs(wrapper_id, limit)

        except (IOError, FileNotFoundError, PermissionError) as e:
            logger.error(
                f"File system error getting logs for wrapper {wrapper_id}: {e}"
            )
            return [f"Error reading logs: {str(e)}"]
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Connection error getting logs for wrapper {wrapper_id}: {e}")
            return ["Error: Service unavailable"]

    async def is_wrapper_actively_executing(self, wrapper_id: str) -> bool:
        """Check if wrapper is actively executing (universal method)

        Args:
            wrapper_id: ID of wrapper to check

        Returns:
            bool: True if wrapper is actively executing
        """
        try:
            wrapper = await self.get_wrapper(wrapper_id)
            if not wrapper:
                return False

            monitor = self._runner.get_monitor()
            return await monitor.is_actively_executing(wrapper_id)

        except (ConnectionError, TimeoutError, ProcessLookupError) as e:
            logger.error(f"Error checking if wrapper {wrapper_id} is executing: {e}")
            return False

    async def retry_failed_wrapper(self, wrapper_id: str) -> bool:
        """Retry a crashed wrapper by calling Gemini with error context.

        Returns True if retry was dispatched successfully, False otherwise.
        Max 3 retries per wrapper.
        """
        max_retries = 3
        try:
            wrapper_doc = await db.generated_wrappers.find_one(
                {"wrapper_id": wrapper_id}
            )
            if not wrapper_doc:
                logger.error(f"Retry: wrapper {wrapper_id} not found")
                return False

            wrapper = GeneratedWrapper(**wrapper_doc)
            current_retry = wrapper.retry_count

            if current_retry >= max_retries:
                logger.warning(
                    f"Retry: wrapper {wrapper_id} exhausted {max_retries} retries"
                )
                await self._update_wrapper_status(
                    wrapper_id,
                    WrapperStatus.ERROR,
                    error_message=f"Failed after {max_retries} auto-retry attempts",
                )
                return False

            await self._update_wrapper_status(wrapper_id, WrapperStatus.RETRYING)

            # Read original prompt from generation trace
            original_prompt = None
            trace_path = f"/app/prompts/{wrapper_id}/generation_trace.json"
            try:
                with open(trace_path, "r", encoding="utf-8") as f:
                    trace_data = json.load(f)
                    original_prompt = trace_data.get("initial_prompt")
            except (FileNotFoundError, json.JSONDecodeError, IOError) as e:
                logger.warning(
                    f"Retry: could not read generation trace for {wrapper_id}: {e}"
                )

            # Read crashed code
            wrapper_file_path = f"/app/generated_wrappers/{wrapper_id}.py"
            try:
                with open(wrapper_file_path, "r", encoding="utf-8") as f:
                    generated_code = f.read()
            except (FileNotFoundError, IOError) as e:
                logger.error(
                    f"Retry: could not read wrapper file for {wrapper_id}: {e}"
                )
                await self._update_wrapper_status(
                    wrapper_id,
                    WrapperStatus.ERROR,
                    error_message=f"Retry failed: wrapper file not found",
                )
                return False

            # Read error logs
            error_log = ""
            stderr_path = f"/app/wrapper_logs/{wrapper_id}_stderr.log"
            try:
                with open(stderr_path, "r", encoding="utf-8") as f:
                    error_log = f.read()
            except (FileNotFoundError, IOError):
                pass

            stdout_log = ""
            stdout_path = f"/app/wrapper_logs/{wrapper_id}_stdout.log"
            try:
                with open(stdout_path, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                    stdout_log = "".join(lines[-20:]) if lines else ""
            except (FileNotFoundError, IOError):
                pass

            # Build retry prompt
            retry_prompt = self.generator.prompt_manager.generate_runtime_retry_prompt(
                generated_code=generated_code,
                error_log=error_log or "No stderr captured",
                original_prompt=original_prompt,
                stdout_log=stdout_log or None,
            )

            # Call Gemini — use tool-calling for API wrappers
            logger.info(
                f"Retry {current_retry + 1}/{max_retries}: calling Gemini for wrapper {wrapper_id}"
            )

            auth_config = {}
            if wrapper.source_type == SourceType.API:
                auth_config = self.generator._build_api_auth_config(
                    wrapper.source_config
                )
                fixed_code = await self.generator._call_model_with_tools(
                    prompt=retry_prompt,
                    auth_config=auth_config,
                    max_tool_calls=10,
                    max_chars=2500,
                    wrapper_id=wrapper_id,
                )
            else:
                fixed_code = await self.generator._call_model(retry_prompt)

            fixed_code = self.generator._clean_code_response(fixed_code)

            # Lint the fixed code
            lint_error = self.generator._validate_generated_code(fixed_code)
            if lint_error:
                lint_retry_prompt = (
                    self.generator.prompt_manager.generate_wrapper_lint_retry_prompt(
                        generated_code=fixed_code,
                        linting_errors=lint_error,
                    )
                )
                lint_retry_response = await self.generator._call_model(
                    lint_retry_prompt
                )
                fixed_code = self.generator._clean_code_response(lint_retry_response)

                final_lint = self.generator._validate_generated_code(fixed_code)
                if final_lint:
                    logger.error(
                        f"Retry: lint failed after retry for {wrapper_id}: {final_lint}"
                    )
                    await self._update_wrapper_status(
                        wrapper_id,
                        WrapperStatus.ERROR,
                        error_message=f"Auto-retry {current_retry + 1} failed: lint errors after fix",
                    )
                    await db.generated_wrappers.update_one(
                        {"wrapper_id": wrapper_id},
                        {"$set": {"retry_count": current_retry + 1}},
                    )
                    return False

            # Save fixed code
            self.generator.save_wrapper(fixed_code, wrapper_id)

            # Save retry trace
            self.generator.debug_logger.log_generation_trace(
                wrapper_id,
                {
                    "type": "runtime_retry",
                    "retry_number": current_retry + 1,
                    "prompt": retry_prompt,
                    "fixed_code_length": len(fixed_code),
                    "error_log_length": len(error_log),
                },
            )

            # Update DB: increment retry_count, set status back to executing, store new code
            await db.generated_wrappers.update_one(
                {"wrapper_id": wrapper_id},
                {
                    "$set": {
                        "retry_count": current_retry + 1,
                        "generated_code": fixed_code,
                        "status": WrapperStatus.EXECUTING.value,
                        "updated_at": datetime.utcnow(),
                        "error_message": None,
                    },
                    "$push": {
                        "execution_log": f"Auto-retry {current_retry + 1} at {datetime.utcnow()}"
                    },
                },
            )

            # Restart execution
            execution_result = await self._runner.execute_wrapper(wrapper)

            if not execution_result.success:
                logger.error(
                    f"Retry: failed to restart wrapper {wrapper_id}: {execution_result.message}"
                )
                await self._update_wrapper_status(
                    wrapper_id,
                    WrapperStatus.ERROR,
                    error_message=f"Auto-retry {current_retry + 1}: process start failed",
                )
                return False

            logger.info(
                f"Retry {current_retry + 1}/{max_retries}: wrapper {wrapper_id} restarted successfully"
            )
            return True

        except Exception as e:
            logger.error(
                f"Retry: unexpected error for wrapper {wrapper_id}: {e}",
                exc_info=True,
            )
            await self._update_wrapper_status(
                wrapper_id,
                WrapperStatus.ERROR,
                error_message=f"Auto-retry failed: {str(e)}",
            )
            return False

    async def restart_executing_wrappers(self):
        """Restart continuous wrappers that were executing before service restart.

        Also re-triggers retries for wrappers stuck in 'retrying' status
        (the asyncio task was lost when the service restarted).
        Skips wrappers whose processes were already adopted from OS by the process manager.
        """
        try:
            # Fail wrappers orphaned mid-generation by a service restart.
            # Their consumer task was killed — requeueing isn't safe because
            # partial state (resource row, file) already exists.
            error_timestamp = datetime.utcnow()
            orphaned = await db.generated_wrappers.update_many(
                {"status": WrapperStatus.GENERATING.value},
                {
                    "$set": {
                        "status": WrapperStatus.ERROR.value,
                        "error_message": "Wrapper generation interrupted by service restart",
                        "updated_at": error_timestamp,
                        "completed_at": error_timestamp,
                    }
                },
            )
            if orphaned.modified_count:
                logger.info(
                    f"Marked {orphaned.modified_count} orphaned wrapper(s) as ERROR after restart"
                )

            # Pick up wrappers in 'retrying' that lost their retry task on restart
            retrying_wrappers = await db.generated_wrappers.find(
                {"status": "retrying"}
            ).to_list(length=None)
            for wrapper_doc in retrying_wrappers:
                wid = wrapper_doc.get("wrapper_id", "unknown")
                logger.info(f"Re-scheduling interrupted retry for wrapper {wid}")
                asyncio.create_task(self.retry_failed_wrapper(wid))

            executing_wrappers = await db.generated_wrappers.find(
                {"status": "executing"}
            ).to_list(length=None)

            if not executing_wrappers and not retrying_wrappers:
                logger.info("No executing/retrying wrappers found to restart")
                return

            if not executing_wrappers:
                return

            restarted_count = 0
            already_running_count = 0

            for wrapper_doc in executing_wrappers:
                try:
                    wrapper = GeneratedWrapper(**wrapper_doc)

                    if wrapper.source_type.value != "API":
                        continue

                    # Skip if process was already adopted from OS
                    if await self.is_wrapper_actively_executing(wrapper.wrapper_id):
                        logger.info(
                            f"Wrapper {wrapper.wrapper_id} already running, skipping restart"
                        )
                        already_running_count += 1
                        continue

                    wrapper_file_path = (
                        f"/app/generated_wrappers/{wrapper.wrapper_id}.py"
                    )
                    if not os.path.exists(wrapper_file_path):
                        await self._update_wrapper_status(
                            wrapper.wrapper_id,
                            WrapperStatus.ERROR,
                            "Wrapper file not found after service restart",
                        )
                        continue

                    hwm = (
                        wrapper.high_water_mark.isoformat()
                        if wrapper.high_water_mark
                        else None
                    )
                    lwm = (
                        wrapper.low_water_mark.isoformat()
                        if wrapper.low_water_mark
                        else None
                    )
                    phase = wrapper.phase.value if wrapper.phase else None

                    if hwm:
                        logger.info(
                            f"Restarting wrapper {wrapper.wrapper_id} phase={phase} hwm={hwm} lwm={lwm}"
                        )
                    else:
                        logger.info(
                            f"Restarting wrapper {wrapper.wrapper_id} (no checkpoint, full historical)"
                        )

                    execution_result = await self._runner.execute_wrapper(
                        wrapper,
                        resume_phase=phase,
                        resume_high_water_mark=hwm,
                        resume_low_water_mark=lwm,
                    )

                    if execution_result.success:
                        restarted_count += 1

                except (FileNotFoundError, subprocess.SubprocessError) as e:
                    logger.error(
                        f"File/Process error restarting wrapper {wrapper_doc.get('wrapper_id', 'unknown')}: {e}"
                    )
                except (ValueError, KeyError) as e:
                    logger.error(
                        f"Configuration error restarting wrapper {wrapper_doc.get('wrapper_id', 'unknown')}: {e}"
                    )

            logger.info(
                f"Restarted {restarted_count} wrappers, {already_running_count} already running"
            )

        except (OperationFailure, ConnectionError) as e:
            logger.error(
                f"Database/Connection error during restart of executing wrappers: {e}"
            )


def create_wrapper_service() -> WrapperService:
    """Factory function to create WrapperService with proper dependencies."""
    runner = ProcessWrapperRunner(wrapper_process_manager)

    generator = WrapperGenerator(
        gemini_api_key=settings.GEMINI_API_KEY,
        rabbitmq_url=settings.DATA_RABBITMQ_URL,
        debug_mode=settings.WRAPPER_GENERATION_DEBUG_MODE,
        debug_dir="/app/prompts",
        model_name=settings.GEMINI_MODEL_NAME,
    )

    return WrapperService(runner=runner, generator=generator)


# Global instance
wrapper_service = create_wrapper_service()


# Consumer function using the decorator pattern
@consumer("wrapper_creation_queue")
async def process_wrapper_creation_message(
    message: aio_pika.abc.AbstractIncomingMessage,
):
    """Process wrapper creation messages from the queue"""
    async with message.process():
        try:
            message_data = json.loads(message.body.decode())
            await wrapper_service.process_wrapper_creation(message_data)
            logger.info(
                f"Successfully processed wrapper creation task {message_data['wrapper_id']}"
            )
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Invalid message format in wrapper creation queue: {e}")
            raise
        except (OperationFailure, ConnectionError, TimeoutError) as e:
            logger.error(f"Database/Connection error processing wrapper creation: {e}")
            raise
