import asyncio
import subprocess
import tempfile
import os
import signal
import logging
from typing import Dict, Optional, List
from datetime import datetime
from bson.objectid import ObjectId
from pymongo.errors import OperationFailure
from dependencies.database import db
from schemas.wrapper import WrapperStatus, GeneratedWrapper, DataSourceConfig
from config import settings

logger = logging.getLogger(__name__)

class WrapperProcess:
    def __init__(self, wrapper_id: str, process: subprocess.Popen, temp_file_path: str):
        self.wrapper_id = wrapper_id
        self.process = process
        self.temp_file_path = temp_file_path
        self.started_at = datetime.utcnow()
        self.last_health_check = datetime.utcnow()

class WrapperProcessManager:
    def __init__(self):
        self.running_processes: Dict[str, WrapperProcess] = {}
        self.monitoring_task: Optional[asyncio.Task] = None
    
    async def start_monitoring(self):
        """Start background monitoring of wrapper processes"""
        if self.monitoring_task and not self.monitoring_task.done():
            return
        
        self.monitoring_task = asyncio.create_task(self._monitor_processes())
        logger.info("Wrapper process monitoring started")
    
    async def stop_monitoring(self):
        """Stop background monitoring"""
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        logger.info("Wrapper process monitoring stopped")
    
    async def _monitor_processes(self):
        """Background task to monitor wrapper processes"""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                await self._check_process_health()
            except asyncio.CancelledError:
                break
            except (OSError, OperationFailure) as e:
                logger.error(f"Error in process monitoring: {e}", exc_info=True)

    async def _append_exit_info_to_logs(self, wrapper_id: str, exit_code: int):
        """Append process exit information to log files without deleting existing logs"""
        try:
            timestamp = datetime.utcnow().isoformat()

            if exit_code == 0:
                exit_message = f"[{timestamp}] Wrapper {wrapper_id}: Process completed successfully (exit code 0)\n"
            else:
                exit_message = f"[{timestamp}] Wrapper {wrapper_id}: Process terminated with exit code {exit_code}\n"

            # Ensure log directory exists
            log_dir = "/app/wrapper_logs"
            os.makedirs(log_dir, exist_ok=True)

            # Append to stdout log (stderr would only get this if there was an error)
            stdout_log_path = f"{log_dir}/{wrapper_id}_stdout.log"

            try:
                with open(stdout_log_path, "a", encoding="utf-8") as log_file:
                    log_file.write(exit_message)
                    log_file.flush()  # Ensure immediate write
            except IOError as e:
                logger.warning(f"Failed to append exit info to {stdout_log_path}: {e}")

            logger.info(f"Appended exit information (code {exit_code}) to log file for wrapper {wrapper_id}")

        except (IOError, OSError) as e:
            logger.error(f"Error appending exit info to logs for wrapper {wrapper_id}: {e}", exc_info=True)

    async def _check_process_health(self):
        """Check health of all running processes"""
        dead_processes = []

        for wrapper_id, wrapper_process in self.running_processes.items():
            try:
                # Check if process is still running
                exit_code = wrapper_process.process.poll()
                if exit_code is not None:
                    # Process has terminated
                    dead_processes.append(wrapper_id)

                    logger.info(f"Wrapper {wrapper_id} process terminated with exit code {exit_code}")

                    # Append exit information to log files (never delete logs!)
                    await self._append_exit_info_to_logs(wrapper_id, exit_code)

                    await db.generated_wrappers.update_one(
                        {"wrapper_id": wrapper_id},
                        {
                            "$set": {"status": WrapperStatus.COMPLETED if exit_code == 0 else WrapperStatus.ERROR},
                            "$push": {"execution_log": f"Process terminated at {datetime.utcnow()} with exit code {exit_code}"}
                        }
                    )
                    
                else:
                    # Process is running, update health check
                    wrapper_process.last_health_check = datetime.utcnow()

            except (OSError, OperationFailure) as e:
                logger.error(f"Health check failed for wrapper {wrapper_id}: {e}", exc_info=True)
        
        # Clean up dead processes
        for wrapper_id in dead_processes:
            await self._cleanup_process(wrapper_id)
    
    async def start_wrapper_process(self, wrapper_id: str, wrapper_file_path: str, source_config: DataSourceConfig, skip_historical: bool = False) -> bool:
        """ Start a wrapper in a separate process
            location can be a file path, an url, etc.
        """
        try:
            # Check if already running
            if wrapper_id in self.running_processes:
                logger.warning(f"Wrapper {wrapper_id} is already running")
                return False

            # Check if wrapper file exists
            if not os.path.exists(wrapper_file_path):
                logger.error(f"Wrapper file not found: {wrapper_file_path}")
                return False
            
            # Start process
            env = os.environ.copy()
            env['DATA_RABBITMQ_URL'] = settings.DATA_RABBITMQ_URL
            env['AMQP_URL'] = settings.DATA_RABBITMQ_URL  # This is what wrappers actually look for
            env['WRAPPER_ID'] = wrapper_id
            env['PYTHONPATH'] = '/app/wrapper_runtime/shared:' + env.get('PYTHONPATH', '')

            # Create log files for wrapper output
            log_dir = "/app/wrapper_logs"
            os.makedirs(log_dir, exist_ok=True)
            stdout_log = open(f"{log_dir}/{wrapper_id}_stdout.log", "w")
            stderr_log = open(f"{log_dir}/{wrapper_id}_stderr.log", "w")

            # Build command arguments
            cmd_args = ['python', '-u', wrapper_file_path, wrapper_id, source_config.model_dump_json()]
            if skip_historical:
                cmd_args.append('--skip-historical')

            process = subprocess.Popen(
                cmd_args,
                env=env,
                stdout=stdout_log,
                stderr=stderr_log,
                preexec_fn=os.setsid  # Create new process group
            )

            # Store process info
            wrapper_process = WrapperProcess(wrapper_id, process, wrapper_file_path)
            self.running_processes[wrapper_id] = wrapper_process

            # Update database status
            await db.generated_wrappers.update_one(
                {"wrapper_id": wrapper_id},
                {
                    "$set": {"status": WrapperStatus.EXECUTING.value},
                    "$push": {"execution_log": f"Started process at {datetime.utcnow()}"}
                }
            )
            
            logger.info(f"Started wrapper {wrapper_id} in process {process.pid}")
            return True

        except (OSError, IOError, ValueError, OperationFailure) as e:
            logger.error(f"Failed to start wrapper process {wrapper_id}: {e}", exc_info=True)
            return False
    
    async def _cleanup_process(self, wrapper_id: str):
        """Properly clean up process resources"""
        if wrapper_id not in self.running_processes:
            logger.warning(f"Attempted to cleanup non-existent process {wrapper_id}")
            return

        wrapper_process = self.running_processes[wrapper_id]

        try:
            # Ensure process is terminated
            if wrapper_process.process.poll() is None:
                logger.info(f"Terminating still-running process for wrapper {wrapper_id}")
                wrapper_process.process.terminate()
                try:
                    # Wait for graceful termination
                    wrapper_process.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # Force kill if graceful termination fails
                    logger.warning(f"Force killing process for wrapper {wrapper_id}")
                    wrapper_process.process.kill()
                    wrapper_process.process.wait()

            # Note: Keep wrapper files and log files for debugging

            # Remove from tracking
            del self.running_processes[wrapper_id]

            logger.info(f"Successfully cleaned up process resources for wrapper {wrapper_id}")

        except (ProcessLookupError, OSError, subprocess.SubprocessError) as e:
            logger.error(f"Error during process cleanup for wrapper {wrapper_id}: {e}", exc_info=True)
            if wrapper_id in self.running_processes:
                del self.running_processes[wrapper_id]

# Global process manager instance
wrapper_process_manager = WrapperProcessManager()