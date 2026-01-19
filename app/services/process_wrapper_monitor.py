import os
import subprocess
import logging
from datetime import datetime
from typing import Dict, Any, List
from services.abc.wrapper_monitor import WrapperMonitor, WrapperHealthStatus
from services.wrapper_process_manager import WrapperProcessManager
from schemas.wrapper import GeneratedWrapper
from dependencies.database import db

logger = logging.getLogger(__name__)

class ProcessWrapperMonitor(WrapperMonitor):
    """Monitor implementation for process-based wrapper execution"""

    def __init__(self, process_manager: WrapperProcessManager):
        """Initialize ProcessWrapperMonitor with a process manager

        Args:
            process_manager: WrapperProcessManager instance for process lifecycle management
        """
        self.process_manager = process_manager

    async def start_monitoring(self, wrapper: GeneratedWrapper) -> bool:
        """Start monitoring a process-based wrapper

        Args:
            wrapper: GeneratedWrapper instance to monitor

        Returns:
            bool: True if monitoring started successfully
        """
        try:
            success = await self.process_manager.start_wrapper_process(wrapper.wrapper_id)

            if success:
                logger.info(f"Started monitoring process wrapper {wrapper.wrapper_id}")
                return True
            else:
                logger.error(f"Failed to start monitoring process wrapper {wrapper.wrapper_id}")
                return False

        except (OSError, PermissionError) as e:
            logger.error(f"System error starting process wrapper monitoring for {wrapper.wrapper_id}: {e}")
            return False
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Connection error starting process wrapper monitoring for {wrapper.wrapper_id}: {e}")
            return False
        except ValueError as e:
            logger.error(f"Invalid configuration for wrapper {wrapper.wrapper_id}: {e}")
            return False

    async def stop_wrapper(self, wrapper_id: str) -> bool:
        """Stop a process wrapper (internal process termination logic)

        Args:
            wrapper_id: ID of wrapper to stop

        Returns:
            bool: True if wrapper stopped successfully
        """
        try:
            if wrapper_id not in self.process_manager.running_processes:
                logger.info(f"Process wrapper {wrapper_id} is not running")
                return True

            wrapper_process = self.process_manager.running_processes[wrapper_id]

            if wrapper_process.process.poll() is None:
                logger.info(f"Stopping process wrapper {wrapper_id}")
                wrapper_process.process.terminate()

                try:
                    wrapper_process.process.wait(timeout=10)
                    logger.info(f"Process wrapper {wrapper_id} terminated gracefully")
                except subprocess.TimeoutExpired:
                    logger.warning(f"Force killing process wrapper {wrapper_id}")
                    wrapper_process.process.kill()
                    wrapper_process.process.wait()

            await self.process_manager._cleanup_process(wrapper_id)

            await db.generated_wrappers.update_one(
                {"wrapper_id": wrapper_id},
                {
                    "$set": {"status": "completed"},
                    "$push": {"execution_log": f"Stopped manually at {datetime.utcnow()}"}
                }
            )

            logger.info(f"Successfully stopped process wrapper {wrapper_id}")
            return True

        except ProcessLookupError as e:
            logger.warning(f"Process {wrapper_id} already terminated: {e}")
            await self.process_manager._cleanup_process(wrapper_id)
            return True
        except subprocess.TimeoutExpired as e:
            logger.error(f"Timeout stopping process wrapper {wrapper_id}: {e}")
            try:
                wrapper_process = self.process_manager.running_processes.get(wrapper_id)
                if wrapper_process:
                    wrapper_process.process.kill()
                    logger.warning(f"Force killed process wrapper {wrapper_id} after timeout")
            except (ProcessLookupError, OSError) as cleanup_error:
                logger.error(f"Failed to force kill process {wrapper_id}: {cleanup_error}", exc_info=True)
            return False
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Database connection error while stopping wrapper {wrapper_id}: {e}")
            return False
        except OSError as e:
            logger.error(f"System error stopping process wrapper {wrapper_id}: {e}")
            return False

    async def get_health_status(self, wrapper_id: str) -> str:
        """Get current health status of process wrapper

        Args:
            wrapper_id: ID of wrapper to check

        Returns:
            str: Health status (HEALTHY, STALLED, DEGRADED, CRASHED, UNKNOWN)
        """
        try:
            if wrapper_id not in self.process_manager.running_processes:
                await self._update_crashed_wrapper_status(wrapper_id, "Process not found in running processes")
                return WrapperHealthStatus.CRASHED

            wrapper_process = self.process_manager.running_processes[wrapper_id]

            if wrapper_process.process.poll() is not None:
                exit_code = wrapper_process.process.poll()
                await self._append_exit_info_to_logs(wrapper_id, exit_code)
                await self._update_crashed_wrapper_status(wrapper_id, f"Process terminated with exit code {exit_code}")
                await self.process_manager._cleanup_process(wrapper_id)
                return WrapperHealthStatus.CRASHED

            return WrapperHealthStatus.HEALTHY

        except ProcessLookupError as e:
            logger.warning(f"Process {wrapper_id} lookup failed: {e}")
            await self._update_crashed_wrapper_status(wrapper_id, "Process not found")
            return WrapperHealthStatus.CRASHED
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Database connection error checking health for {wrapper_id}: {e}")
            return WrapperHealthStatus.UNKNOWN
        except (OSError, ValueError) as e:
            logger.error(f"Error checking health status for wrapper {wrapper_id}: {e}")
            return WrapperHealthStatus.UNKNOWN

    async def _update_crashed_wrapper_status(self, wrapper_id: str, reason: str):
        """Update database status for crashed wrapper with actual crash logs"""
        try:
            from datetime import datetime

            crash_logs = await self._get_crash_logs(wrapper_id)

            if crash_logs:
                error_message = f"Process crashed. Last logs:\n{crash_logs}"
            else:
                error_message = f"Process crashed: {reason}. No logs available."

            wrapper_doc = await db.generated_wrappers.find_one({"wrapper_id": wrapper_id})
            if wrapper_doc and wrapper_doc.get("status") in ["executing", "generating"]:
                await db.generated_wrappers.update_one(
                    {"wrapper_id": wrapper_id},
                    {
                        "$set": {
                            "status": "error",
                            "error_message": error_message,
                            "completed_at": datetime.utcnow(),
                            "updated_at": datetime.utcnow()
                        },
                        "$push": {
                            "execution_log": f"Process crashed at {datetime.utcnow()}: {reason}"
                        }
                    }
                )
                logger.info(f"Updated database status for crashed wrapper {wrapper_id}: {reason}")

        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Database connection error updating crashed wrapper {wrapper_id}: {e}")
        except ValueError as e:
            logger.error(f"Invalid data updating crashed wrapper {wrapper_id}: {e}")
        except OSError as e:
            logger.error(f"System error updating crashed wrapper {wrapper_id}: {e}")

    async def _get_crash_logs(self, wrapper_id: str) -> str:
        """Get the last few lines of logs before crash"""
        try:
            crash_logs = []

            stderr_file = f"/app/wrapper_logs/{wrapper_id}_stderr.log"
            if os.path.exists(stderr_file):
                try:
                    with open(stderr_file, 'r') as f:
                        lines = f.readlines()
                        if lines:
                            last_stderr = lines[-5:]
                            crash_logs.extend([f"[STDERR] {line.strip()}" for line in last_stderr if line.strip()])
                except IOError:
                    pass

            stdout_file = f"/app/wrapper_logs/{wrapper_id}_stdout.log"
            if os.path.exists(stdout_file):
                try:
                    with open(stdout_file, 'r') as f:
                        lines = f.readlines()
                        if lines:
                            last_stdout = lines[-3:]
                            crash_logs.extend([f"[STDOUT] {line.strip()}" for line in last_stdout if line.strip()])
                except IOError:
                    pass

            return "\n".join(crash_logs) if crash_logs else ""

        except (IOError, OSError, PermissionError) as e:
            logger.error(f"File system error reading crash logs for wrapper {wrapper_id}: {e}")
            return ""
        except UnicodeDecodeError as e:
            logger.error(f"Encoding error reading crash logs for wrapper {wrapper_id}: {e}")
            return ""

    async def get_monitoring_details(self, wrapper_id: str) -> Dict[str, Any]:
        """Get process-specific monitoring details

        Args:
            wrapper_id: ID of wrapper to get details for

        Returns:
            Dict[str, Any]: Process-specific monitoring data
        """
        try:
            if wrapper_id not in self.process_manager.running_processes:
                return {"status": "not_running"}

            wrapper_process = self.process_manager.running_processes[wrapper_id]

            details = {
                "process_id": wrapper_process.process.pid,
                "exit_code": wrapper_process.process.poll(),
                "uptime_seconds": (datetime.utcnow() - wrapper_process.started_at).total_seconds(),
                "started_at": wrapper_process.started_at.isoformat(),
                "last_health_check": wrapper_process.last_health_check.isoformat(),
                "log_files": {
                    "stdout": f"/app/wrapper_logs/{wrapper_id}_stdout.log",
                    "stderr": f"/app/wrapper_logs/{wrapper_id}_stderr.log"
                }
            }

            return details

        except ProcessLookupError as e:
            logger.warning(f"Process {wrapper_id} lookup failed in monitoring details: {e}")
            return {"status": "process_not_found", "error": str(e)}
        except (AttributeError, ValueError) as e:
            logger.error(f"Invalid process data for wrapper {wrapper_id}: {e}")
            return {"error": str(e)}
        except OSError as e:
            logger.error(f"System error getting monitoring details for wrapper {wrapper_id}: {e}")
            return {"error": str(e)}

    async def get_logs(self, wrapper_id: str, limit: int = 100) -> List[str]:
        """Get wrapper logs from stdout/stderr log files

        Args:
            wrapper_id: ID of wrapper to get logs for
            limit: Maximum number of log lines to return

        Returns:
            List[str]: Log lines from log files
        """
        try:
            logs = []

            stdout_file = f"/app/wrapper_logs/{wrapper_id}_stdout.log"
            if os.path.exists(stdout_file):
                try:
                    with open(stdout_file, 'r') as f:
                        stdout_lines = f.readlines()
                        logs.extend([f"[STDOUT] {line.strip()}" for line in stdout_lines if line.strip()])
                except IOError as e:
                    logger.warning(f"Failed to read stdout log for {wrapper_id}: {e}")

            stderr_file = f"/app/wrapper_logs/{wrapper_id}_stderr.log"
            if os.path.exists(stderr_file):
                try:
                    with open(stderr_file, 'r') as f:
                        stderr_lines = f.readlines()
                        logs.extend([f"[STDERR] {line.strip()}" for line in stderr_lines if line.strip()])
                except IOError as e:
                    logger.warning(f"Failed to read stderr log for {wrapper_id}: {e}")

            return logs[-limit:] if logs else []

        except (IOError, OSError, PermissionError) as e:
            logger.error(f"File system error getting logs for wrapper {wrapper_id}: {e}")
            return [f"Error reading logs: {str(e)}"]
        except UnicodeDecodeError as e:
            logger.error(f"Encoding error reading logs for wrapper {wrapper_id}: {e}")
            return [f"Error reading logs: encoding issue"]
        except ValueError as e:
            logger.error(f"Invalid limit parameter for wrapper {wrapper_id}: {e}")
            return [f"Error reading logs: {str(e)}"]

    async def is_actively_executing(self, wrapper_id: str) -> bool:
        """Check if process wrapper is actively executing

        Args:
            wrapper_id: ID of wrapper to check

        Returns:
            bool: True if wrapper process is running
        """
        try:
            if wrapper_id not in self.process_manager.running_processes:
                return False

            wrapper_process = self.process_manager.running_processes[wrapper_id]

            return wrapper_process.process.poll() is None

        except ProcessLookupError as e:
            logger.warning(f"Process {wrapper_id} lookup failed in execution check: {e}")
            return False
        except (AttributeError, ValueError) as e:
            logger.error(f"Invalid process data checking execution for {wrapper_id}: {e}")
            return False
        except OSError as e:
            logger.error(f"System error checking if wrapper {wrapper_id} is executing: {e}")
            return False