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
            # For process wrappers, monitoring starts when the process is started
            # The process manager handles the actual process creation and tracking
            success = await self.process_manager.start_wrapper_process(wrapper.wrapper_id)

            if success:
                logger.info(f"Started monitoring process wrapper {wrapper.wrapper_id}")
                return True
            else:
                logger.error(f"Failed to start monitoring process wrapper {wrapper.wrapper_id}")
                return False

        except Exception as e:
            logger.error(f"Error starting process wrapper monitoring for {wrapper.wrapper_id}: {e}")
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

            # Graceful shutdown: SIGTERM → wait → SIGKILL
            if wrapper_process.process.poll() is None:
                logger.info(f"Stopping process wrapper {wrapper_id}")
                wrapper_process.process.terminate()

                try:
                    # Wait for graceful termination
                    wrapper_process.process.wait(timeout=10)
                    logger.info(f"Process wrapper {wrapper_id} terminated gracefully")
                except subprocess.TimeoutExpired:
                    # Force kill if graceful termination fails
                    logger.warning(f"Force killing process wrapper {wrapper_id}")
                    wrapper_process.process.kill()
                    wrapper_process.process.wait()

            # Clean up process resources
            await self.process_manager._cleanup_process(wrapper_id)

            # Update database status
            await db.generated_wrappers.update_one(
                {"wrapper_id": wrapper_id},
                {
                    "$set": {"status": "completed"},
                    "$push": {"execution_log": f"Stopped manually at {datetime.utcnow()}"}
                }
            )

            logger.info(f"Successfully stopped process wrapper {wrapper_id}")
            return True

        except Exception as e:
            logger.error(f"Error stopping process wrapper {wrapper_id}: {e}")
            return False

    async def get_health_status(self, wrapper_id: str) -> str:
        """Get current health status of process wrapper

        Args:
            wrapper_id: ID of wrapper to check

        Returns:
            str: Health status (HEALTHY, STALLED, DEGRADED, CRASHED, UNKNOWN)
        """
        try:
            # Check if wrapper is not in running processes
            if wrapper_id not in self.process_manager.running_processes:
                # Update database status if it's incorrectly showing as executing
                await self._update_crashed_wrapper_status(wrapper_id, "Process not found in running processes")
                return WrapperHealthStatus.CRASHED

            wrapper_process = self.process_manager.running_processes[wrapper_id]

            # Check if process is still running
            if wrapper_process.process.poll() is not None:
                # Process has terminated - append exit info to logs and update database
                exit_code = wrapper_process.process.poll()
                await self._append_exit_info_to_logs(wrapper_id, exit_code)
                await self._update_crashed_wrapper_status(wrapper_id, f"Process terminated with exit code {exit_code}")
                await self.process_manager._cleanup_process(wrapper_id)
                return WrapperHealthStatus.CRASHED

            return WrapperHealthStatus.HEALTHY

        except Exception as e:
            logger.error(f"Error checking health status for wrapper {wrapper_id}: {e}")
            return WrapperHealthStatus.UNKNOWN

    async def _update_crashed_wrapper_status(self, wrapper_id: str, reason: str):
        """Update database status for crashed wrapper with actual crash logs"""
        try:
            from datetime import datetime

            # Try to get actual crash logs first
            crash_logs = await self._get_crash_logs(wrapper_id)

            # Build comprehensive error message
            if crash_logs:
                error_message = f"Process crashed. Last logs:\n{crash_logs}"
            else:
                error_message = f"Process crashed: {reason}. No logs available."

            # Check current status in database
            wrapper_doc = await db.generated_wrappers.find_one({"wrapper_id": wrapper_id})
            if wrapper_doc and wrapper_doc.get("status") in ["executing", "generating"]:
                # Update status to error only if it's currently showing as active
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

        except Exception as e:
            logger.error(f"Failed to update database status for crashed wrapper {wrapper_id}: {e}")

    async def _get_crash_logs(self, wrapper_id: str) -> str:
        """Get the last few lines of logs before crash"""
        try:
            crash_logs = []

            # Try to read stderr log (most likely to contain error info)
            stderr_file = f"/app/wrapper_logs/{wrapper_id}_stderr.log"
            if os.path.exists(stderr_file):
                try:
                    with open(stderr_file, 'r') as f:
                        lines = f.readlines()
                        if lines:
                            # Get last 5 lines of stderr
                            last_stderr = lines[-5:]
                            crash_logs.extend([f"[STDERR] {line.strip()}" for line in last_stderr if line.strip()])
                except IOError:
                    pass

            # Try to read stdout log for additional context
            stdout_file = f"/app/wrapper_logs/{wrapper_id}_stdout.log"
            if os.path.exists(stdout_file):
                try:
                    with open(stdout_file, 'r') as f:
                        lines = f.readlines()
                        if lines:
                            # Get last 3 lines of stdout
                            last_stdout = lines[-3:]
                            crash_logs.extend([f"[STDOUT] {line.strip()}" for line in last_stdout if line.strip()])
                except IOError:
                    pass

            return "\n".join(crash_logs) if crash_logs else ""

        except Exception as e:
            logger.error(f"Error reading crash logs for wrapper {wrapper_id}: {e}")
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

            # Process is running - no additional resource monitoring needed

            return details

        except Exception as e:
            logger.error(f"Error getting monitoring details for wrapper {wrapper_id}: {e}")
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

            # Read stdout log
            stdout_file = f"/app/wrapper_logs/{wrapper_id}_stdout.log"
            if os.path.exists(stdout_file):
                try:
                    with open(stdout_file, 'r') as f:
                        stdout_lines = f.readlines()
                        logs.extend([f"[STDOUT] {line.strip()}" for line in stdout_lines if line.strip()])
                except IOError as e:
                    logger.warning(f"Failed to read stdout log for {wrapper_id}: {e}")

            # Read stderr log
            stderr_file = f"/app/wrapper_logs/{wrapper_id}_stderr.log"
            if os.path.exists(stderr_file):
                try:
                    with open(stderr_file, 'r') as f:
                        stderr_lines = f.readlines()
                        logs.extend([f"[STDERR] {line.strip()}" for line in stderr_lines if line.strip()])
                except IOError as e:
                    logger.warning(f"Failed to read stderr log for {wrapper_id}: {e}")

            # Return most recent entries
            return logs[-limit:] if logs else []

        except Exception as e:
            logger.error(f"Error getting logs for wrapper {wrapper_id}: {e}")
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

            # Check if process is still running
            return wrapper_process.process.poll() is None

        except Exception as e:
            logger.error(f"Error checking if wrapper {wrapper_id} is executing: {e}")
            return False