import asyncio
import subprocess
import os
import re
import signal
import logging
from typing import Dict, Optional, List
from datetime import datetime
from pymongo.errors import OperationFailure
from dependencies.database import db
from schemas.wrapper import WrapperStatus, DataSourceConfig
from config import settings

logger = logging.getLogger(__name__)

WRAPPER_DIR = "/app/generated_wrappers"


class WrapperProcess:
    def __init__(self, wrapper_id: str, process: subprocess.Popen, temp_file_path: str):
        self.wrapper_id = wrapper_id
        self.process = process
        self.temp_file_path = temp_file_path
        self.started_at = datetime.utcnow()
        self.last_health_check = datetime.utcnow()
        # Whether this wrapper is holding a concurrency slot from the manager's
        # semaphore. Set after a successful acquire in start_wrapper_process.
        self.holds_slot = False


class _AdoptedProcess:
    """Minimal wrapper around an OS PID not started by this manager"""

    def __init__(self, pid: int):
        self._pid = pid

    def poll(self) -> Optional[int]:
        try:
            os.kill(self._pid, 0)
            return None
        except ProcessLookupError:
            return -1
        except PermissionError:
            return None

    def terminate(self):
        os.kill(self._pid, signal.SIGTERM)

    def kill(self):
        os.kill(self._pid, signal.SIGKILL)

    def wait(self, timeout=None):
        pass

    @property
    def pid(self):
        return self._pid


class WrapperProcessManager:
    def __init__(self):
        self.running_processes: Dict[str, WrapperProcess] = {}
        self.monitoring_task: Optional[asyncio.Task] = None
        # Cap concurrent subprocesses to protect the service from OOM
        # when many wrappers are created at once.
        max_concurrent = max(1, int(settings.MAX_CONCURRENT_WRAPPERS))
        self._start_semaphore = asyncio.Semaphore(max_concurrent)
        self._max_concurrent = max_concurrent
        timeout_cfg = int(settings.WRAPPER_EXECUTION_TIMEOUT_SECONDS)
        self._execution_timeout = timeout_cfg if timeout_cfg > 0 else None

    async def start_monitoring(self):
        """Start background monitoring of wrapper processes"""
        if self.monitoring_task and not self.monitoring_task.done():
            return

        self._adopt_orphaned_processes()

        self.monitoring_task = asyncio.create_task(self._monitor_processes())
        logger.info("Wrapper process monitoring started")

    def _adopt_orphaned_processes(self):
        """Scan /proc for wrapper processes not tracked by this manager.

        This handles uvicorn --reload or service restart where child processes
        survive but the in-memory dict is lost.
        """
        adopted = 0
        try:
            for entry in os.listdir("/proc"):
                if not entry.isdigit():
                    continue
                pid = int(entry)
                try:
                    cmdline_path = f"/proc/{pid}/cmdline"
                    with open(cmdline_path, "rb") as f:
                        cmdline = f.read().decode("utf-8", errors="replace")

                    if WRAPPER_DIR not in cmdline:
                        continue

                    match = re.search(r"/([0-9a-f\-]{36})\.py", cmdline)
                    if not match:
                        continue

                    wrapper_id = match.group(1)
                    if wrapper_id in self.running_processes:
                        continue

                    wrapper_file = f"{WRAPPER_DIR}/{wrapper_id}.py"
                    process = _AdoptedProcess(pid)
                    wp = WrapperProcess(wrapper_id, process, wrapper_file)
                    # Adopted processes already exist; they don't consume a
                    # new-spawn slot. holds_slot stays False so cleanup won't
                    # release an uncounted slot.
                    self.running_processes[wrapper_id] = wp
                    adopted += 1
                    logger.info(
                        f"Adopted orphaned wrapper process {wrapper_id} (pid {pid})"
                    )

                except (IOError, OSError, ValueError):
                    continue
        except OSError as e:
            logger.error(f"Failed to scan /proc for orphaned processes: {e}")

        if adopted:
            logger.info(f"Adopted {adopted} orphaned wrapper process(es)")

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
            except Exception as e:
                # Catch-all so the monitor survives any unexpected error
                # (e.g. transient DB / rabbit / filesystem issues).
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

            logger.info(
                f"Appended exit information (code {exit_code}) to log file for wrapper {wrapper_id}"
            )

        except (IOError, OSError) as e:
            logger.error(
                f"Error appending exit info to logs for wrapper {wrapper_id}: {e}",
                exc_info=True,
            )

    async def _check_process_health(self):
        """Check health of all running processes"""
        dead_processes = []

        # Snapshot to tolerate concurrent mutation of running_processes.
        for wrapper_id, wrapper_process in list(self.running_processes.items()):
            try:
                # Check if process is still running
                exit_code = wrapper_process.process.poll()
                if exit_code is not None:
                    # Process has terminated
                    dead_processes.append(wrapper_id)

                    logger.info(
                        f"Wrapper {wrapper_id} process terminated with exit code {exit_code}"
                    )

                    # Append exit information to log files (never delete logs!)
                    await self._append_exit_info_to_logs(wrapper_id, exit_code)

                    if exit_code == 0:
                        await db.generated_wrappers.update_one(
                            {"wrapper_id": wrapper_id},
                            {
                                "$set": {"status": WrapperStatus.COMPLETED},
                                "$push": {
                                    "execution_log": f"Process completed at {datetime.utcnow()} with exit code 0"
                                },
                            },
                        )
                    else:
                        await self._handle_crashed_wrapper(wrapper_id, exit_code)

                else:
                    # Process is running. Enforce execution timeout if set.
                    now = datetime.utcnow()
                    wrapper_process.last_health_check = now
                    if self._execution_timeout is not None:
                        runtime = (now - wrapper_process.started_at).total_seconds()
                        if runtime > self._execution_timeout:
                            logger.warning(
                                f"Wrapper {wrapper_id} exceeded execution timeout "
                                f"({runtime:.0f}s > {self._execution_timeout}s); terminating"
                            )
                            try:
                                wrapper_process.process.terminate()
                            except (OSError, ProcessLookupError) as e:
                                logger.warning(
                                    f"Terminate failed for wrapper {wrapper_id}: {e}"
                                )
                            await db.generated_wrappers.update_one(
                                {"wrapper_id": wrapper_id},
                                {
                                    "$set": {
                                        "status": WrapperStatus.ERROR.value,
                                        "error_message": (
                                            f"Execution timeout after {self._execution_timeout}s"
                                        ),
                                        "updated_at": now,
                                        "completed_at": now,
                                    },
                                    "$push": {
                                        "execution_log": (
                                            f"Terminated at {now} due to execution timeout"
                                        )
                                    },
                                },
                            )
                            dead_processes.append(wrapper_id)

            except asyncio.CancelledError:
                # Preserve cancellation semantics so the monitoring task
                # stops cleanly on shutdown.
                raise
            except Exception as e:
                # Never let a single wrapper's health check failure break
                # the monitor or skip remaining wrappers.
                logger.error(
                    f"Health check failed for wrapper {wrapper_id}: {e}", exc_info=True
                )

        # Clean up dead processes
        for wrapper_id in dead_processes:
            await self._cleanup_process(wrapper_id)

    async def _handle_crashed_wrapper(self, wrapper_id: str, exit_code: int):
        """Check if a crashed wrapper should be auto-retried or marked as error."""
        max_retries = 3
        try:
            wrapper_doc = await db.generated_wrappers.find_one(
                {"wrapper_id": wrapper_id}
            )
            retry_count = (wrapper_doc or {}).get("retry_count", 0)

            if retry_count < max_retries:
                logger.info(
                    f"Wrapper {wrapper_id} crashed (exit {exit_code}), "
                    f"scheduling auto-retry {retry_count + 1}/{max_retries}"
                )
                await db.generated_wrappers.update_one(
                    {"wrapper_id": wrapper_id},
                    {
                        "$set": {"status": "retrying"},
                        "$push": {
                            "execution_log": f"Crash at {datetime.utcnow()} (exit {exit_code}), auto-retry scheduled"
                        },
                    },
                )

                # Lazy import to avoid circular dependency
                from services.wrapper_service import wrapper_service

                asyncio.create_task(wrapper_service.retry_failed_wrapper(wrapper_id))
            else:
                logger.warning(
                    f"Wrapper {wrapper_id} crashed (exit {exit_code}), "
                    f"retries exhausted ({max_retries}/{max_retries})"
                )
                await db.generated_wrappers.update_one(
                    {"wrapper_id": wrapper_id},
                    {
                        "$set": {
                            "status": WrapperStatus.ERROR,
                            "error_message": f"Failed after {max_retries} auto-retry attempts (exit code {exit_code})",
                        },
                        "$push": {
                            "execution_log": f"Process terminated at {datetime.utcnow()} with exit code {exit_code}, retries exhausted"
                        },
                    },
                )
        except (OperationFailure, ConnectionError) as e:
            logger.error(
                f"Error handling crashed wrapper {wrapper_id}: {e}", exc_info=True
            )
            await db.generated_wrappers.update_one(
                {"wrapper_id": wrapper_id},
                {
                    "$set": {"status": WrapperStatus.ERROR},
                    "$push": {
                        "execution_log": f"Process terminated at {datetime.utcnow()} with exit code {exit_code}"
                    },
                },
            )

    async def start_wrapper_process(
        self,
        wrapper_id: str,
        wrapper_file_path: str,
        source_config: DataSourceConfig,
        skip_historical: bool = False,
        resume_phase: Optional[str] = None,
        resume_high_water_mark: Optional[str] = None,
        resume_low_water_mark: Optional[str] = None,
    ) -> bool:
        """Start a wrapper in a separate process.

        Args:
            resume_phase: "historical" or "continuous" — which phase was active before stop
            resume_high_water_mark: ISO timestamp of newest data point ever sent
            resume_low_water_mark: ISO timestamp of oldest data point ever sent
        """
        slot_acquired = False
        try:
            if wrapper_id in self.running_processes:
                logger.warning(f"Wrapper {wrapper_id} is already running")
                return False

            if not os.path.exists(wrapper_file_path):
                logger.error(f"Wrapper file not found: {wrapper_file_path}")
                return False

            # Reserve a concurrency slot before spawning. Callers await this
            # naturally when the service is at capacity, providing backpressure.
            if self._start_semaphore.locked():
                logger.info(
                    f"Wrapper {wrapper_id} waiting for a concurrency slot "
                    f"(cap={self._max_concurrent})"
                )
            await self._start_semaphore.acquire()
            slot_acquired = True

            env = os.environ.copy()
            env["DATA_RABBITMQ_URL"] = settings.DATA_RABBITMQ_URL
            env["AMQP_URL"] = settings.DATA_RABBITMQ_URL
            env["WRAPPER_ID"] = wrapper_id
            env["PYTHONPATH"] = "/app/wrapper_runtime/shared:" + env.get(
                "PYTHONPATH", ""
            )
            if resume_phase:
                env["RESUME_PHASE"] = resume_phase
            if resume_high_water_mark:
                env["RESUME_HIGH_WATER_MARK"] = resume_high_water_mark
            if resume_low_water_mark:
                env["RESUME_LOW_WATER_MARK"] = resume_low_water_mark

            log_dir = "/app/wrapper_logs"
            os.makedirs(log_dir, exist_ok=True)
            stdout_log = open(f"{log_dir}/{wrapper_id}_stdout.log", "w")
            stderr_log = open(f"{log_dir}/{wrapper_id}_stderr.log", "w")

            cmd_args = [
                "python",
                "-u",
                wrapper_file_path,
                wrapper_id,
                source_config.model_dump_json(),
            ]
            if skip_historical:
                cmd_args.append("--skip-historical")

            process = subprocess.Popen(
                cmd_args,
                env=env,
                stdout=stdout_log,
                stderr=stderr_log,
                preexec_fn=os.setsid,  # Create new process group
            )

            # Store process info
            wrapper_process = WrapperProcess(wrapper_id, process, wrapper_file_path)
            wrapper_process.holds_slot = True
            self.running_processes[wrapper_id] = wrapper_process
            # Slot ownership transferred to wrapper_process; do not release
            # in the except/finally path.
            slot_acquired = False

            # Update database status
            await db.generated_wrappers.update_one(
                {"wrapper_id": wrapper_id},
                {
                    "$set": {"status": WrapperStatus.EXECUTING.value},
                    "$push": {
                        "execution_log": f"Started process at {datetime.utcnow()}"
                    },
                },
            )

            logger.info(f"Started wrapper {wrapper_id} in process {process.pid}")
            return True

        except (OSError, IOError, ValueError, OperationFailure) as e:
            logger.error(
                f"Failed to start wrapper process {wrapper_id}: {e}", exc_info=True
            )
            return False
        finally:
            # If we acquired a slot but never attached it to a running
            # process, release it to avoid leaking capacity.
            if slot_acquired:
                self._start_semaphore.release()

    async def _cleanup_process(self, wrapper_id: str):
        """Properly clean up process resources"""
        if wrapper_id not in self.running_processes:
            logger.warning(f"Attempted to cleanup non-existent process {wrapper_id}")
            return

        wrapper_process = self.running_processes[wrapper_id]

        try:
            # Ensure process is terminated
            if wrapper_process.process.poll() is None:
                logger.info(
                    f"Terminating still-running process for wrapper {wrapper_id}"
                )
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

            logger.info(
                f"Successfully cleaned up process resources for wrapper {wrapper_id}"
            )

        except (ProcessLookupError, OSError, subprocess.SubprocessError) as e:
            logger.error(
                f"Error during process cleanup for wrapper {wrapper_id}: {e}",
                exc_info=True,
            )
            if wrapper_id in self.running_processes:
                del self.running_processes[wrapper_id]
        finally:
            # Always give the concurrency slot back, even if cleanup failed.
            if wrapper_process.holds_slot:
                wrapper_process.holds_slot = False
                self._start_semaphore.release()

    async def stop_wrapper_process(self, wrapper_id: str) -> bool:
        """Stop a running wrapper process by wrapper ID."""
        if wrapper_id not in self.running_processes:
            return False
        await self._cleanup_process(wrapper_id)
        return True


# Global process manager instance
wrapper_process_manager = WrapperProcessManager()
