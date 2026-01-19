import logging
from datetime import datetime
from typing import Optional

from services.abc.wrapper_runner import WrapperRunner
from services.abc.wrapper_monitor import WrapperMonitor
from services.wrapper_process_manager import WrapperProcessManager
from services.process_wrapper_monitor import ProcessWrapperMonitor
from schemas.wrapper import GeneratedWrapper, WrapperExecutionResult, SourceType

logger = logging.getLogger(__name__)


class ProcessWrapperRunner(WrapperRunner):
    """
    Process-based wrapper execution implementation.

    Spawns wrappers in separate processes for continuous execution.
    Suitable for API data sources that need to run continuously.
    """

    def __init__(self, process_manager: WrapperProcessManager):
        self.process_manager = process_manager
        self._monitor = ProcessWrapperMonitor(process_manager)

    def get_monitor(self) -> WrapperMonitor:
        return self._monitor

    async def execute_wrapper(self, wrapper: GeneratedWrapper, skip_historical: bool = False) -> WrapperExecutionResult:
        """
        Execute a wrapper in a separate process.

        For continuous wrappers (API sources), this starts the process and returns immediately.
        For one-time wrappers (CSV/XLSX), this could still use process isolation.

        Args:
            wrapper: GeneratedWrapper instance with code and metadata

        Returns:
            WrapperExecutionResult with execution details
        """
        try:
            skip_msg = " (skipping historical data)" if skip_historical else ""
            logger.info(f"Starting process execution for wrapper {wrapper.wrapper_id}{skip_msg}")

            # Start the wrapper process
            wrapper_file_path = f"/app/generated_wrappers/{wrapper.wrapper_id}.py"
            success = await self.process_manager.start_wrapper_process(
                wrapper.wrapper_id,
                wrapper_file_path,
                wrapper.source_config,
                skip_historical=skip_historical
            )

            if success:
                logger.info(f"Successfully started process for wrapper {wrapper.wrapper_id}")

                # For process execution, we return success immediately after starting
                execution_mode = "continuous" if wrapper.source_type == SourceType.API else "once"

                return WrapperExecutionResult(
                    wrapper_id=wrapper.wrapper_id,
                    success=True,
                    message=f"Wrapper process started successfully in {execution_mode} mode",
                    data_points_sent=None,  # Unknown for process-based execution
                    execution_time=datetime.utcnow()
                )
            else:
                logger.error(f"Failed to start process for wrapper {wrapper.wrapper_id}")

                return WrapperExecutionResult(
                    wrapper_id=wrapper.wrapper_id,
                    success=False,
                    message="Failed to start wrapper process",
                    data_points_sent=None,
                    execution_time=datetime.utcnow()
                )

        except (OSError, ValueError, RuntimeError) as e:
            logger.error(f"Error executing wrapper {wrapper.wrapper_id} in process: {e}")

            return WrapperExecutionResult(
                wrapper_id=wrapper.wrapper_id,
                success=False,
                message=f"Process execution failed: {str(e)}",
                data_points_sent=None,
                execution_time=datetime.utcnow()
            )