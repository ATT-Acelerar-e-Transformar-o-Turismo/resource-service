from abc import ABC, abstractmethod
from typing import Dict, Any, List
from schemas.wrapper import GeneratedWrapper

class WrapperHealthStatus:
    """Health status for wrapper monitoring"""
    HEALTHY = "healthy"         # Sending data within SLA
    STALLED = "stalled"         # No data beyond SLA threshold
    DEGRADED = "degraded"       # Sending data but below expected rate
    CRASHED = "crashed"         # Process terminated (ProcessWrapperRunner only)
    UNKNOWN = "unknown"         # No data to assess

class WrapperMonitor(ABC):
    """Abstract base class for wrapper monitoring implementations"""

    @abstractmethod
    async def start_monitoring(self, wrapper: GeneratedWrapper) -> bool:
        """Start monitoring a wrapper

        Args:
            wrapper: GeneratedWrapper instance to monitor

        Returns:
            bool: True if monitoring started successfully
        """
        pass

    @abstractmethod
    async def stop_wrapper(self, wrapper_id: str) -> bool:
        """Stop a wrapper (internal stop logic varies by type)

        Args:
            wrapper_id: ID of wrapper to stop

        Returns:
            bool: True if wrapper stopped successfully
        """
        pass

    @abstractmethod
    async def get_health_status(self, wrapper_id: str) -> str:
        """Get current health status of wrapper

        Args:
            wrapper_id: ID of wrapper to check

        Returns:
            str: Health status (HEALTHY, STALLED, DEGRADED, CRASHED, UNKNOWN)
        """
        pass

    @abstractmethod
    async def get_monitoring_details(self, wrapper_id: str) -> Dict[str, Any]:
        """Get type-specific monitoring details

        Args:
            wrapper_id: ID of wrapper to get details for

        Returns:
            Dict[str, Any]: Type-specific monitoring data
            - ProcessWrapper: process_id, cpu_usage, memory_mb, exit_code, etc.
            - AsyncWrapper: execution_time_ms, rows_processed, last_execution, etc.
        """
        pass

    @abstractmethod
    async def get_logs(self, wrapper_id: str, limit: int = 100) -> List[str]:
        """Get wrapper logs from log files (not database)

        Args:
            wrapper_id: ID of wrapper to get logs for
            limit: Maximum number of log lines to return

        Returns:
            List[str]: Log lines from log files
        """
        pass

    @abstractmethod
    async def is_actively_executing(self, wrapper_id: str) -> bool:
        """Check if wrapper is actively executing

        Args:
            wrapper_id: ID of wrapper to check

        Returns:
            bool: True if wrapper is actively executing
        """
        pass