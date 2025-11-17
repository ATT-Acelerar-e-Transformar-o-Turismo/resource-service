from abc import ABC, abstractmethod
from schemas.wrapper import GeneratedWrapper, WrapperExecutionResult
from services.abc.wrapper_monitor import WrapperMonitor

class WrapperRunner(ABC):
    """Abstract base class for wrapper execution adapters"""

    @abstractmethod
    async def execute_wrapper(self, wrapper: GeneratedWrapper, skip_historical: bool = False) -> WrapperExecutionResult:
        """Execute a wrapper and return the result"""
        pass

    @abstractmethod
    def get_monitor(self) -> WrapperMonitor:
        """Get the monitor instance for this wrapper runner

        Returns:
            WrapperMonitor: Type-specific monitor implementation
        """
        pass