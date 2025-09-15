from abc import ABC, abstractmethod
from schemas.wrapper import GeneratedWrapper, WrapperExecutionResult

class WrapperRunner(ABC):
    """Abstract base class for wrapper execution adapters"""
    
    @abstractmethod
    async def execute_wrapper(self, wrapper: GeneratedWrapper) -> WrapperExecutionResult:
        """Execute a wrapper and return the result"""
        pass