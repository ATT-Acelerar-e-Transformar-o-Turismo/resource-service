import os
import sys
import importlib.util
from datetime import datetime
from typing import Optional
from schemas.wrapper import GeneratedWrapper, WrapperExecutionResult, WrapperStatus
from services.abc.wrapper_runner import WrapperRunner
from config import settings
import logging

logger = logging.getLogger(__name__)

class AsyncWrapperRunner(WrapperRunner):
    """Async wrapper execution adapter - executes wrappers in same process with isolation"""
    
    def __init__(self):
        self.wrapper_runtime_dir = "/app/wrapper_runtime"
    
    async def execute_wrapper(self, wrapper: GeneratedWrapper) -> WrapperExecutionResult:
        """Execute a wrapper asynchronously with sys.path isolation"""
        wrapper_id = wrapper.wrapper_id
        
        try:
            # Create isolated directory for this wrapper
            wrapper_dir = f"{self.wrapper_runtime_dir}/{wrapper_id}"
            os.makedirs(wrapper_dir, exist_ok=True)
            
            # Write wrapper code to isolated directory
            wrapper_file_path = f"{wrapper_dir}/wrapper.py"
            with open(wrapper_file_path, 'w') as f:
                f.write(wrapper.generated_code)
            
            # Store original sys.path for restoration
            original_sys_path = sys.path.copy()
            
            try:
                shared_dir = f"{self.wrapper_runtime_dir}/shared"
                sys.path = [
                    wrapper_dir,
                    shared_dir,
                    *[p for p in original_sys_path if 'site-packages' in p or 'dist-packages' in p]
                ]
                
                
                # Clear import cache for modules with same names as files in shared_dir
                shared_dir = f"{self.wrapper_runtime_dir}/shared"
                if os.path.exists(shared_dir):
                    shared_files = [f[:-3] for f in os.listdir(shared_dir) if f.endswith('.py')]  # Remove .py extension
                    for module_name in shared_files:
                        if module_name in sys.modules:
                            del sys.modules[module_name]
                
                # Load and execute wrapper module
                spec = importlib.util.spec_from_file_location("wrapper_module", wrapper_file_path)
                wrapper_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(wrapper_module)
                
                # Create wrapper instance
                wrapper_instance = wrapper_module.ATTWrapper(
                    wrapper_id=wrapper_id,
                    rabbitmq_url=settings.DATA_RABBITMQ_URL
                )
                
                # Execute wrapper
                await wrapper_instance.run_once()
                
                return WrapperExecutionResult(
                    wrapper_id=wrapper_id,
                    success=True,
                    message="Wrapper executed successfully",
                    data_points_sent=None,  # Could be enhanced to return actual count
                    execution_time=datetime.utcnow().isoformat()
                )
                
            finally:
                # Always restore original sys.path
                sys.path = original_sys_path
                
                # Cleanup wrapper file
                if os.path.exists(wrapper_file_path):
                    os.unlink(wrapper_file_path)
                
        except Exception as e:
            logger.error(f"Failed to execute wrapper {wrapper_id}: {e}")
            
            return WrapperExecutionResult(
                wrapper_id=wrapper_id,
                success=False,
                message=f"Execution failed: {str(e)}",
                data_points_sent=None,
                execution_time=datetime.utcnow().isoformat()
            )