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
from schemas.wrapper import WrapperStatus, GeneratedWrapper
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
            except Exception as e:
                logger.error(f"Error in process monitoring: {e}")
    
    async def _check_process_health(self):
        """Check health of all running processes"""
        dead_processes = []
        
        for wrapper_id, wrapper_process in self.running_processes.items():
            try:
                # Check if process is still running
                if wrapper_process.process.poll() is not None:
                    # Process has terminated
                    dead_processes.append(wrapper_id)
                    logger.warning(f"Wrapper {wrapper_id} process terminated unexpectedly")
                    
                    # Update database status
                    await db.generated_wrappers.update_one(
                        {"wrapper_id": wrapper_id},
                        {
                            "$set": {"status": WrapperStatus.ERROR.value},
                            "$push": {"execution_log": f"Process terminated at {datetime.utcnow()}"}
                        }
                    )
                else:
                    # Process is running, update health check
                    wrapper_process.last_health_check = datetime.utcnow()
                    
            except Exception as e:
                logger.error(f"Health check failed for wrapper {wrapper_id}: {e}")
        
        # Clean up dead processes
        for wrapper_id in dead_processes:
            await self._cleanup_process(wrapper_id)
    
    async def start_wrapper_process(self, wrapper_id: str) -> bool:
        """Start a wrapper in a separate process"""
        try:
            # Check if already running
            if wrapper_id in self.running_processes:
                logger.warning(f"Wrapper {wrapper_id} is already running")
                return False
            
            # Get wrapper from database
            wrapper_doc = await db.generated_wrappers.find_one({"wrapper_id": wrapper_id})
            if not wrapper_doc:
                logger.error(f"Wrapper {wrapper_id} not found in database")
                return False
            
            wrapper = GeneratedWrapper(**wrapper_doc)
            
            # Create temporary file with wrapper code
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as temp_file:
                # Use the generated wrapper code directly (it already has run_continuous)
                process_script = self._create_process_script(wrapper.generated_code, wrapper_id)
                temp_file.write(process_script)
                temp_file_path = temp_file.name
            
            # Start process
            env = os.environ.copy()
            env['DATA_RABBITMQ_URL'] = settings.DATA_RABBITMQ_URL
            env['WRAPPER_ID'] = wrapper_id
            
            process = subprocess.Popen(
                ['python', temp_file_path],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid  # Create new process group
            )
            
            # Store process info
            wrapper_process = WrapperProcess(wrapper_id, process, temp_file_path)
            self.running_processes[wrapper_id] = wrapper_process
            
            # Update database status
            await db.generated_wrappers.update_one(
                {"wrapper_id": wrapper_id},
                {
                    "$set": {"status": WrapperStatus.RUNNING.value},
                    "$push": {"execution_log": f"Started process at {datetime.utcnow()}"}
                }
            )
            
            logger.info(f"Started wrapper {wrapper_id} in process {process.pid}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start wrapper process {wrapper_id}: {e}")
            return False
    
    def _create_process_script(self, wrapper_code: str, wrapper_id: str) -> str:
        """Create a script that runs the generated wrapper directly"""
        return f'''
import asyncio
import sys
import os

# Set wrapper ID from environment
wrapper_id = "{wrapper_id}"

# Generated wrapper code
{wrapper_code}

if __name__ == "__main__":
    # Create wrapper instance
    data_rabbitmq_url = os.getenv('DATA_RABBITMQ_URL')
    wrapper = ATTWrapper(wrapper_id, data_rabbitmq_url)
    
    # The wrapper template already handles continuous vs once execution
    # based on source_type (CSV/XLSX = once, API = continuous)
    if wrapper.source_type in ["CSV", "XLSX"]:
        print(f"File source ({{wrapper.source_type}}) detected - running once")
        asyncio.run(wrapper.run_once())
    else:
        print(f"API source detected - running continuously")
        asyncio.run(wrapper.run_continuous())
'''

# Global process manager instance
wrapper_process_manager = WrapperProcessManager()