import asyncio
import aio_pika
import json
from google import genai
from datetime import datetime, timedelta
import os
import importlib.util
import tempfile
import sys
from typing import Dict, List, Any, Optional
import requests
import random
import pandas as pd
from .wrapper_template import get_wrapper_template
from .prompt_manager import PromptManager

class IndicatorMetadata:
    """
    Class to hold indicator metadata
    """
    def __init__(self, 
                 name: str,
                 domain: str,
                 subdomain: str,
                 description: str,
                 unit: str,
                 source: str,
                 scale: str,
                 governance_indicator: bool,
                 carrying_capacity: Optional[float],
                 periodicity: str):
        self.name = name
        self.domain = domain
        self.subdomain = subdomain
        self.description = description
        self.unit = unit
        self.source = source
        self.scale = scale
        self.governance_indicator = governance_indicator
        self.carrying_capacity = carrying_capacity
        self.periodicity = periodicity

class DataSourceConfig:
    """
    Configuration for different data source types
    """
    def __init__(self, 
                 source_type: str,  # "API", "CSV", "XLSX"
                 location: str,     # endpoint URL or file path
                 auth_config: Optional[Dict[str, Any]] = None):  # authentication config for APIs
        self.source_type = source_type
        self.location = location
        self.auth_config = auth_config or {}

class DebugLogger:
    """Logger for debug mode that saves prompts and responses"""
    
    def __init__(self, debug_mode: bool = False, debug_dir: str = "prompts"):
        self.debug_mode = debug_mode
        self.debug_dir = debug_dir
        
        if self.debug_mode:
            self._setup_debug_directory()
    
    def _setup_debug_directory(self):
        """Setup debug directory"""
        if not os.path.exists(self.debug_dir):
            os.makedirs(self.debug_dir)
            print(f"Debug mode activated: {self.debug_dir}/")
    
    def log_prompt_response(self, prompt: str, response: str, metadata: Dict[str, Any] = None):
        """Save prompt and response to debug directory using wrapper_id"""
        if not self.debug_mode:
            return
        
        # Use wrapper_id from metadata as directory name
        wrapper_id = metadata.get('wrapper_id', 'unknown') if metadata else 'unknown'
        prompt_dir = os.path.join(self.debug_dir, str(wrapper_id))
        
        try:
            os.makedirs(prompt_dir, exist_ok=True)
            
            # Save prompt
            prompt_file = os.path.join(prompt_dir, "prompt.txt")
            with open(prompt_file, 'w', encoding='utf-8') as f:
                f.write(prompt)
            
            # Save response
            response_file = os.path.join(prompt_dir, "response.txt")
            with open(response_file, 'w', encoding='utf-8') as f:
                f.write(response)
            
            # Save metadata if provided
            if metadata:
                metadata_file = os.path.join(prompt_dir, "metadata.json")
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"Debug: Wrapper {wrapper_id} prompt saved to {prompt_dir}/")
            
        except Exception as e:
            print(f"Error saving debug prompt for wrapper {wrapper_id}: {e}")
    
    def log_error(self, error: str, context: Dict[str, Any] = None):
        """Save error to debug using wrapper_id"""
        if not self.debug_mode:
            return
        
        # Use wrapper_id from context as directory name
        wrapper_id = context.get('wrapper_id', 'unknown') if context else 'unknown'
        error_dir = os.path.join(self.debug_dir, f"{wrapper_id}_ERROR")
        
        try:
            os.makedirs(error_dir, exist_ok=True)
            
            # Save error
            error_file = os.path.join(error_dir, "error.txt")
            with open(error_file, 'w', encoding='utf-8') as f:
                f.write(f"ERROR: {error}\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                if context:
                    f.write(f"\nContext:\n{json.dumps(context, indent=2, ensure_ascii=False, default=str)}")
            
            print(f"Debug: Wrapper {wrapper_id} error saved to {error_dir}/")
            
        except Exception as e:
            print(f"Error saving debug error for wrapper {wrapper_id}: {e}")

class WrapperGenerator:
    def __init__(self, gemini_api_key: str, rabbitmq_url: str = "amqp://guest:guest@localhost/", debug_mode: bool = False, debug_dir: str = "prompts", model_name: str = "gemini-2.5-flash"):
        """
        Initialize the wrapper generator with Gemini API key and RabbitMQ connection
        
        Args:
            gemini_api_key: API key for Gemini
            rabbitmq_url: RabbitMQ connection URL
            debug_mode: Enable debug mode for saving prompts/responses
            debug_dir: Directory to save debug files
            model_name: Gemini model name to use (default: gemini-2.5-flash)
        """
        self.gemini_api_key = gemini_api_key
        self.rabbitmq_url = rabbitmq_url
        self.debug_logger = DebugLogger(debug_mode, debug_dir)
        self.client = genai.Client(api_key=gemini_api_key)
        self.model_name = model_name
        self.prompt_manager = PromptManager()

    def get_csv_sample(self, file_path: str, max_lines: int = 20) -> str:
        """
        Extract first 20 lines from CSV file for analysis
        """
        try:
            sample_lines = []
            with open(file_path, 'r', encoding='utf-8') as file:
                for i, line in enumerate(file):
                    if i >= max_lines:
                        break
                    sample_lines.append(line.strip())
            
            return '\n'.join(sample_lines)
        except Exception as e:
            return f"Error reading CSV file: {str(e)}"

    def get_xlsx_sample(self, file_path: str, max_lines_per_sheet: int = 15) -> str:
        """
        Extract pages and first 15 lines of each page from XLSX file
        """
        try:
            # Read Excel file and get all sheet names
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names
            
            sample_data = []
            sample_data.append(f"XLSX File: {file_path}")
            sample_data.append(f"Total sheets: {len(sheet_names)}")
            sample_data.append(f"Sheet names: {sheet_names}")
            sample_data.append("")
            
            # Sample from each sheet
            for sheet_name in sheet_names:
                sample_data.append(f"=== Sheet: {sheet_name} ===")
                try:
                    df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=max_lines_per_sheet)
                    sample_data.append(f"Columns: {list(df.columns)}")
                    sample_data.append(f"Shape: {df.shape}")
                    sample_data.append("Sample data:")
                    sample_data.append(df.to_string())
                except Exception as sheet_error:
                    sample_data.append(f"Error reading sheet {sheet_name}: {sheet_error}")
                sample_data.append("")
            
            return '\n'.join(sample_data)
        except Exception as e:
            return f"Error reading XLSX file: {str(e)}"

    def get_api_sample(self, endpoint: str, auth_config: Dict[str, Any], max_chars: int = 2500) -> str:
        """
        Make API call and return response sample
        """
        try:
            print(f"Making API sample call to: {endpoint}")
            print(f"Auth config: {auth_config}")
            # Prepare headers
            headers = auth_config.get('headers', {})
            
            # Add API key to headers if specified
            if 'api_key' in auth_config and 'header_name' in auth_config:
                headers[auth_config['header_name']] = auth_config['api_key']
            
            # Prepare parameters
            params = auth_config.get('params', {})
            
            # Make API call with timeout
            response = requests.get(
                endpoint, 
                headers=headers, 
                params=params,
                timeout=30
            )
            
            # Check if request was successful
            response.raise_for_status()
            
            # Try to parse as JSON
            try:
                json_data = response.json()
                # Limit the sample size for large responses
                sample_text = json.dumps(json_data, indent=2)
                if len(sample_text) > max_chars:
                    sample_text = sample_text[:max_chars] + "\n... (truncated)"
                return f"API Response (Status: {response.status_code}):\n{sample_text}"
            except json.JSONDecodeError:
                # If not JSON, return text content
                content = response.text
                if len(content) > max_chars:
                    content = content[:max_chars] + "\n... (truncated)"
                return f"API Response (Status: {response.status_code}):\n{content}"
                
        except requests.exceptions.RequestException as e:
            return f"Error calling API: {str(e)}"
        except Exception as e:
            return f"Unexpected error: {str(e)}"

    async def generate_wrapper(self,
                             indicator_metadata: IndicatorMetadata,
                             source_config: DataSourceConfig,
                             source_type: str,
                             wrapper_id: str) -> str:
        """
        Use Gemini to generate a customized wrapper based on indicator metadata and source configuration
        Note: source_config.location is always populated (computed from file_id for CSV/XLSX in the route handler)
        """

        # Get data sample based on source type
        print(f"Extracting sample from {source_type} source...")
        if source_type == "CSV":
            data_sample = self.get_csv_sample(source_config.location)
        elif source_type == "XLSX":
            data_sample = self.get_xlsx_sample(source_config.location)
        elif source_type == "API":
            # Convert new API config format to legacy auth_config for compatibility
            auth_config = {}
            if source_config.auth_type == "api_key" and source_config.api_key:
                auth_config['api_key'] = source_config.api_key
                auth_config['header_name'] = source_config.api_key_header or "X-API-Key"
            elif source_config.auth_type == "bearer" and source_config.bearer_token:
                auth_config['headers'] = {"Authorization": f"Bearer {source_config.bearer_token}"}
            elif source_config.auth_type == "basic" and source_config.username and source_config.password:
                import base64
                credentials = base64.b64encode(f"{source_config.username}:{source_config.password}".encode()).decode()
                auth_config['headers'] = {"Authorization": f"Basic {credentials}"}
            
            # Add custom headers and query params
            if source_config.custom_headers:
                auth_config.setdefault('headers', {}).update(source_config.custom_headers)
            if source_config.query_params:
                auth_config['params'] = source_config.query_params
                
            data_sample = self.get_api_sample(source_config.location, auth_config)
        else:
            data_sample = "Unknown source type"
        
        print(f"Data sample: {data_sample[:400]}" + "..." if len(data_sample) > 400 else "")

        prompt = self.prompt_manager.generate_wrapper_prompt(
            indicator_metadata, source_config, source_type, wrapper_id, data_sample
        )

        # Prepare metadata for debug
        debug_metadata = {
            "wrapper_id": wrapper_id,
            "indicator_name": indicator_metadata.name,
            "source_type": source_type,
            "source_location": source_config.location,
            "timestamp": datetime.now().isoformat(),
            "prompt_length": len(prompt),
            "data_sample_length": len(data_sample)
        }

        try:
            print("Calling Gemini...")
            response = await self.client.aio.models.generate_content(
                model=self.model_name,
                contents=prompt
            )
            generated_code = response.text.strip()
            
            # Log prompt and response in debug mode
            self.debug_logger.log_prompt_response(
                prompt=prompt,
                response=generated_code,
                metadata=debug_metadata
            )
            
            # Clean markdown code block markers if present
            generated_code = self._clean_code_response(generated_code)
            
            # Verify that the code was actually customized (not just the template)
            template_code = get_wrapper_template(source_type, indicator_metadata.periodicity)
            if generated_code == template_code:
                error_msg = "Gemini returned unchanged template"
                
                # If debug_mode is activated, show prompt and response
                if self.debug_logger.debug_mode:
                    print(f"\n{'='*80}")
                    print(f"DEBUG: {error_msg}")
                    print(f"Wrapper ID: {wrapper_id}")
                    print(f"{'='*80}")
                    print(f"\nPROMPT SENT TO GEMINI:")
                    print(f"{'-'*40}")
                    print(prompt[:2000] + "..." if len(prompt) > 2000 else prompt)
                    print(f"\n{'-'*40}")
                    print(f"\nGEMINI RESPONSE:")
                    print(f"{'-'*40}")
                    print(generated_code[:2000] + "..." if len(generated_code) > 2000 else generated_code)
                    print(f"\n{'-'*40}")
                    print(f"Response length: {len(generated_code)} characters")
                    print(f"Contains PLACEHOLDER: {'PLACEHOLDER' in generated_code}")
                    print(f"Contains ...: {'...' in generated_code}")
                    print(f"Equals template: {generated_code == template_code}")
                    print(f"{'='*80}\n")
                
                self.debug_logger.log_error(error_msg, debug_metadata)
                raise Exception(error_msg)
            
            return generated_code
            
        except Exception as e:
            error_msg = f"Error generating wrapper with Gemini: {str(e)}"
            print(error_msg)
            
            # Log error in debug mode
            debug_metadata["error"] = str(e)
            self.debug_logger.log_error(error_msg, debug_metadata)
            
            raise Exception(f"Failed to generate customized wrapper: {error_msg}")

    def _clean_code_response(self, code: str) -> str:
        """
        Clean markdown code block markers from Gemini response
        """
        # Remove markdown code block markers
        if code.startswith('```python'):
            code = code[9:]  # Remove ```python
        elif code.startswith('```'):
            code = code[3:]   # Remove ```
        
        if code.endswith('```'):
            code = code[:-3]  # Remove trailing ```
        
        # Strip any remaining whitespace
        code = code.strip()
        
        return code


    def save_wrapper(self, wrapper_code: str, wrapper_id: str) -> str:
        """
        Save the generated wrapper to a file
        """
        # Create wrappers directory
        wrapper_dir = "/app/generated_wrappers"
        os.makedirs(wrapper_dir, exist_ok=True)

        # Simple filename pattern
        filename = f"{wrapper_id}.py"
        file_path = os.path.join(wrapper_dir, filename)

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(wrapper_code)

        print(f"Wrapper saved to: {file_path}")
        return file_path

    async def execute_wrapper(self, wrapper_file: str, wrapper_id: int, mode: str = "once", timeout_seconds: int = 8):
        """
        Execute the generated wrapper with timeout
        """
        try:
            # Load and execute the wrapper module
            spec = importlib.util.spec_from_file_location(f"wrapper_{wrapper_id}", wrapper_file)
            wrapper_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(wrapper_module)
            
            # Create wrapper instance
            wrapper = wrapper_module.ATTWrapper(wrapper_id)
            
            # Execute wrapper with timeout
            try:
                if mode == "once":
                    await asyncio.wait_for(wrapper.run_once(), timeout=timeout_seconds)
                else:
                    # For continuous mode, the wrapper handles its own scheduling
                    await asyncio.wait_for(wrapper.run_continuous(), timeout=timeout_seconds)
                    
            except asyncio.TimeoutError:
                print(f"Wrapper {wrapper_id} execution timed out after {timeout_seconds} seconds")
                # Timeout is expected for continuous mode, not an error
                if mode == "continuous":
                    print(f"Continuous wrapper {wrapper_id} stopped after timeout (normal behavior)")
                else:
                    print(f"Warning: 'once' mode wrapper {wrapper_id} timed out (may indicate an issue)")
                
        except Exception as e:
            print(f"Error executing wrapper: {str(e)}")
            raise

    async def generate_and_run_wrapper(self, 
                                     indicator_metadata: IndicatorMetadata,
                                     source_config: DataSourceConfig,
                                     wrapper_id: int,
                                     mode: str = "once"):
        """
        Complete workflow: generate, save and execute wrapper
        """
        print(f"Generating wrapper {wrapper_id} for indicator: {indicator_metadata.name}...")
        
        # Generate wrapper code
        wrapper_code = await self.generate_wrapper(
            indicator_metadata, 
            source_config, 
            wrapper_id
        )
        
        # Save to file
        wrapper_file = self.save_wrapper(wrapper_code, wrapper_id, indicator_metadata.name)
        
        # Execute wrapper
        print(f"Executing wrapper {wrapper_id} in {mode} mode...")
        await self.execute_wrapper(wrapper_file, wrapper_id, mode, timeout_seconds=60)

def create_wrapper_from_config(
    # Indicator metadata
    indicator_name: str,
    domain: str,
    subdomain: str,
    description: str,
    unit: str,
    source: str,
    scale: str,
    governance_indicator: bool,
    carrying_capacity: Optional[float],
    periodicity: str,
    # Source configuration
    source_type: str,
    location: str,
    # Generation parameters
    wrapper_id: int,
    gemini_api_key: str,
    mode: str = "once",
    auth_config: Optional[Dict[str, Any]] = None,
    model_name: str = "gemini-2.5-flash"
) -> None:
    """
    Utility function to create a wrapper from configuration parameters
    
    Args:
        indicator_name: Name of the indicator
        domain: Main domain (e.g., "Economy", "Environment")
        subdomain: Subdomain (e.g., "Fishing", "Climate")
        description: Description of the indicator
        unit: Unit of measurement (e.g., "%", "°C")
        source: Data source (e.g., "INE", "gov")
        scale: Scale of measurement (e.g., "Municipal", "National")
        governance_indicator: Whether this is a governance indicator
        carrying_capacity: Maximum capacity value (if applicable)
        periodicity: How often data is updated ("Annual", "Monthly", etc.)
        source_type: Type of source ("API", "CSV", "XLSX")
        location: URL for API or file path for CSV/XLSX
        wrapper_id: Unique identifier for the wrapper
        gemini_api_key: API key for Gemini
        mode: Execution mode ("once" or "continuous")
        auth_config: Authentication configuration for APIs (optional)
        model_name: Gemini model name to use (default: gemini-2.5-flash)
    """
    async def _create_wrapper():
        generator = WrapperGenerator(gemini_api_key, model_name=model_name)
        
        metadata = IndicatorMetadata(
            name=indicator_name,
            domain=domain,
            subdomain=subdomain,
            description=description,
            unit=unit,
            source=source,
            scale=scale,
            governance_indicator=governance_indicator,
            carrying_capacity=carrying_capacity,
            periodicity=periodicity
        )
        
        source_config = DataSourceConfig(
            source_type=source_type,
            location=location,
            auth_config=auth_config
        )
    
        await generator.generate_and_run_wrapper(
            indicator_metadata=metadata,
            source_config=source_config,
            wrapper_id=wrapper_id,
            mode=mode
        )
    
    asyncio.run(_create_wrapper())

if __name__ == "__main__":
    print("WrapperGenerator module loaded successfully!")
    print("Use integration_tests.py to run integration tests")
    print("Use test_wrapper_generator.py to run unit tests")
    print("Use example_usage.py to see usage examples") 