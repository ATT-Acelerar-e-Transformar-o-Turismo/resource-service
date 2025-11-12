from jinja2 import Environment, FileSystemLoader

class PromptManager:
    """Manages prompt templates for wrapper generation"""
    
    def __init__(self, template_dir: str = None):
        if template_dir is None:
            import os
            template_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompt_templates")
        self.env = Environment(
            loader=FileSystemLoader(template_dir),
            trim_blocks=True,
            lstrip_blocks=True
        )
    
    def generate_wrapper_prompt(self, indicator_metadata, source_config, source_type: str, wrapper_id: str, data_sample: str) -> str:
        """Generate wrapper prompt using templates - exact same logic as before, just templated"""
        # Get the main template
        template = self.env.get_template("wrapper_generation.j2")

        # Get source-specific instructions using templates
        source_specific_instructions = self._get_source_specific_instructions(
            source_type,
            indicator_metadata.periodicity
        )

        # Get wrapper template using Jinja2
        wrapper_template = self._get_wrapper_template(source_type, indicator_metadata.periodicity)
        
        # Render the template - exact same variables as the f-string
        return template.render(
            indicator_metadata=indicator_metadata,
            source_config=source_config,
            wrapper_id=wrapper_id,
            data_sample=data_sample,
            wrapper_template=wrapper_template,
            source_specific_instructions=source_specific_instructions
        )
    
    def _get_source_specific_instructions(self, source_type: str, periodicity: str) -> str:
        """Get specific instructions based on source type - same logic, just templated"""
        if source_type == "CSV":
            template = self.env.get_template("csv_instructions.j2")
        elif source_type == "XLSX":
            template = self.env.get_template("xlsx_instructions.j2")
        else:  # API
            template = self.env.get_template("api_instructions.j2")
        
        return template.render(periodicity=periodicity)
    
    def _get_wrapper_template(self, source_type: str, periodicity: str = "Daily") -> str:
        """Get the wrapper template rendered with source type and periodicity"""
        additional_imports = self._get_additional_imports(source_type)
        
        template = self.env.get_template("wrapper_template.j2")
        
        return template.render(
            additional_imports=additional_imports,
            source_type=source_type,
            periodicity=periodicity
        )
    
    def _get_additional_imports(self, source_type: str) -> str:
        """Get additional imports based on source type"""
        if source_type in ["CSV", "XLSX"]:
            return "import pandas as pd\nimport os"
        return ""