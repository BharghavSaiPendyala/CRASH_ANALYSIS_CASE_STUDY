from typing import Dict
from pathlib import Path
import yaml


class ResultWriter:
    """Handles writing analysis results"""
    
    def __init__(self, output_path: str):
        self.output_path = output_path
    
    def write_results(self, results: Dict[str, any]):
        """Write results to YAML file"""
        output_path = Path(self.output_path)
        output_path.mkdir(parents=True, exist_ok=True)  # Create folder if it doesn't exist
        output_file = output_path / 'results.yaml' 
        with open(output_file, 'w') as f:
            yaml.dump(results, f)