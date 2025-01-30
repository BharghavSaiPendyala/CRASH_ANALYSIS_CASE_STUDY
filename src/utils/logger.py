import logging
import logging.config
import os
import yaml
from pathlib import Path

def get_project_root():
    """Get the absolute path to the project root directory"""
    current_file = Path(__file__)
    return str(current_file.parent.parent.parent)

def setup_logging(
    default_path='config/logging_config.yaml',
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """
    Setup logging configuration
    
    Parameters:
    -----------
    default_path : str
        Path to the logging configuration file
    default_level : logging.level
        Default logging level
    env_key : str
        Environment variable key for logging config
    """
    project_root = get_project_root()
    config_path = os.path.join(project_root, default_path)
    logs_dir = os.path.join(project_root, 'logs')
    
    # Create logs directory if it doesn't exist
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    path = default_path
    value = os.getenv(env_key, None)
    
    if value:
        path = value
        
    if os.path.exists(config_path):
        with open(config_path, 'rt') as f:
            try:
                config = yaml.safe_load(f.read())
                # Update log file path to use absolute path
                config['handlers']['file']['filename'] = os.path.join(project_root, 
                    config['handlers']['file']['filename'])
                logging.config.dictConfig(config)
            except Exception as e:
                print(f'Error loading logging configuration: {e}')
                logging.basicConfig(level=default_level)
    else:
        logging.basicConfig(level=default_level)
        print(f'Failed to load configuration file at {config_path}')

def get_logger(name):
    """
    Get logger instance
    
    Parameters:
    -----------
    name : str
        Name of the logger
        
    Returns:
    --------
    logging.Logger
        Logger instance
    """
    return logging.getLogger(f'car_crash_analysis.{name}')