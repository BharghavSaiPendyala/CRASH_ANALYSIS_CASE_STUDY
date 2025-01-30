import os
from pathlib import Path
from pyspark.sql import SparkSession
import yaml
from analysis.crash_analyzer import CrashAnalyzer
from utils.result_writer import ResultWriter
from utils.spark_helper import SparkHelper
from utils.logger import setup_logging, get_logger

# Setup logging at the start
setup_logging()
logger = get_logger('main')

def get_project_root():
    """Get the absolute path to the project root directory"""
    current_file = Path(__file__)
    return str(current_file.parent.parent)

def load_config():
    """Load configuration from yaml file"""
    project_root = get_project_root()
    config_path = os.path.join(project_root, 'config', 'config.yaml')
    
    try:
        with open(config_path, 'r') as file:
            logger.info(f"Loading configuration from {config_path}")
            return yaml.safe_load(file)
    except FileNotFoundError:
        logger.error(f"Configuration file not found at {config_path}")
        raise FileNotFoundError(f"Configuration file not found at {config_path}. "
                              f"Please ensure config.yaml exists in the config directory.")

def main():
    """Main function to execute all analyses"""
    logger.info("\n" + "="*60 + "\n" + "="*60 + "\n" + "="*60 + "\n" + "="*60 + "\n" + "="*60 + "\n" + "="*60 )
    logger.info("Starting Car Crash Analysis application")
    
    try:
        # Load configuration
        config = load_config()
        
        # Initialize Spark
        logger.info("Initializing Spark session")
        spark_helper = SparkHelper(config['spark_configs'])
        spark = spark_helper.get_spark_session()
        
        # Load DataFrames
        logger.info("Initializing CrashAnalyzer")
        crash_analyzer = CrashAnalyzer(spark, config['input_paths'])
        
        # Perform analyses
        results = {
            'analysis_1': crash_analyzer.analyze_male_fatalities(),
            'analysis_2': crash_analyzer.count_two_wheeler_crashes(),
            'analysis_3': crash_analyzer.top_5_makes_fatal_crashes(),
            'analysis_4': crash_analyzer.hit_and_run_valid_licenses(),
            'analysis_5': crash_analyzer.state_with_highest_female_accidents(),
            'analysis_6': crash_analyzer.top_vehicle_makes_contributing_to_injuries(),
            'analysis_7': crash_analyzer.top_ethnic_user_groups_per_body_style(),
            'analysis_8': crash_analyzer.top_5_zip_codes_with_alcohols(),
            'analysis_9': crash_analyzer.crashes_with_no_damage(),
            'analysis_10': crash_analyzer.analyze_speed_related_offenses()
        }
        
        # Save results
        output_base = os.path.join(get_project_root(), config['output_path'])
        for analysis_name, result in results.items():

            try:
                logger.info(f"Started Writing Results of {analysis_name}")
                output_path = os.path.join(output_base, analysis_name)
                writer = ResultWriter(output_path)
                writer.write_results(result)
                logger.info(f"Completed Writing Results of {analysis_name} to {output_path}")

            except Exception as e:
                    logger.error(f"Error in {analysis_name}: {str(e)}", exc_info=True)
                    continue
            
        logger.info("Analysis complete!")
        spark.stop()
    
    except Exception as e:
        logger.error("Fatal error in main execution", exc_info=True)
        raise

if __name__ == "__main__":
    main()
