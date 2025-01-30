from pyspark.sql import SparkSession

class SparkHelper:
    """Helper class to manage Spark session and configurations"""
    
    def __init__(self, spark_configs: dict):
        """
        Initialize SparkHelper with configurations
        
        Parameters:
        -----------
        spark_configs : dict
            Dictionary containing Spark configurations
        """
        self.spark_configs = spark_configs
        self._spark = None
    
    def get_spark_session(self) -> SparkSession:
        """
        Create or get existing Spark session
        
        Returns:
        --------
        SparkSession
            Configured Spark session
        """
        if self._spark is None:
            self._spark = (SparkSession.builder
                          .appName(self.spark_configs['app_name'])
                          .master(self.spark_configs['master'])
                          .getOrCreate())
            
            self._spark.sparkContext.setLogLevel(self.spark_configs['log_level'])
        
        return self._spark