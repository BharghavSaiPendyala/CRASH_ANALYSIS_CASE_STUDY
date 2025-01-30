from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Dict
from utils.logger import get_logger

class CrashAnalyzer:
    """
    Class to handle all crash analysis operations on vehicle accident data.
    
    This class provides methods to analyze various aspects of crash data including
    fatalities, vehicle types, damages, and contributing factors.
    """
    
    def __init__(self, spark: SparkSession, input_paths: dict):
        """
        Initialize CrashAnalyzer with spark session and data paths
        
        Parameters:
        -----------
        spark : SparkSession
            Active Spark session
        input_paths : dict
            Dictionary containing paths to input CSV files
            
        Raises:
        -------
        Exception
            If there's an error reading any of the input CSV files
        """
        self.logger = get_logger(self.__class__.__name__)
        self.spark = spark
        
        self.logger.info("Starting to load input DataFrames")
        try:
            self.charges_df = spark.read.csv(input_paths['charges'], header=True)
            self.damages_df = spark.read.csv(input_paths['damages'], header=True)
            self.endorse_df = spark.read.csv(input_paths['endorse'], header=True)
            self.primary_person_df = spark.read.csv(input_paths['primary_person'], header=True)
            self.restrict_df = spark.read.csv(input_paths['restrict'], header=True)
            self.units_df = spark.read.csv(input_paths['units'], header=True)
            self.logger.info("Successfully loaded all input DataFrames")
        except Exception as e:
            self.logger.error("Failed to load input DataFrames", exc_info=True)
            raise
        
    def analyze_male_fatalities(self) -> int:
        """
        Analysis 1: Find the number of crashes where more than 2 males were killed
        
        Returns:
        --------
        int
            Count of crashes where male deaths exceeded 2
            
        Raises:
        -------
        Exception
            If there's an error processing the data
        """
        self.logger.info("Starting analysis of male fatalities")
        try:
            result = (self.primary_person_df
                .filter((F.col('PRSN_GNDR_ID') == 'MALE') & 
                       (F.col('DEATH_CNT') > 0))
                .groupBy('CRASH_ID')
                .agg(F.sum('DEATH_CNT').alias('male_deaths'))
                .filter(F.col('male_deaths') > 2)
                .count())
            self.logger.info(f"Completed male fatalities analysis. Found {result} crashes")
            return result
        except Exception as e:
            self.logger.error("Error in male fatalities analysis", exc_info=True)
            raise

    def count_two_wheeler_crashes(self) -> int:
        """
        Analysis 2: Count the number of two wheelers involved in crashes
        
        Returns:
        --------
        int
            Count of motorcycle crashes
            
        Raises:
        -------
        Exception
            If there's an error processing the data
        """
        self.logger.info("Starting analysis of two wheeler crashes")
        try:
            result = (self.units_df
                .filter(F.col('VEH_BODY_STYL_ID').like('%MOTORCYCLE%'))
                .select('CRASH_ID')
                .count())
            self.logger.info(f"Completed two wheeler crash analysis. Found {result} crashes")
            return result
        except Exception as e:
            self.logger.error("Error in two wheeler crashes analysis", exc_info=True)
            raise

    def top_5_makes_fatal_crashes(self) -> List[str]:
        """
        Analysis 3: Identify top 5 Vehicle Makes in fatal crashes without airbag deployment
        
        Returns:
        --------
        List[str]
            List of top 5 vehicle makes
            
        Raises:
        -------
        Exception
            If there's an error processing the data
        """
        self.logger.info("Starting analysis of top 5 vehicle makes in fatal crashes")
        try:
            fatal_crashes = (self.primary_person_df
                .select('CRASH_ID', 'PRSN_AIRBAG_ID')
                .filter((F.col('DEATH_CNT') > 0) & 
                       (F.col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED')))
            
            result = (self.units_df
                .select('CRASH_ID', 'VEH_MAKE_ID')
                .join(fatal_crashes, 'CRASH_ID')
                .groupBy('VEH_MAKE_ID')
                .count()
                .orderBy(F.col('count').desc())
                .filter(F.col('VEH_MAKE_ID') != 'NA')
                .limit(5)
                .select('VEH_MAKE_ID')
                .rdd.flatMap(lambda x: x).collect())
            self.logger.info(f"Completed top 5 makes analysis. Results: {result}")
            return result
        except Exception as e:
            self.logger.error("Error in top 5 makes analysis", exc_info=True)
            raise

    def hit_and_run_valid_licenses(self) -> int:
        """
        Analysis 4: Count vehicles involved in hit and run crashes with valid licenses
        
        Returns:
        --------
        int
            Count of vehicles in hit and run crashes with valid licenses
            
        Raises:
        -------
        Exception
            If there's an error processing the data
        """
        self.logger.info("Starting analysis of hit and run cases with valid licenses")
        try:
            result = (self.units_df
                .filter(F.col('VEH_HNR_FL') == 'Y')
                .join(self.primary_person_df, 'CRASH_ID')
                .filter(F.col('DRVR_LIC_TYPE_ID').isin(['DRIVER_LICENSE', 'COMMERCIAL DRIVER LIC.']))
                .select('VIN')
                .distinct()
                .count())
            self.logger.info(f"Completed hit and run analysis. Found {result} cases")
            return result
        except Exception as e:
            self.logger.error("Error in hit and run analysis", exc_info=True)
            raise

    def state_with_highest_female_accidents(self) -> str:
        """
        Analysis 5: Find state with highest number of accidents where females were not involved
        
        Returns:
        --------
        str
            State ID with highest number of non-female accidents
            
        Raises:
        -------
        Exception
            If there's an error processing the data
        """
        self.logger.info("Starting analysis of states with highest non-female accidents")
        try:
            result = (self.units_df
                .join(self.primary_person_df, 'CRASH_ID')
                .filter(~F.col('PRSN_GNDR_ID').isin(['FEMALE']))
                .groupBy('VEH_LIC_STATE_ID')
                .count()
                .orderBy(F.col('count').desc())
                .first()['VEH_LIC_STATE_ID'])
            self.logger.info(f"Completed state analysis. State with highest non-female accidents: {result}")
            return result
        except Exception as e:
            self.logger.error("Error in state with highest non-female accidents analysis", exc_info=True)
            raise

    def top_vehicle_makes_contributing_to_injuries(self) -> List[str]:
        """
        Analysis 6: Identify 3rd to 5th VEH_MAKE_IDs that contribute to largest number of injuries
        
        Returns:
        --------
        List[str]
            List of vehicle makes ranked 3rd to 5th by injury count
            
        Raises:
        -------
        Exception
            If there's an error processing the data
        """
        self.logger.info("Starting analysis of vehicle makes contributing to injuries")
        try:
            result = (self.units_df
                .filter(F.col('VEH_MAKE_ID')!= 'NA')
                .withColumn('TOTAL_INJURIES', F.col('DEATH_CNT')+F.col('TOT_INJRY_CNT'))
                .groupBy('VEH_MAKE_ID')
                .agg(F.sum('TOTAL_INJURIES').alias('TOTAL_INJURIES'))
                .orderBy(F.col('TOTAL_INJURIES').desc())
                .select('VEH_MAKE_ID')
                .rdd.flatMap(lambda x: x)
                .collect()[2:5])
            self.logger.info(f"Completed vehicle makes analysis. 3rd to 5th ranked makes: {result}")
            return result
        except Exception as e:
            self.logger.error("Error in top vehicle makes contributing to injuries analysis", exc_info=True)
            raise

    def top_ethnic_user_groups_per_body_style(self) -> Dict[str, str]:
        """
        Analysis 7: Find top ethnic user group for each vehicle body style
        
        Returns:
        --------
        Dict[str, str]
            Dictionary mapping vehicle body styles to their top ethnic user group
            
        Raises:
        -------
        Exception
            If there's an error processing the data
        """
        self.logger.info("Starting analysis of ethnic groups per vehicle body style")
        try:
            result = (self.units_df
                .join(self.primary_person_df, 'CRASH_ID')
                .filter(
                    ~F.col('VEH_BODY_STYL_ID').isin(
                        ["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"]
                    )
                )
                .filter(~F.col('PRSN_ETHNICITY_ID').isin(["NA", "UNKNOWN","OTHER"]))
                .groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID')
                .count()
                .withColumn('rank', F.row_number().over(
                    Window.partitionBy('VEH_BODY_STYL_ID')
                    .orderBy(F.col('count').desc())))
                .filter(F.col('rank') == 1)
                .select('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID')
                .rdd.collectAsMap())
            self.logger.info(f"Completed ethnic groups analysis. Found mappings for {len(result)} body styles")
            self.logger.debug(f"Body style to ethnic group mappings: {result}")
            return result
        except Exception as e:
            self.logger.error("Error in top ethnic user groups per body style analysis", exc_info=True)
            raise

    def top_5_zip_codes_with_alcohols(self) -> List[str]:
        """
        Analysis 8: Find top 5 zip codes with highest number of crashes with alcohol as contributing factor
        
        Returns:
        --------
        List[str]
            List of top 5 zip codes
            
        Raises:
        -------
        Exception
            If there's an error processing the data
        """
        self.logger.info("Starting analysis of zip codes with alcohol-related crashes")
        try:
            result = (self.units_df
                .join(self.primary_person_df, 'CRASH_ID')
                .dropna(subset=['DRVR_ZIP'])
                .filter(
                    (F.col('VEH_BODY_STYL_ID').isin(['PASSENGER CAR, 4-DOOR', 'SPORT UTILITY VEHICLE', 'PASSENGER CAR, 2-DOOR'])) &
                    (
                        (F.col('CONTRIB_FACTR_1_ID').like('%ALCOHOL%')) |
                        (F.col('CONTRIB_FACTR_2_ID').like('%ALCOHOL%')) |
                        (F.col('PRSN_ALC_RSLT_ID') == 'Positive')
                    )
                )
                .groupBy('DRVR_ZIP')
                .count()
                .orderBy(F.col('count').desc())
                .limit(5)
                .select('DRVR_ZIP')
                .rdd.flatMap(lambda x: x)
                .collect())
            self.logger.info(f"Completed zip codes analysis. Top 5 zip codes: {result}")
            return result
        except Exception as e:
            self.logger.error("Error in top 5 zip codes with alcohols analysis", exc_info=True)
            raise

    def crashes_with_no_damage(self) -> int:
        """
        Analysis 9: Count distinct crash IDs where no damaged property was observed
        but damage level (VEH_DMAG_SCL) is above 4 and car avails insurance
        
        Returns:
        --------
        int
            Count of crashes meeting the criteria
            
        Raises:
        -------
        Exception
            If there's an error processing the data
        """
        self.logger.info("Starting analysis of crashes with no property damage but high damage level")
        try:
            result = (self.damages_df
                .join(self.units_df, 'CRASH_ID')
                .filter(
                    (
                        (F.col('VEH_DMAG_SCL_1_ID') > 'DAMAGED 4')
                        & (
                            ~F.col('VEH_DMAG_SCL_1_ID').isin(
                                ["NA", "NO DAMAGE", "INVALID VALUE"]
                            )
                        )
                    )
                    | (
                        (F.col('VEH_DMAG_SCL_2_ID') > 'DAMAGED 4')
                        & (
                            ~F.col('VEH_DMAG_SCL_2_ID').isin(
                                ["NA", "NO DAMAGE", "INVALID VALUE"]
                            )
                        )
                    )
                )
                .filter(F.col('DAMAGED_PROPERTY') == "NONE")
                .filter(F.col('FIN_RESP_TYPE_ID') == "PROOF OF LIABILITY INSURANCE")
                .select('CRASH_ID')
                .distinct()
                .count())
            self.logger.info(f"Completed no damage crashes analysis. Found {result} crashes")
            return result
        except Exception as e:
            self.logger.error("Error in crashes with no damage analysis", exc_info=True)
            raise

    def analyze_speed_related_offenses(self) -> List[str]:
        """
        Analysis 10: Analyze top vehicle makes involved in speed-related offenses
        
        Identifies the top 5 vehicle makes where:
        - Drivers were charged with speeding
        - Drivers had valid licenses
        - Vehicles had one of the top 10 most used colors
        - Vehicles were licensed in one of the top 25 states with highest offenses
        
        Returns:
        --------
        List[str]
            List of top 5 vehicle makes meeting the criteria
            
        Raises:
        -------
        Exception
            If there's an error processing the data
        """
        self.logger.info("Starting analysis of speed-related offenses")
        try:
            # Get top 25 states
            self.logger.debug("Calculating top 25 states")
            top_25_state_list = [
                row[0]
                for row in self.units_df.filter(
                    F.col("VEH_LIC_STATE_ID").cast("int").isNull()
                )
                .groupby("VEH_LIC_STATE_ID")
                .count()
                .orderBy(F.col("count").desc())
                .limit(25)
                .collect()
            ]

            # Get top 10 vehicle colors
            self.logger.debug("Calculating top 10 vehicle colors")
            top_10_used_vehicle_colors = [
                row[0]
                for row in self.units_df.filter(F.col('VEH_COLOR_ID') != "NA")
                .groupby("VEH_COLOR_ID")
                .count()
                .orderBy(F.col("count").desc())
                .limit(10)
                .collect()
            ]

            # Final analysis
            result = (
                self.charges_df.join(self.primary_person_df, on=["CRASH_ID"], how="inner")
                .join(self.units_df, on=["CRASH_ID"], how="inner")
                .filter(F.col('CHARGE').like("%SPEED%"))
                .filter(
                    self.primary_person_df.DRVR_LIC_TYPE_ID.isin(
                        ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
                    )
                )
                .filter(F.col('VEH_COLOR_ID').isin(top_10_used_vehicle_colors))
                .filter(F.col('VEH_LIC_STATE_ID').isin(top_25_state_list))
                .groupby("VEH_MAKE_ID")
                .count()
                .orderBy(F.col("count").desc())
                .limit(5)
                .select('VEH_MAKE_ID')
                .rdd.flatMap(lambda x: x)
                .collect()
            )
            
            self.logger.info(f"Completed speed-related offenses analysis. Top makes: {result}")
            return result
            
        except Exception as e:
            self.logger.error("Error in speed-related offenses analysis", exc_info=True)
            raise