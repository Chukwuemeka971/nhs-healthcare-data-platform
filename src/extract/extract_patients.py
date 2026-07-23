from pyspark.sql import SparkSession
from src.utils.config import load_config
from src.utils.logger import get_logger
import os

logger = get_logger(__name__)
config = load_config()

def extract_patient_data():
    try:
        logger.info("Starting patient extraction...")

        spark = (
            SparkSession.builder
            .appName("NHS Healthcare Pipeline")
            .getOrCreate()
        )

        landing_path = os.path.join(config["storage"]["landing"],"patient_admissions_sample.csv")
        
        logger.info(f"Reading files from {landing_path}")

        df = (
            spark.read
            .option("header",True)
            .option("inferSchema", True)
            .csv(landing_path)
        )

        logger.info(f"Successfully read {df.count()} records")
        return df
    except Exception as error:
        logger.error( f"Extraction failed: {error}")
        raise
                

                
        



