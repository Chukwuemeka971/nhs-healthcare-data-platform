import glob
import os

import pandas as pd

from pyspark.sql import DataFrame
from src.utils.config import load_config
from src.utils.logger import get_logger
from src.utils.spark import get_spark

logger = get_logger(__name__)
config = load_config()


def extract_patient_data() -> DataFrame:
    try:
        logger.info("Starting patient extraction...")

        spark = get_spark("NHS Healthcare Pipeline")

        landing_folder = config["storage"]["landing"]

        excel_files = glob.glob(os.path.join(landing_folder,"*.xlsx"))

        if not excel_files:
            raise FileNotFoundError("No Excel files files found in landing folder.")

        landing_path = max(excel_files,key = os.path.getmtime)

        logger.info(f"Processing file: {os.path.basename(landing_path)}")
        logger.info(f"Found {len(excel_files)} Excel file(s)")

        # Read Excel into a Pandas DataFrame
        pdf = pd.read_excel(landing_path)

        logger.info(f"Read {len(pdf)} records from Excel")

        # Convert to a Spark DataFrame
        df = spark.createDataFrame(pdf)

        logger.info("Successfully converted Excel to Spark DataFrame")

        filename = os.path.basename(landing_path)

        return df, filename

    except Exception as error:
        logger.error(f"Extraction failed: {error}")
        raise