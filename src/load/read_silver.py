import os

from pyspark.sql import SparkSession

from src.config.config import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


def read_silver():

    spark = (
        SparkSession.builder
        .appName("Read Silver")
        .getOrCreate()
    )

    config = load_config()

    silver_path = os.path.join(config["storage"]["silver"],"patient_admissions")

    try:

        logger.info(f"Reading Silver layer from {silver_path}")

        df = spark.read.format("delta").load(silver_path)

        logger.info(f"Loaded {df.count()} records from Silver.")

        return df

    except Exception:

        logger.info("Silver layer does not exist yet.")

        return None