import os

from src.utils.logger import get_logger
from src.utils.config import load_config

logger = get_logger(__name__)
config = load_config()


def write_silver(df):

    silver_path = os.path.join(config["storage"]["silver"],"patient_admissions")

    logger.info(f"Writing Silver data to {silver_path}")

    df.write.format("delta").mode("overwrite").save(silver_path)
    

    logger.info("Silver write completed.")