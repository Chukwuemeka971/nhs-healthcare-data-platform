import os

from src.utils.logger import get_logger
from src.utils.config import load_config

logger = get_logger(__name__)
config = load_config()


def write_bronze(df):

    bronze_path = os.path.join(
        config["storage"]["bronze"],
        "patient_admissions"
    )

    logger.info(f"Writing Bronze data to {bronze_path}")

    (
        df.write
        .mode("overwrite")
        .parquet(bronze_path)
    )

    logger.info("Bronze write completed.")