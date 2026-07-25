import os

from src.utils.logger import get_logger
from src.utils.config import load_config

logger = get_logger(__name__)
config = load_config()


def write_bronze(df):
    dataset_name = config["datasets"]["patient_admissions"]

    bronze_path = os.path.join(
        config["storage"]["bronze"],
        dataset_name
    )

    logger.info(f"Writing Bronze data to {bronze_path}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(bronze_path)
    )

    logger.info("Bronze write completed.")