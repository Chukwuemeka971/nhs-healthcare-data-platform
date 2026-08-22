from pyspark.sql import SparkSession

from src.config.config import load_config
from src.utils.logger import get_logger


logger = get_logger(__name__)


def extract_patient_admissions(spark: SparkSession):
    """
    Extract patient admission records
    from PostgreSQL using JDBC.
    """

    logger.info(
        "Starting PostgreSQL extraction."
    )

    config = load_config()

    postgres_config = config["postgres"]

    jdbc_url = postgres_config["jdbc_url"]
    table = postgres_config["table"]
    user = postgres_config["user"]
    password = postgres_config["password"]
    jdbc_driver = postgres_config["jdbc_driver"]

    logger.info(
        f"Reading table '{table}' from PostgreSQL."
    )

    patient_df = (
        spark.read
        .format("jdbc")
        .option(
            "url",
            jdbc_url,
        )
        .option(
            "dbtable",
            table,
        )
        .option(
            "user",
            user,
        )
        .option(
            "password",
            password,
        )
        .option(
            "driver",
            jdbc_driver,
        )
        .load()
    )

    logger.info(
        f"Successfully extracted "
        f"{patient_df.count()} records from PostgreSQL."
    )

    return patient_df