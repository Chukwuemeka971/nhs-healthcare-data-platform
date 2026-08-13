from pyspark.sql import DataFrame, SparkSession

from src.load.load_bronze import write_bronze
from src.utils.logger import get_logger


logger = get_logger(__name__)


def build_bronze_layer(
    spark: SparkSession,
    patients: DataFrame,
    filename: str,
) -> None:
    """
    Builds the Bronze layer.

    Responsibilities:
        - Pass source data to the Bronze writer.
        - Apply Bronze-layer processing.
        - Store the source filename as metadata.

    Args:
        spark:
            Active Spark session.

        patients:
            Patient admission records to process.

        filename:
            Name of the source file being processed.
    """

    logger.info(
        "Starting Bronze layer."
    )

    write_bronze(
        patients,
        filename,
        spark,
    )

    logger.info(
        "Bronze layer completed successfully."
    )