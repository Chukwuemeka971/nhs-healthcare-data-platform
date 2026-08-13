from pyspark.sql import DataFrame, SparkSession

from src.load.load_bronze import write_bronze
from src.utils.logger import get_logger

logger = get_logger(__name__)


def build_bronze_layer(
    spark: SparkSession,
    patients: DataFrame,
    filename: str
) -> None:
    """
    Executes the complete Bronze layer.

    Responsibilities:
    - Schema governance
    - Add Bronze metadata
    - Write to Bronze
    """

    logger.info(
        "Starting Bronze layer."
    )

    write_bronze(
        patients,
        filename,
        spark
    )

    logger.info(
        "Bronze layer completed successfully."
    )