import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.utils.logger import get_logger


# Ensure Spark driver and workers use the same
# Python interpreter.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


logger = get_logger(__name__)


def get_spark(app_name: str) -> SparkSession:
    """
    Creates and returns a SparkSession configured
    for Delta Lake.

    Args:
        app_name:
            Name of the Spark application.

    Returns:
        Configured SparkSession.
    """

    logger.info(
        "Starting Spark session: %s",
        app_name,
    )

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.jars",
            "jars/postgresql-42.7.3.jar",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "2",
        )
        .config(
            "spark.default.parallelism",
            "2",
        )
        .config(
            "spark.ui.enabled",
            "false",
        )
    )

    spark = (
        configure_spark_with_delta_pip(builder)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    logger.info(
        "Spark session created successfully."
    )

    return spark