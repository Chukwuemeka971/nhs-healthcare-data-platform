from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    to_date,
    trim,
    when
)

from src.extract.extract_patients import extract_patient_data
from src.load.archive_file import archive_file
from src.metadata.processed_files import register_processed_file
from src.pipelines.bronze_pipeline import build_bronze_layer
from src.pipelines.gold_pipeline import build_gold_layer
from src.pipelines.silver_pipeline import build_silver_layer
from src.utils.logger import get_logger
from src.watermark.watermark import (
    read_watermark,
    update_watermark
)

logger = get_logger(__name__)


def run_pipeline(
    spark: SparkSession
) -> None:
    """
    Executes the complete Healthcare ETL pipeline.

    Workflow:

    Extract
        ↓
    Standardise
        ↓
    Watermark Check
        ↓
    Bronze Layer
        ↓
    Silver Layer
        ↓
    Gold Layer
        ↓
    Update Metadata
        ↓
    Archive Source File
    """

    logger.info(
        "Healthcare pipeline started."
    )

    # --------------------------------------------------
    # Extract
    # --------------------------------------------------

    patients, filename = extract_patient_data()

    if patients is None:

        logger.info(
            "No new files found."
        )

        return

    # --------------------------------------------------
    # Standardise
    # --------------------------------------------------

    patients = patients.withColumn(
        "admission_date",
        to_date(
            col("admission_date"),
            "yyyy-MM-dd"
        )
    )

    patients = patients.withColumn(
        "hospital",
        when(
            lower(
                trim(col("hospital"))
            ) == "nan",
            None
        ).otherwise(
            col("hospital")
        )
    )

    logger.info(
        "Extracted %d records.",
        patients.count()
    )

    # --------------------------------------------------
    # Watermark Check
    # --------------------------------------------------

    watermark = read_watermark()

    if (
        watermark is not None
        and watermark.get("last_processed_file") == filename
    ):

        logger.info(
            "%s has already been processed.",
            filename
        )

        return

    # --------------------------------------------------
    # Bronze
    # --------------------------------------------------

    build_bronze_layer(
        spark,
        patients,
        filename
    )

    # --------------------------------------------------
    # Silver
    # --------------------------------------------------

    build_silver_layer(
        spark,
        patients
    )

    # --------------------------------------------------
    # Gold
    # --------------------------------------------------

    build_gold_layer(
        spark
    )

    # --------------------------------------------------
    # Metadata
    # --------------------------------------------------

    update_watermark(
        filename,
        datetime.now().isoformat()
    )

    logger.info(
        "Watermark updated."
    )

    register_processed_file(
        filename
    )

    logger.info(
        "%s registered successfully.",
        filename
    )

    # --------------------------------------------------
    # Archive
    # --------------------------------------------------

    archive_file(
        filename
    )

    logger.info(
        "%s archived successfully.",
        filename
    )

    logger.info(
        "Healthcare pipeline completed successfully."
    )