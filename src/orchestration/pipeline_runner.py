from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    to_date,
    trim,
    when,
)

from src.extract.extract_patients import extract_patient_data
from src.load.archive_file import archive_file
from src.metadata.pipeline_state import (
    initialize_file_state,
    is_layer_successful,
    is_pipeline_complete,
    update_layer_state,
)
from src.metadata.processed_files import register_processed_file
from src.pipelines.bronze_pipeline import build_bronze_layer
from src.pipelines.gold_pipeline import build_gold_layer
from src.pipelines.silver_pipeline import build_silver_layer
from src.utils.logger import get_logger
from src.watermark.watermark import (
    read_watermark,
    update_watermark,
)

logger = get_logger(__name__)


def run_pipeline(
    spark: SparkSession,
) -> None:
    """
    Executes the complete Healthcare ETL pipeline.

    Workflow:

        Extract
            ↓
        Standardise
            ↓
        Pipeline State
            ↓
        Watermark Check
            ↓
        Bronze
            ↓
        Silver
            ↓
        Gold
            ↓
        Update Metadata
            ↓
        Archive Source File

    Pipeline state allows failed runs to resume from
    the layer where processing stopped.
    """

    logger.info(
        "Healthcare pipeline started."
    )

    # --------------------------------------------------
    # Extract
    # --------------------------------------------------

    patients, filename = extract_patient_data(
        spark
    )

    if patients is None:

        logger.info(
            "No new files found."
        )

        return

    # --------------------------------------------------
    # Initialize Pipeline State
    # --------------------------------------------------

    initialize_file_state(
        filename
    )

    # --------------------------------------------------
    # Standardise
    # --------------------------------------------------

    patients = patients.withColumn(
        "admission_date",
        to_date(
            col("admission_date"),
            "yyyy-MM-dd",
        ),
    )

    patients = patients.withColumn(
        "hospital",
        when(
            lower(
                trim(col("hospital"))
            ) == "nan",
            None,
        ).otherwise(
            col("hospital")
        ),
    )

    logger.info(
        "Extracted %d records.",
        patients.count(),
    )

    # --------------------------------------------------
    # Watermark Check
    # --------------------------------------------------

    watermark = read_watermark()

    if (
        watermark is not None
        and watermark.get(
            "last_processed_file"
        ) == filename
        and is_pipeline_complete(
            filename
        )
    ):

        logger.info(
            "%s has already completed the pipeline.",
            filename,
        )

        return

    # ==================================================
    # BRONZE
    # ==================================================

    if is_layer_successful(
        filename,
        "bronze",
    ):

        logger.info(
            "Bronze already completed for %s. "
            "Skipping Bronze.",
            filename,
        )

    else:

        try:

            logger.info(
                "Starting Bronze layer for %s.",
                filename,
            )

            build_bronze_layer(
                spark,
                patients,
                filename,
            )

            update_layer_state(
                filename,
                "bronze",
                "SUCCESS",
            )

            logger.info(
                "Bronze completed successfully."
            )

        except Exception:

            update_layer_state(
                filename,
                "bronze",
                "FAILED",
            )

            logger.exception(
                "Bronze layer failed."
            )

            raise

    # ==================================================
    # SILVER
    # ==================================================

    if is_layer_successful(
        filename,
        "silver",
    ):

        logger.info(
            "Silver already completed for %s. "
            "Skipping Silver.",
            filename,
        )

    else:

        try:

            logger.info(
                "Starting Silver layer for %s.",
                filename,
            )

            build_silver_layer(
                spark,
                patients,
            )

            update_layer_state(
                filename,
                "silver",
                "SUCCESS",
            )

            logger.info(
                "Silver completed successfully."
            )

        except Exception:

            update_layer_state(
                filename,
                "silver",
                "FAILED",
            )

            logger.exception(
                "Silver layer failed."
            )

            raise

    # ==================================================
    # GOLD
    # ==================================================

    if is_layer_successful(
        filename,
        "gold",
    ):

        logger.info(
            "Gold already completed for %s. "
            "Skipping Gold.",
            filename,
        )

    else:

        try:

            logger.info(
                "Starting Gold layer for %s.",
                filename,
            )

            build_gold_layer(
                spark
            )

            update_layer_state(
                filename,
                "gold",
                "SUCCESS",
            )

            logger.info(
                "Gold completed successfully."
            )

        except Exception:

            update_layer_state(
                filename,
                "gold",
                "FAILED",
            )

            logger.exception(
                "Gold layer failed."
            )

            raise

    # ==================================================
    # VERIFY PIPELINE COMPLETION
    # ==================================================

    if not is_pipeline_complete(
        filename
    ):

        raise RuntimeError(
            f"Pipeline state indicates that "
            f"{filename} has not completed all layers."
        )

    logger.info(
        "All pipeline layers completed successfully."
    )

    # --------------------------------------------------
    # Update Watermark
    # --------------------------------------------------

    update_watermark(
        filename,
        datetime.now().isoformat(),
    )

    logger.info(
        "Watermark updated."
    )

    # --------------------------------------------------
    # Register Processed File
    # --------------------------------------------------

    register_processed_file(
        filename,
    )

    logger.info(
        "%s registered successfully.",
        filename,
    )

    # --------------------------------------------------
    # Archive
    # --------------------------------------------------

    archive_file(
        filename,
    )

    logger.info(
        "%s archived successfully.",
        filename,
    )

    logger.info(
        "Healthcare pipeline completed successfully."
    )