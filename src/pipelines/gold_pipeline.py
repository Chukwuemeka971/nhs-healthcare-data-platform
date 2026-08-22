from pyspark.sql import SparkSession

from src.gold.dim_date import create_date_dimension
from src.gold.dim_department import build_dim_department
from src.gold.dim_hospital import build_dim_hospital
from src.gold.dim_patient import build_dim_patient
from src.gold.fact_patient_admissions import (
    build_fact_patient_admissions,
)
from src.quality.quality_runner import run_gold_quality_checks
from src.utils.logger import get_logger


logger = get_logger(__name__)


def build_gold_layer(
    spark: SparkSession,
) -> None:
    
    """
    Builds the complete Gold layer and validates
    dimension and fact table quality.
    """

    logger.info(
        "Starting Gold layer."
    )

    # -----------------------------------------
    # Build Gold dimensions
    # -----------------------------------------

    build_dim_hospital(
        spark
    )

    build_dim_department(
        spark
    )

    build_dim_patient(
        spark
    )

    create_date_dimension(
        spark
    )

    # -----------------------------------------
    # Build Gold fact table
    # -----------------------------------------

    build_fact_patient_admissions(
        spark
    )

    logger.info(
        "Gold tables built successfully."
    )

    # -----------------------------------------
    # Gold Data Quality
    # -----------------------------------------

    quality_results = run_gold_quality_checks(
        spark
    )

    # -----------------------------------------
    # Quality Summary
    # -----------------------------------------

    logger.info(
        "Gold quality results:"
    )

    for result in quality_results:

        logger.info(
            "%s: %s",
            result.get(
                "dimension",
                result.get(
                    "table",
                    "unknown",
                ),
            ),
            result["status"],
        )

    logger.info(
        "Gold layer completed successfully."
    )