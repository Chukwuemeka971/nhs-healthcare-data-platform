from pyspark.sql import SparkSession

from src.gold.dim_date import create_date_dimension
from src.gold.dim_department import build_dim_department
from src.gold.dim_hospital import build_dim_hospital
from src.gold.dim_patient import build_dim_patient
from src.gold.fact_patient_admissions import (
    build_fact_patient_admissions
)

from src.utils.logger import get_logger

logger = get_logger(__name__)


def build_gold_layer(
    spark: SparkSession
) -> None:
    """
    Executes the complete Gold layer.
    """

    logger.info(
        "Starting Gold layer."
    )

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

    build_fact_patient_admissions(
        spark
    )

    logger.info(
        "Gold layer completed successfully."
    )