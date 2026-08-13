from pyspark.sql import DataFrame

from src.utils.logger import get_logger

logger = get_logger(__name__)


REQUIRED_COLUMNS = [
    "patient_id",
    "patient_name",
    "age",
    "gender",
    "hospital",
    "ward",
    "consultant",
    "admission_date",
    "admission_type"
]


def validate_required_columns(
    df: DataFrame
) -> DataFrame:
    """
    Validates that all required columns
    exist in the input DataFrame.

    Args:
        df: Input Spark DataFrame.

    Returns:
        The original DataFrame if validation succeeds.

    Raises:
        ValueError:
            If one or more required columns are missing.
    """

    logger.info("Validating required columns.")

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:

        logger.error(
            f"Missing required columns: {missing_columns}"
        )

        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    logger.info("Required column validation passed.")

    return df