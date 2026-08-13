from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date

from src.utils.logger import get_logger

logger = get_logger(__name__)


VALID_TYPES = [
    "Emergency",
    "Elective",
    "Transfer",
    "Maternity"
]


def check_missing_patient_id(
    df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Checks for records with missing patient IDs.

    Returns:
        tuple:
            - valid_df: Records with a patient ID.
            - invalid_df: Records missing a patient ID.
    """

    logger.info("Checking for missing patient IDs.")

    invalid_df = df.filter(
        col("patient_id").isNull()
    )

    valid_df = df.filter(
        col("patient_id").isNotNull()
    )

    logger.info("Missing patient ID check completed.")

    return valid_df, invalid_df


def check_admission_type(
    df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Validates admission types.

    Returns:
        tuple:
            - valid_df: Records with valid admission types.
            - invalid_df: Records with invalid admission types.
    """

    logger.info("Checking admission types.")

    invalid_df = df.filter(
        ~col("admission_type").isin(VALID_TYPES)
    )

    valid_df = df.filter(
        col("admission_type").isin(VALID_TYPES)
    )

    logger.info("Admission type validation completed.")

    return valid_df, invalid_df


def check_future_admission_date(
    df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Identifies admissions with future dates.

    Returns:
        tuple:
            - valid_df: Records with valid admission dates.
            - invalid_df: Records with future admission dates.
    """

    logger.info("Checking for future admission dates.")

    invalid_df = df.filter(
        col("admission_date") > current_date()
    )

    valid_df = df.filter(
        col("admission_date") <= current_date()
    )

    logger.info("Future admission date check completed.")

    return valid_df, invalid_df


def check_missing_hospital(
    df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Checks for missing hospital values.

    Returns:
        tuple:
            - valid_df: Records with hospital values.
            - invalid_df: Records missing hospital values.
    """

    logger.info("Checking for missing hospital values.")

    invalid_df = df.filter(
        col("hospital").isNull()
    )

    valid_df = df.filter(
        col("hospital").isNotNull()
    )

    logger.info("Missing hospital check completed.")

    return valid_df, invalid_df


def check_duplicates(
    df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Identifies duplicate patient IDs.

    Returns:
        tuple:
            - valid_df: Records with unique patient IDs.
            - invalid_df: Duplicate patient records.
    """

    logger.info("Checking for duplicate patient IDs.")

    duplicate_ids = (
        df.groupBy("patient_id")
        .count()
        .filter(col("count") > 1)
        .select("patient_id")
    )

    invalid_df = (
        df.join(
            duplicate_ids,
            on="patient_id",
            how="inner"
        )
    )

    valid_df = (
        df.join(
            duplicate_ids,
            on="patient_id",
            how="left_anti"
        )
    )

    logger.info("Duplicate patient ID check completed.")

    return valid_df, invalid_df