from pyspark.sql import DataFrame

from src.utils.logger import get_logger
from src.validate.quality_rules import (
    check_admission_type,
    check_duplicate_episode_ids,
    check_future_admission_date,
    check_missing_hospital,
    check_missing_patient_id,
)

logger = get_logger(__name__)


RULES = [
    check_missing_patient_id,
    check_missing_hospital,
    check_duplicate_episode_ids,
    check_future_admission_date,
    check_admission_type,
]


def run_quality_checks(
    df: DataFrame
) -> tuple[DataFrame, list[DataFrame]]:
    """
    Executes all configured quality rules.

    Each rule returns:
        - valid DataFrame
        - invalid DataFrame

    The valid DataFrame from one rule becomes the
    input to the next rule.

    Returns:
        tuple:
            - Clean DataFrame
            - List of invalid DataFrames
    """

    logger.info("Starting data quality checks.")

    invalid_records = []

    current_df = df

    for rule in RULES:

        logger.info(f"Running rule: {rule.__name__}")

        current_df, invalid_df = rule(current_df)

        invalid_records.append(invalid_df)

    logger.info("All quality checks completed successfully.")

    return current_df, invalid_records