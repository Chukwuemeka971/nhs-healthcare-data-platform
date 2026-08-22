from pyspark.sql import DataFrame

from src.utils.logger import get_logger


logger = get_logger(__name__)


def check_dimension_quality(
    df: DataFrame,
    dimension_name: str,
    primary_key: str,
) -> dict:
    """
    Runs data quality checks on a dimension table.

    Checks:
        - Dimension is not empty
        - Null primary keys
        - Duplicate primary keys

    Returns:
        Dictionary containing quality results.
    """

    logger.info(
        "Running quality checks for %s.",
        dimension_name,
    )

    # -----------------------------------------
    # Total Records
    # -----------------------------------------

    total_records = df.count()

    # -----------------------------------------
    # Empty Dimension Check
    # -----------------------------------------

    is_empty = total_records == 0

    # -----------------------------------------
    # Null Primary Key Check
    # -----------------------------------------

    null_primary_keys = (
        df.filter(
            df[primary_key].isNull()
        ).count()
    )

    # -----------------------------------------
    # Duplicate Primary Key Check
    # -----------------------------------------

    duplicate_primary_keys = (
        df.groupBy(primary_key)
        .count()
        .filter("count > 1")
        .count()
    )

    # -----------------------------------------
    # Overall Status
    # -----------------------------------------

    passed = (
        not is_empty
        and null_primary_keys == 0
        and duplicate_primary_keys == 0
    )

    results = {
        "dimension": dimension_name,
        "total_records": total_records,
        "is_empty": is_empty,
        "null_primary_keys": null_primary_keys,
        "duplicate_primary_keys": duplicate_primary_keys,
        "status": "PASS" if passed else "FAIL",
    }

    logger.info(
        "%s quality check completed. Status: %s",
        dimension_name,
        results["status"],
    )

    return results