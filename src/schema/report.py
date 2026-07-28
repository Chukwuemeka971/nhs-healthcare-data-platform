from src.utils.logger import get_logger

logger = get_logger(__name__)


def log_schema_changes(changes: dict):
    """
    Log schema comparison results in a readable format.
    """

    new_columns = changes["new_columns"]
    removed_columns = changes["removed_columns"]
    type_changes = changes["type_changes"]

    logger.info("=" * 60)
    logger.info("SCHEMA EVOLUTION REPORT")
    logger.info("=" * 60)

    # New columns
    if new_columns:
        logger.warning("New Columns Detected:")
        for col in new_columns:
            logger.warning(f"   + {col}")
    else:
        logger.info("No new columns.")

    # Removed columns
    if removed_columns:
        logger.warning("Removed Columns:")
        for col in removed_columns:
            logger.warning(f"   - {col}")
    else:
        logger.info("No removed columns.")

    # Data type changes
    if type_changes:
        logger.warning("Data Type Changes:")
        for change in type_changes:
            logger.warning(
                f"   {change['column']}: "
                f"{change['old_type']} -> {change['new_type']}"
            )
    else:
        logger.info("No data type changes.")

    logger.info("=" * 60)