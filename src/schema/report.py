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

    if new_columns:
        logger.warning("New Columns Detected:")
        for column in new_columns:
            logger.warning(f"   + {column}")
    else:
        logger.info("No new columns.")

    if removed_columns:
        logger.warning("Removed Columns:")
        for column in removed_columns:
            logger.warning(f"   - {column}")
    else:
        logger.info("No removed columns.")

    if type_changes:
        logger.warning("Data Type Changes:")
        for change in type_changes:
            logger.warning(
                f"   {change['column']}: "
                f"{change['old_type']} -> "
                f"{change['new_type']}"
            )
    else:
        logger.info("No data type changes.")

    logger.info("=" * 60)