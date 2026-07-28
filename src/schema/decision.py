from src.utils.logger import get_logger

logger = get_logger(__name__)


def evaluate_schema_changes(changes: dict):
    """
    Decide whether the pipeline should continue.

    Returns:
        {
            "approved": bool,
            "reason": str
        }
    """

    # Rule 1
    # Data type changes are dangerous
    if changes["type_changes"]:
        return {
            "approved": False,
            "reason": "Data type changes detected."
        }

    # Rule 2
    # Removed columns are dangerous
    if changes["removed_columns"]:
        return {
            "approved": False,
            "reason": "Columns have been removed."
        }

    # Rule 3
    # New columns are allowed
    if changes["new_columns"]:
        logger.warning(
            "New columns detected. "
            "Schema evolution approved."
        )

    return {
        "approved": True,
        "reason": "Schema approved."
    }