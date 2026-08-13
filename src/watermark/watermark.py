import json
from pathlib import Path

from src.config.config import load_config
from src.utils.logger import get_logger


logger = get_logger(__name__)


def get_watermark_path() -> Path:
    """
    Returns the configured watermark file path.
    """

    config = load_config()

    return Path(
        config["watermark"]["file"]
    )


def read_watermark() -> dict | None:
    """
    Reads the watermark file.

    Returns:
        The watermark dictionary, or None if the file
        does not exist.
    """

    watermark_path = get_watermark_path()

    if not watermark_path.exists():

        logger.info(
            "Watermark file does not exist yet."
        )

        return None

    try:

        with watermark_path.open(
            "r",
            encoding="utf-8"
        ) as file:

            return json.load(file)

    except json.JSONDecodeError:

        logger.warning(
            "Watermark file contains invalid JSON."
        )

        return None


def update_watermark(
    filename: str,
    processed_at: str,
) -> None:
    """
    Saves the latest processed file information.

    Args:
        filename:
            Name of the latest successfully processed file.

        processed_at:
            Timestamp when processing completed.
    """

    watermark_path = get_watermark_path()

    watermark_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    watermark = {
        "last_processed_file": filename,
        "processed_at": processed_at,
    }

    with watermark_path.open(
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            watermark,
            file,
            indent=4,
        )

    logger.info(
        "Watermark updated for file: %s",
        filename,
    )