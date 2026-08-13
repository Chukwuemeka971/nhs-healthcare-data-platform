import json
from pathlib import Path

from src.utils.logger import get_logger


logger = get_logger(__name__)

REGISTRY_PATH = Path(
    "src/metadata/processed_files.json"
)


def load_registry() -> list[str]:
    """
    Loads the processed file registry.

    Returns:
        A list of previously processed filenames.
    """

    if not REGISTRY_PATH.exists():

        logger.info(
            "Processed file registry does not exist yet."
        )

        return []

    try:

        with REGISTRY_PATH.open(
            "r",
            encoding="utf-8"
        ) as file:

            return json.load(file)

    except json.JSONDecodeError:

        logger.warning(
            "Processed file registry contains invalid JSON."
        )

        return []


def save_registry(
    files: list[str]
) -> None:
    """
    Saves the processed file registry.

    Args:
        files:
            List of processed filenames.
    """

    REGISTRY_PATH.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with REGISTRY_PATH.open(
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            files,
            file,
            indent=4
        )


def already_processed(
    filename: str
) -> bool:
    """
    Checks whether a file has already been processed.

    Args:
        filename:
            Name of the file to check.

    Returns:
        True if the file has already been processed.
    """

    return filename in load_registry()


def register_processed_file(
    filename: str
) -> None:
    """
    Registers a file as successfully processed.

    Args:
        filename:
            Name of the processed file.
    """

    files = load_registry()

    if filename in files:

        logger.info(
            "File is already registered: %s",
            filename
        )

        return

    files.append(filename)

    save_registry(files)

    logger.info(
        "Registered processed file: %s",
        filename
    )