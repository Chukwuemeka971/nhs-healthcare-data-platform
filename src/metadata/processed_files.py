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
    """

    if not REGISTRY_PATH.exists():
        return []

    with REGISTRY_PATH.open(
        "r",
        encoding="utf-8"
    ) as file:

        return json.load(file)


def save_registry(
    files: list[str]
) -> None:
    """
    Saves the processed file registry.
    """

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
    Checks whether a file
    has already been processed.
    """

    return filename in load_registry()


def register_processed_file(
    filename: str
) -> None:
    """
    Registers a processed file.
    """

    files = load_registry()

    if filename not in files:

        files.append(
            filename
        )

        save_registry(files)

        logger.info(
            f"Registered {filename}"
        )