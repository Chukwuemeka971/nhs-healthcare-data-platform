import shutil
from pathlib import Path

from src.config.config import load_config
from src.utils.logger import get_logger


logger = get_logger(__name__)


def archive_file(
    filename: str
) -> Path:
    """
    Moves a successfully processed file
    from the Landing layer to the Archive layer.

    Args:
        filename:
            Name of the file to archive.

    Returns:
        Path to the archived file.

    Raises:
        FileNotFoundError:
            If the source file does not exist.
    """

    config = load_config()

    landing_path = Path(
        config["storage"]["landing"]
    )

    archive_path = Path(
        config["storage"]["archive"]
    )

    source_path = landing_path / filename
    destination_path = archive_path / filename

    if not source_path.exists():

        raise FileNotFoundError(
            f"Source file not found: {source_path}"
        )

    archive_path.mkdir(
        parents=True,
        exist_ok=True
    )

    logger.info(
        "Archiving file: %s",
        filename
    )

    shutil.move(
        str(source_path),
        str(destination_path)
    )

    logger.info(
        "File archived successfully: %s",
        destination_path
    )

    return destination_path