from pathlib import Path

import yaml

from src.utils.logger import get_logger

logger = get_logger(__name__)


def load_config() -> dict:
    """
    Loads the project configuration from config.yaml.

    Returns:
        dict: Parsed configuration dictionary.
    """

    config_path = Path("configs/config.yaml")

    try:

        with config_path.open(
            "r",
            encoding="utf-8"
        ) as file:

            logger.info("Loading configuration.")

            return yaml.safe_load(file)

    except FileNotFoundError:

        logger.error(
            f"Configuration file not found: {config_path}"
        )
        raise

    except yaml.YAMLError as error:

        logger.error(
            f"Invalid YAML configuration: {error}"
        )
        raise