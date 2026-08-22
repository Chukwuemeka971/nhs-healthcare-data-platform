import json
import os
from datetime import UTC, datetime

from src.utils.logger import get_logger


logger = get_logger(__name__)


STATE_FILE = "data/pipeline_state.json"


def load_pipeline_state() -> dict:
    """
    Loads the pipeline state file.

    Returns an empty dictionary if the file
    does not exist yet.
    """

    if not os.path.exists(
        STATE_FILE
    ):
        return {}

    with open(
        STATE_FILE,
        "r",
        encoding="utf-8",
    ) as file:

        return json.load(file)


def save_pipeline_state(
    state: dict,
) -> None:
    """
    Saves the pipeline state to disk.
    """

    os.makedirs(
        os.path.dirname(
            STATE_FILE
        ),
        exist_ok=True,
    )

    with open(
        STATE_FILE,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            state,
            file,
            indent=4,
        )


def initialize_file_state(
    filename: str,
) -> None:
    """
    Creates pipeline state tracking for a file
    if it does not already exist.
    """

    state = load_pipeline_state()

    if filename not in state:

        state[filename] = {
            "bronze": "PENDING",
            "silver": "PENDING",
            "gold": "PENDING",
            "updated_at": (
                datetime.now(UTC).isoformat()
            ),
        }

        save_pipeline_state(
            state
        )

        logger.info(
            "Pipeline state initialized for file: %s",
            filename,
        )


def update_layer_state(
    filename: str,
    layer: str,
    status: str,
) -> None:
    """
    Updates the processing status of a pipeline layer.

    Example statuses:

    PENDING
    SUCCESS
    FAILED
    """

    state = load_pipeline_state()

    if filename not in state:

        initialize_file_state(
            filename
        )

        state = load_pipeline_state()

    state[filename][layer] = status

    state[filename]["updated_at"] = (
        datetime.now(UTC).isoformat()
    )

    save_pipeline_state(
        state
    )

    logger.info(
        "%s status for %s updated to %s.",
        layer.capitalize(),
        filename,
        status,
    )


def get_layer_state(
    filename: str,
    layer: str,
) -> str:
    """
    Returns the current status of a layer
    for a specific file.
    """

    state = load_pipeline_state()

    if filename not in state:

        return "PENDING"

    return state[filename].get(
        layer,
        "PENDING",
    )


def is_layer_successful(
    filename: str,
    layer: str,
) -> bool:
    """
    Returns True if the specified layer
    completed successfully.
    """

    return (
        get_layer_state(
            filename,
            layer,
        )
        == "SUCCESS"
    )


def is_pipeline_complete(
    filename: str,
) -> bool:
    """
    Returns True only when Bronze, Silver,
    and Gold have all completed successfully.
    """

    return all(
        is_layer_successful(
            filename,
            layer,
        )
        for layer in [
            "bronze",
            "silver",
            "gold",
        ]
    )