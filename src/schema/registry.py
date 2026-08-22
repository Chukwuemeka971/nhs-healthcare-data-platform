import json
import os
from datetime import UTC, datetime


REGISTRY_FILE = "data/schema_registry.json"


def register_schema_change(
    changes: dict
) -> None:
    """
    Records an approved schema change
    in the schema registry.
    """

    record = {
        "timestamp": datetime.now(UTC).isoformat(),
        "new_columns": changes["new_columns"],
        "removed_columns": changes["removed_columns"],
        "type_changes": changes["type_changes"],
    }

    os.makedirs(
        "data",
        exist_ok=True,
    )

    if os.path.exists(
        REGISTRY_FILE
    ):

        with open(
            REGISTRY_FILE,
            "r",
            encoding="utf-8",
        ) as file:

            registry = json.load(file)

    else:

        registry = []

    registry.append(
        record
    )

    with open(
        REGISTRY_FILE,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            registry,
            file,
            indent=4,
        )

    