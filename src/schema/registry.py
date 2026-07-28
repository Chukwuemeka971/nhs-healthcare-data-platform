import json
import os
from datetime import datetime, UTC

from src.utils.config import load_config

config = load_config()


REGISTRY_FILE = "data/schema_registry.json"


def register_schema_change(changes: dict):

    record = {
        "timestamp": datetime.now(UTC).isoformat(),
        "new_columns": changes["new_columns"],
        "removed_columns": changes["removed_columns"],
        "type_changes": changes["type_changes"]
    }

    os.makedirs("data", exist_ok=True)

    if os.path.exists(REGISTRY_FILE):

        with open(REGISTRY_FILE, "r") as f:
            registry = json.load(f)

    else:

        registry = []

    registry.append(record)

    with open(REGISTRY_FILE, "w") as f:
        json.dump(registry, f, indent=4)