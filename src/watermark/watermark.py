import json
import os

from src.config.config import load_config

config = load_config()
WATERMARK_FILE = config["watermark"]["file"]


def read_watermark():
    """
    Read the watermark file.
    Returns a dictionary or None if it doesn't exist.
    """
    if not os.path.exists(WATERMARK_FILE):
        return None

    with open(WATERMARK_FILE, "r") as f:
        return json.load(f)


def update_watermark(filename, processed_at):
    """
    Save the latest processed file information.
    """
    watermark = {
        "last_processed_file": filename,
        "processed_at": processed_at
    }

    with open(WATERMARK_FILE, "w") as f:
        json.dump(watermark, f, indent=4)