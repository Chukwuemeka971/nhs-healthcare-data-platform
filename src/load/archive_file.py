import os
import shutil

from src.config.config import load_config

config = load_config()


def archive_file(filename):
    """
    Move a successfully processed file
    from Landing to Archive.
    """

    landing = config["storage"]["landing"]
    archive = config["storage"]["archive"]

    os.makedirs(archive, exist_ok=True)

    source = os.path.join(landing, filename)
    destination = os.path.join(archive, filename)

    shutil.move(source, destination)