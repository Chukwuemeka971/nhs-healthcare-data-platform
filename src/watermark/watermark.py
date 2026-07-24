import json
import os

from src.utils.config import load_config

config = load_config()


def read_watermark():

    watermark_file = config["watermark"]["file"]

    if not os.path.exists(watermark_file):

        return None

    with open(watermark_file, "r") as file:

        try:

            return json.load(file)

        except json.JSONDecodeError:

            return None
    

#================================================================
# Updating the watermark
#================================================================
def update_watermark(process_date):

    watermark_file = config["watermark"]["file"]

    with open(watermark_file, "w") as file:

        json.dump(
            {
                "last_processed_date": process_date
            },
            file,
            indent=4
        )