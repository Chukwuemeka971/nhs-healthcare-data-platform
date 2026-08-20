import os

from src.config.config import load_config
from src.utils.spark import get_spark


spark = get_spark(
    "Verify Quarantine"
)

config = load_config()

quarantine_path = config["storage"]["quarantine"]

print(
    f"Quarantine path: {quarantine_path}"
)

print("\n" + "=" * 60)

rule_folders = [
    folder
    for folder in os.listdir(quarantine_path)
    if os.path.isdir(
        os.path.join(
            quarantine_path,
            folder
        )
    )
]

total_records = 0

for rule_name in rule_folders:

    rule_path = os.path.join(
        quarantine_path,
        rule_name
    )

    print(
        f"\nQuarantine rule: {rule_name}"
    )

    try:

        df = (
            spark.read
            .parquet(rule_path)
        )

        record_count = df.count()

        total_records += record_count

        print(
            f"Records quarantined: {record_count}"
        )

        df.show(
            truncate=False
        )

    except Exception as error:

        print(
            f"Could not read records: {error}"
        )

print("\n" + "=" * 60)

print(
    f"Total quarantined records: {total_records}"
)

spark.stop()