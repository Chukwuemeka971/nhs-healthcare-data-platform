import json
import os

from delta.tables import DeltaTable

from src.config.config import load_config
from src.utils.spark import get_spark


def get_delta_count(
    spark,
    table_name: str,
    table_path: str,
) -> int | None:

    print("\n" + "=" * 70)
    print(table_name.upper())
    print("=" * 70)

    if not DeltaTable.isDeltaTable(
        spark,
        table_path,
    ):
        print("Status: NOT FOUND")
        return None

    df = (
        spark.read
        .format("delta")
        .load(table_path)
    )

    count = df.count()

    print("Status: OK")
    print(f"Records: {count}")

    return count


def verify_schema_registry() -> None:

    print("\n" + "=" * 70)
    print("SCHEMA REGISTRY")
    print("=" * 70)

    registry_path = "data/schema_registry.json"

    if not os.path.exists(registry_path):
        print("Status: NOT FOUND")
        return

    with open(
        registry_path,
        "r",
        encoding="utf-8",
    ) as file:

        registry = json.load(file)

    print("Status: OK")
    print(f"Schema changes recorded: {len(registry)}")

    for change in registry:

        print("\nNew columns:")
        print(change.get("new_columns", []))

        print("Removed columns:")
        print(change.get("removed_columns", []))

        print("Type changes:")
        print(change.get("type_changes", []))


def verify_silver_duplicates(
    spark,
    silver_path: str,
) -> None:

    print("\n" + "=" * 70)
    print("SILVER DUPLICATE CHECK")
    print("=" * 70)

    if not DeltaTable.isDeltaTable(
        spark,
        silver_path,
    ):
        print("Status: SILVER TABLE NOT FOUND")
        return

    silver_df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    duplicates = (
        silver_df
        .groupBy("patient_id")
        .count()
        .filter("count > 1")
    )

    duplicate_count = duplicates.count()

    print(
        f"Duplicate patient IDs: {duplicate_count}"
    )

    if duplicate_count == 0:

        print(
            "Status: PASS - No duplicate patient IDs."
        )

    else:

        print(
            "Status: FAIL - Duplicate patient IDs found."
        )

        duplicates.show(
            truncate=False,
        )


def verify_quarantine(
    spark,
    quarantine_path: str,
) -> None:

    print("\n" + "=" * 70)
    print("QUARANTINE")
    print("=" * 70)

    if not os.path.exists(quarantine_path):

        print("Status: NOT FOUND")
        return

    total_records = 0

    rule_folders = sorted(
        folder
        for folder in os.listdir(quarantine_path)
        if os.path.isdir(
            os.path.join(
                quarantine_path,
                folder,
            )
        )
    )

    if not rule_folders:

        print("Status: NO QUARANTINE DATA")
        return

    for rule_name in rule_folders:

        rule_path = os.path.join(
            quarantine_path,
            rule_name,
        )

        try:

            df = (
                spark.read
                .parquet(rule_path)
            )

            count = df.count()

            total_records += count

            print(
                f"{rule_name}: {count} record(s)"
            )

        except Exception as error:

            print(
                f"{rule_name}: ERROR - {error}"
            )

    print(
        f"\nTotal quarantined records: {total_records}"
    )


def verify_processed_files() -> None:

    print("\n" + "=" * 70)
    print("PROCESSED FILES")
    print("=" * 70)

    registry_path = (
        "src/metadata/processed_files.json"
    )

    if not os.path.exists(registry_path):

        print("Status: NOT FOUND")
        return

    with open(
        registry_path,
        "r",
        encoding="utf-8",
    ) as file:

        files = json.load(file)

    print(
        f"Processed files: {len(files)}"
    )

    for filename in files:

        print(f"- {filename}")


def verify_watermark(
    watermark_path: str,
) -> None:

    print("\n" + "=" * 70)
    print("WATERMARK")
    print("=" * 70)

    if not os.path.exists(watermark_path):

        print("Status: NOT FOUND")
        return

    with open(
        watermark_path,
        "r",
        encoding="utf-8",
    ) as file:

        watermark = json.load(file)

    print("Status: OK")
    print(
        "Last processed file:",
        watermark.get("last_processed_file"),
    )
    print(
        "Processed at:",
        watermark.get("processed_at"),
    )


def main():

    spark = None

    try:

        spark = get_spark(
            "Healthcare Pipeline Final Verification"
        )

        config = load_config()

        dataset_name = (
            config["datasets"][
                "patient_admissions"
            ]
        )

        bronze_path = os.path.join(
            config["storage"]["bronze"],
            dataset_name,
        )

        silver_path = os.path.join(
            config["storage"]["silver"],
            dataset_name,
        )

        gold_path = (
            config["storage"]["gold"]
        )

        quarantine_path = (
            config["storage"]["quarantine"]
        )

        watermark_path = (
            config["watermark"]["file"]
        )

        print("\n")
        print("=" * 70)
        print("HEALTHCARE PIPELINE FINAL VERIFICATION")
        print("=" * 70)

        # Schema
        verify_schema_registry()

        # Bronze
        get_delta_count(
            spark,
            "Bronze",
            bronze_path,
        )

        # Silver
        get_delta_count(
            spark,
            "Silver",
            silver_path,
        )

        verify_silver_duplicates(
            spark,
            silver_path,
        )

        # Quarantine
        verify_quarantine(
            spark,
            quarantine_path,
        )

        # Gold
        get_delta_count(
            spark,
            "Gold - dim_patient",
            os.path.join(
                gold_path,
                "dim_patient",
            ),
        )

        get_delta_count(
            spark,
            "Gold - dim_hospital",
            os.path.join(
                gold_path,
                "dim_hospital",
            ),
        )

        get_delta_count(
            spark,
            "Gold - dim_department",
            os.path.join(
                gold_path,
                "dim_department",
            ),
        )

        get_delta_count(
            spark,
            "Gold - dim_date",
            os.path.join(
                gold_path,
                "dim_date",
            ),
        )

        get_delta_count(
            spark,
            "Gold - fact_patient_admissions",
            os.path.join(
                gold_path,
                "fact_patient_admissions",
            ),
        )

        # Metadata
        verify_processed_files()

        verify_watermark(
            watermark_path,
        )

        print("\n" + "=" * 70)
        print("FINAL VERIFICATION COMPLETED")
        print("=" * 70)

    finally:

        if spark is not None:

            spark.stop()


if __name__ == "__main__":

    main()