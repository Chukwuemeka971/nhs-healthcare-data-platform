import os

from delta.tables import DeltaTable

from src.config.config import load_config
from src.utils.spark import get_spark


def verify_table(
    spark,
    table_name: str,
    table_path: str,
) -> None:

    print("\n" + "=" * 70)
    print(f"TABLE: {table_name}")
    print(f"PATH: {table_path}")
    print("=" * 70)

    if not DeltaTable.isDeltaTable(
        spark,
        table_path,
    ):

        print(
            "ERROR: Delta table does not exist."
        )

        return

    df = (
        spark.read
        .format("delta")
        .load(table_path)
    )

    print(
        f"Record count: {df.count()}"
    )

    print("\nSchema:")

    df.printSchema()

    print("\nSample records:")

    df.show(
        10,
        truncate=False,
    )


def main():

    spark = None

    try:

        spark = get_spark(
            "Verify Gold Layer"
        )

        config = load_config()

        gold_path = config["storage"]["gold"]

        tables = {
            "dim_patient": os.path.join(
                gold_path,
                "dim_patient",
            ),
            "dim_hospital": os.path.join(
                gold_path,
                "dim_hospital",
            ),
            "dim_department": os.path.join(
                gold_path,
                "dim_department",
            ),
            "dim_date": os.path.join(
                gold_path,
                "dim_date",
            ),
            "fact_patient_admissions": os.path.join(
                gold_path,
                "fact_patient_admissions",
            ),
        }

        print("\nVERIFYING GOLD LAYER")

        for table_name, table_path in tables.items():

            verify_table(
                spark,
                table_name,
                table_path,
            )

        print("\n" + "=" * 70)
        print("GOLD LAYER VERIFICATION COMPLETED")
        print("=" * 70)

    finally:

        if spark is not None:

            spark.stop()


if __name__ == "__main__":

    main()