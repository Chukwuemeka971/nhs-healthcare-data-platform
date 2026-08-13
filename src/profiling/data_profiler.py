from pyspark.sql import DataFrame


def profile_dataframe(df: DataFrame, dataset_name: str):
    """
    Generates a basic profiling report for a Spark DataFrame.
    """

    print("=" * 60)
    print("DATA PROFILING REPORT")
    print("=" * 60)

    print(f"Dataset : {dataset_name}")
    print(f"Rows     : {df.count()}")
    print(f"Columns  : {len(df.columns)}")

    print("\nColumn Names")
    print("-" * 60)

    for column in df.columns:
        print(column)

    print("=" * 60)