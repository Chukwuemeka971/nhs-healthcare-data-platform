from pyspark.sql import DataFrame


def merge_records(
    silver_df: DataFrame,
    new_records: DataFrame,
    updated_records: DataFrame
):
    """
    Merge new and updated records into the Silver layer
    using SCD Type 1 logic.
    """

    # First load
    if silver_df is None:
        return new_records

    # Remove records that will be updated
    if updated_records is not None:

        silver_df = silver_df.join(
            updated_records.select("patient_id"),
            on="patient_id",
            how="left_anti"
        )

    # Append updated records
    if updated_records is not None:

        silver_df = silver_df.unionByName(
            updated_records
        )

    # Append new records
    if new_records is not None:

        silver_df = silver_df.unionByName(
            new_records
        )

    return silver_df