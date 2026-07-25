from pyspark.sql.functions import col


def split_records(
    incoming_df,
    silver_df
):
    """
    Splits incoming records into:

    - New records
    - Updated records
    - Unchanged records
    """

    if silver_df is None:

        return incoming_df, None, None

    # New patients
    new_records = (
        incoming_df.join(
            silver_df.select("patient_id"),
            on="patient_id",
            how="left_anti"
        )
    )

    # Existing patients
    existing = (
        incoming_df.alias("incoming")
        .join(
            silver_df.alias("silver"),
            on="patient_id",
            how="inner"
        )
    )

    # Records whose values changed
    updated_records = existing.filter(

        (col("incoming.ward") != col("silver.ward")) |

        (col("incoming.consultant") != col("silver.consultant")) |

        (col("incoming.hospital") != col("silver.hospital"))

    ).select("incoming.*")

    # Records that didn't change
    unchanged_records = existing.filter(

        (col("incoming.ward") == col("silver.ward")) &

        (col("incoming.consultant") == col("silver.consultant")) &

        (col("incoming.hospital") == col("silver.hospital"))

    ).select("incoming.*")

    return (
        new_records,
        updated_records,
        unchanged_records
    )