from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def check_null_surrogate_keys(
    fact_df: DataFrame
):
    """
    Raises an exception if any surrogate key is NULL.
    """

    surrogate_keys = [
        "patient_key",
        "hospital_key",
        "department_key",
        "date_key"
    ]

    for key in surrogate_keys:

        null_count = (
            fact_df
            .filter(col(key).isNull())
            .count()
        )

        if null_count > 0:

            raise ValueError(
                f"{key} contains {null_count} NULL values."
            )

    print("✓ All surrogate key checks passed.")


def check_row_count(
    silver_df: DataFrame,
    fact_df: DataFrame
):
    """
    Ensures the fact table contains the same
    number of rows as the Silver layer.
    """

    silver_count = silver_df.count()

    fact_count = fact_df.count()

    if silver_count != fact_count:

        raise ValueError(
            f"""
            Row count validation failed.

            Silver rows : {silver_count}

            Fact rows   : {fact_count}
            """
        )

    print(
        f"✓ Row count validation passed ({fact_count} rows)."
    )


def check_foreign_key_relationship(
    fact_df: DataFrame,
    dimension_df: DataFrame,
    fact_key: str,
    dimension_key: str
):
    """
    Ensures every foreign key in the fact table
    exists in its corresponding dimension.
    """

    missing = (
        fact_df.alias("f")
        .join(
            dimension_df.alias("d"),
            col(f"f.{fact_key}") == col(f"d.{dimension_key}"),
            "leftanti"
        )
    )

    missing_count = missing.count()

    if missing_count > 0:

        raise ValueError(
            f"""
            Referential Integrity Failed

            {missing_count} missing values found for

            {fact_key}
            """
        )

    print(
        f"✓ Referential integrity passed for {fact_key}"
    )