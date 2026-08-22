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
        "date_key",
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


def check_duplicate_episode_ids(
    fact_df: DataFrame
):
    """
    Ensures episode_id is unique in the fact table.
    """

    duplicate_count = (
        fact_df
        .groupBy("episode_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    if duplicate_count > 0:

        raise ValueError(
            f"""
            Fact table duplicate check failed.

            {duplicate_count} duplicate episode_id values found.
            """
        )

    print(
        "✓ Episode ID uniqueness check passed."
    )


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
            col(f"f.{fact_key}")
            == col(f"d.{dimension_key}"),
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


def check_fact_quality(
    silver_df: DataFrame,
    fact_df: DataFrame,
    patient_df: DataFrame,
    hospital_df: DataFrame,
    department_df: DataFrame,
    date_df: DataFrame
):
    """
    Runs all fact table quality checks.
    """

    print("\nFACT TABLE QUALITY CHECKS")
    print("-" * 50)

    # 1. Check surrogate keys
    check_null_surrogate_keys(
        fact_df
    )

    # 2. Check episode_id uniqueness
    check_duplicate_episode_ids(
        fact_df
    )

    # 3. Check row count
    check_row_count(
        silver_df=silver_df,
        fact_df=fact_df
    )

    # 4. Referential integrity checks
    check_foreign_key_relationship(
        fact_df=fact_df,
        dimension_df=patient_df,
        fact_key="patient_key",
        dimension_key="patient_key"
    )

    check_foreign_key_relationship(
        fact_df=fact_df,
        dimension_df=hospital_df,
        fact_key="hospital_key",
        dimension_key="hospital_key"
    )

    check_foreign_key_relationship(
        fact_df=fact_df,
        dimension_df=department_df,
        fact_key="department_key",
        dimension_key="department_key"
    )

    check_foreign_key_relationship(
        fact_df=fact_df,
        dimension_df=date_df,
        fact_key="date_key",
        dimension_key="date_key"
    )

    print(
        "✓ All fact quality checks passed."
    )

    return {
        "table": "fact_patient_admissions",
        "status": "PASS"
    }