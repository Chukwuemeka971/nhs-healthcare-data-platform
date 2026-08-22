import os

from src.gold.dim_patient import (
    build_dim_patient,
)


def test_dim_patient_initial_load(
    spark,
    patient_dataframe,
    tmp_path,
    monkeypatch,
):
    """
    Tests the initial creation of the
    Patient Dimension.

    Verifies that:

    1. The Patient Dimension is created.
    2. One row exists for each unique patient.
    3. Patient surrogate keys are generated.
    4. No duplicate patient IDs exist.
    5. Required columns are present.
    """

    # ------------------------------------------
    # Temporary paths
    # ------------------------------------------

    silver_path = os.path.join(
        str(tmp_path),
        "silver",
        "patient_admissions",
    )

    gold_path = os.path.join(
        str(tmp_path),
        "gold",
        "dim_patient",
    )

    # ------------------------------------------
    # Patch module paths
    # ------------------------------------------

    monkeypatch.setattr(
        "src.gold.dim_patient.silver_path",
        silver_path,
    )

    monkeypatch.setattr(
        "src.gold.dim_patient.gold_dim_patient_path",
        gold_path,
    )

    # ------------------------------------------
    # Create Silver table
    # ------------------------------------------

    (
        patient_dataframe.write
        .format("delta")
        .mode("overwrite")
        .save(silver_path)
    )

    # ------------------------------------------
    # Run dimension build
    # ------------------------------------------

    build_dim_patient(
        spark
    )

    # ------------------------------------------
    # Read result
    # ------------------------------------------

    result_df = (
        spark.read
        .format("delta")
        .load(gold_path)
    )

    # ------------------------------------------
    # Expected unique patients
    # ------------------------------------------

    expected_patient_count = (
        patient_dataframe
        .select("patient_id")
        .distinct()
        .count()
    )

    # ------------------------------------------
    # Assertions
    # ------------------------------------------

    # One dimension record per unique patient.
    assert result_df.count() == expected_patient_count

    # Patient surrogate keys must not be NULL.
    assert (
        result_df
        .filter(
            "patient_key IS NULL"
        )
        .count()
        == 0
    )

    # No duplicate patient IDs.
    assert (
        result_df
        .select("patient_id")
        .distinct()
        .count()
        == result_df.count()
    )

    # Required columns must exist.
    assert {
        "patient_key",
        "patient_id",
        "patient_name",
        "date_of_birth",
        "gender",
        "created_at",
        "updated_at",
    }.issubset(
        set(result_df.columns)
    )