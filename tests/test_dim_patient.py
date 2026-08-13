import os

from src.gold.dim_patient import (
    build_dim_patient,
)


def test_dim_patient_initial_load(
    spark,
    patient_dataframe,
    tmp_path,
    monkeypatch
):
    """
    Tests the initial creation of the
    Patient Dimension.
    """

    # ------------------------------------------
    # Temporary paths
    # ------------------------------------------

    silver_path = os.path.join(
        str(tmp_path),
        "silver",
        "patient_admissions"
    )

    gold_path = os.path.join(
        str(tmp_path),
        "gold",
        "dim_patient"
    )

    # ------------------------------------------
    # Patch module paths
    # ------------------------------------------

    monkeypatch.setattr(
        "src.gold.dim_patient.silver_path",
        silver_path
    )

    monkeypatch.setattr(
        "src.gold.dim_patient.gold_dim_patient_path",
        gold_path
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
    # Assertions
    # ------------------------------------------

    assert result_df.count() == len(
        patient_dataframe
        .select("patient_id")
        .distinct()
        .collect()
    )

    assert (
        result_df
        .filter(
            result_df.patient_key.isNull()
        )
        .count()
        == 0
    )

    assert (
        result_df
        .select("patient_id")
        .distinct()
        .count()
        == result_df.count()
    )

    assert {
        "patient_key",
        "patient_id",
        "patient_name",
        "age",
        "gender",
        "created_at",
        "updated_at",
    }.issubset(
        set(result_df.columns)
    )