import os
from datetime import date

from src.gold.fact_patient_admissions import (
    build_fact_patient_admissions,
)


def test_fact_patient_admissions_initial_load(
    spark,
    patient_records,
    tmp_path,
    monkeypatch,
):
    """
    Tests the initial Fact Patient Admissions load.

    Verifies that:

    - Fact records are created.
    - Dimension surrogate keys are attached.
    - Fact keys are generated.
    - The expected number of rows is written.
    """

    # ======================================================
    # TEMPORARY PATHS
    # ======================================================

    silver_path = os.path.join(
        str(tmp_path),
        "silver",
        "patient_admissions",
    )

    patient_path = os.path.join(
        str(tmp_path),
        "gold",
        "dim_patient",
    )

    hospital_path = os.path.join(
        str(tmp_path),
        "gold",
        "dim_hospital",
    )

    department_path = os.path.join(
        str(tmp_path),
        "gold",
        "dim_department",
    )

    date_path = os.path.join(
        str(tmp_path),
        "gold",
        "dim_date",
    )

    fact_path = os.path.join(
        str(tmp_path),
        "gold",
        "fact_patient_admissions",
    )

    # ======================================================
    # PATCH MODULE PATHS
    # ======================================================

    monkeypatch.setattr(
        "src.gold.fact_patient_admissions.silver_path",
        silver_path,
    )

    monkeypatch.setattr(
        "src.gold.fact_patient_admissions.patient_path",
        patient_path,
    )

    monkeypatch.setattr(
        "src.gold.fact_patient_admissions.hospital_path",
        hospital_path,
    )

    monkeypatch.setattr(
        "src.gold.fact_patient_admissions.department_path",
        department_path,
    )

    monkeypatch.setattr(
        "src.gold.fact_patient_admissions.date_path",
        date_path,
    )

    monkeypatch.setattr(
        "src.gold.fact_patient_admissions.gold_path",
        fact_path,
    )

    # ======================================================
    # CREATE SILVER DATA
    # ======================================================

    silver_df = spark.createDataFrame(
        patient_records
    )

    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .save(silver_path)
    )

    # ======================================================
    # CREATE DIM_PATIENT
    # ======================================================

    dim_patient_df = spark.createDataFrame(
        [
            (
                1,
                "P001",
                "John Smith",
                45,
                "Male",
            ),
            (
                2,
                "P002",
                "Mary Jones",
                37,
                "Female",
            ),
        ],
        [
            "patient_key",
            "patient_id",
            "patient_name",
            "age",
            "gender",
        ],
    )

    (
        dim_patient_df.write
        .format("delta")
        .mode("overwrite")
        .save(patient_path)
    )

    # ======================================================
    # CREATE DIM_HOSPITAL
    # ======================================================

    dim_hospital_df = spark.createDataFrame(
        [
            (
                1,
                "St Mary's Hospital",
            ),
            (
                2,
                "General Hospital",
            ),
        ],
        [
            "hospital_key",
            "hospital",
        ],
    )

    (
        dim_hospital_df.write
        .format("delta")
        .mode("overwrite")
        .save(hospital_path)
    )

    # ======================================================
    # CREATE DIM_DEPARTMENT
    # ======================================================

    dim_department_df = spark.createDataFrame(
        [
            (
                1,
                1,
                "Cardiology",
            ),
            (
                2,
                2,
                "Orthopaedics",
            ),
        ],
        [
            "department_key",
            "hospital_key",
            "department",
        ],
    )

    (
        dim_department_df.write
        .format("delta")
        .mode("overwrite")
        .save(department_path)
    )

    # ======================================================
    # CREATE DIM_DATE
    # ======================================================

    dim_date_df = spark.createDataFrame(
        [
            (
                20260110,
                date(2026, 1, 10),
            ),
            (
                20260205,
                date(2026, 2, 5),
            ),
        ],
        [
            "date_key",
            "full_date",
        ],
    )

    (
        dim_date_df.write
        .format("delta")
        .mode("overwrite")
        .save(date_path)
    )

    # ======================================================
    # BUILD FACT TABLE
    # ======================================================

    result_df = build_fact_patient_admissions(
        spark
    )

    # ======================================================
    # READ PERSISTED FACT TABLE
    # ======================================================

    fact_df = (
        spark.read
        .format("delta")
        .load(fact_path)
    )

    # ======================================================
    # ASSERTIONS
    # ======================================================

    # Two Silver admissions create two fact rows.
    assert result_df.count() == 2

    assert fact_df.count() == 2

    # Fact keys are generated.
    assert (
        fact_df
        .filter("fact_key IS NULL")
        .count()
        == 0
    )

    # All dimension keys are attached.
    for column_name in [
        "patient_key",
        "hospital_key",
        "department_key",
        "date_key",
    ]:
        assert (
            fact_df
            .filter(
                f"{column_name} IS NULL"
            )
            .count()
            == 0
        )

    # Verify P001's expected dimensional relationships.
    p001_fact = (
        fact_df
        .filter("patient_key = 1")
        .first()
    )

    assert p001_fact["hospital_key"] == 1
    assert p001_fact["department_key"] == 1
    assert p001_fact["date_key"] == 20260110