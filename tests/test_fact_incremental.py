import os
from datetime import date, datetime

from src.gold.fact_patient_admissions import (
    build_fact_patient_admissions,
)


def test_fact_patient_admissions_incremental_load(
    spark,
    patient_records,
    tmp_path,
    monkeypatch,
):
    """
    Tests incremental loading of Fact Patient Admissions.

    Verifies that:

    1. Initial records are loaded.
    2. Existing fact records are not duplicated.
    3. A new admission is inserted.
    4. Fact keys remain unique.
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
    # FIRST LOAD
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

    # ------------------------------------------------------
    # Patient Dimension
    # ------------------------------------------------------

    dim_patient_df = spark.createDataFrame(
        [
            (
                1,
                "P001",
                "John Smith",
                date(1981,5,15),
                "Male",
            ),
            (
                2,
                "P002",
                "Mary Jones",
                date(1985,5,8),
                "Female",
            ),
        ],
        [
            "patient_key",
            "patient_id",
            "patient_name",
            "date_of_birth",
            "gender",
        ],
    )

    (
        dim_patient_df.write
        .format("delta")
        .mode("overwrite")
        .save(patient_path)
    )

    # ------------------------------------------------------
    # Hospital Dimension
    # ------------------------------------------------------

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

    # ------------------------------------------------------
    # Department Dimension
    # ------------------------------------------------------

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

    # ------------------------------------------------------
    # Date Dimension
    # ------------------------------------------------------

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
    # BUILD INITIAL FACT TABLE
    # ======================================================

    build_fact_patient_admissions(
        spark
    )

    initial_fact_df = (
        spark.read
        .format("delta")
        .load(fact_path)
    )

    # Verify initial load.

    assert initial_fact_df.count() == 2

    assert (
        initial_fact_df
        .select("fact_key")
        .distinct()
        .count()
        == 2
    )

    # ======================================================
    # SECOND LOAD — ADD ONE NEW ADMISSION
    # ======================================================

    new_record = {
        "episode_id": "E003",
        "patient_id": "P003",
        "patient_name": "David Brown",
        "date_of_birth": date(1942,5,12),
        "gender": "Male",
        "hospital": "City Hospital",
        "department": "Neurology",
        "ward": "Ward C",
        "consultant": "Dr Green",
        "admission_date": date(2026, 3, 15),
        "admission_type": "Emergency",
        "created_at": datetime(
            2026,
            3,
            15,
            10,
            0,
            0,
        ),
        "updated_at": datetime(
            2026,
            3,
            15,
            10,
            0,
            0,
        ),
        "pipeline_name": "healthcare_pipeline",
        "batch_id": "batch_002",
    }

    second_load_records = (
        patient_records
        + [new_record]
    )

    second_silver_df = spark.createDataFrame(
        second_load_records
    )

    (
        second_silver_df.write
        .format("delta")
        .mode("overwrite")
        .save(silver_path)
    )

    # ======================================================
    # UPDATE DIMENSIONS
    #
    # This simulates the real pipeline where dimensions
    # are updated before the fact table is built.
    # ======================================================

    # ------------------------------------------------------
    # Patient Dimension
    # ------------------------------------------------------

    updated_patient_df = spark.createDataFrame(
        [
            (
                1,
                "P001",
                "John Smith",
                date(1945,5,15),
                "Male",
            ),
            (
                2,
                "P002",
                "Mary Jones",
                date(1937,5,8),
                "Female",
            ),
            (
                3,
                "P003",
                "David Brown",
                date(1952,7,23),
                "Male",
            ),
        ],
        [
            "patient_key",
            "patient_id",
            "patient_name",
            "date_of_birth",
            "gender",
        ],
    )

    (
        updated_patient_df.write
        .format("delta")
        .mode("overwrite")
        .save(patient_path)
    )

    # ------------------------------------------------------
    # Hospital Dimension
    # ------------------------------------------------------

    updated_hospital_df = spark.createDataFrame(
        [
            (
                1,
                "St Mary's Hospital",
            ),
            (
                2,
                "General Hospital",
            ),
            (
                3,
                "City Hospital",
            ),
        ],
        [
            "hospital_key",
            "hospital",
        ],
    )

    (
        updated_hospital_df.write
        .format("delta")
        .mode("overwrite")
        .save(hospital_path)
    )

    # ------------------------------------------------------
    # Department Dimension
    # ------------------------------------------------------

    updated_department_df = spark.createDataFrame(
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
            (
                3,
                3,
                "Neurology",
            ),
        ],
        [
            "department_key",
            "hospital_key",
            "department",
        ],
    )

    (
        updated_department_df.write
        .format("delta")
        .mode("overwrite")
        .save(department_path)
    )

    # ------------------------------------------------------
    # Date Dimension
    # ------------------------------------------------------

    updated_date_df = spark.createDataFrame(
        [
            (
                20260110,
                date(2026, 1, 10),
            ),
            (
                20260205,
                date(2026, 2, 5),
            ),
            (
                20260315,
                date(2026, 3, 15),
            ),
        ],
        [
            "date_key",
            "full_date",
        ],
    )

    (
        updated_date_df.write
        .format("delta")
        .mode("overwrite")
        .save(date_path)
    )

    # ======================================================
    # SECOND FACT LOAD
    # ======================================================

    build_fact_patient_admissions(
        spark
    )

    # Read the final physical Delta table.
    # We test this rather than the returned DataFrame because
    # Spark DataFrames are lazily evaluated.

    final_fact_df = (
        spark.read
        .format("delta")
        .load(fact_path)
    )

    # ======================================================
    # ASSERTIONS
    # ======================================================

    # Two original records + one new record.

    assert final_fact_df.count() == 3

    # Verify all fact keys are unique.

    assert (
        final_fact_df
        .select("fact_key")
        .distinct()
        .count()
        == 3
    )

    # Verify the new admission was inserted.

    new_fact = (
        final_fact_df
        .filter("patient_key = 3")
        .first()
    )

    assert new_fact is not None

    assert new_fact["hospital_key"] == 3

    assert new_fact["department_key"] == 3

    assert new_fact["date_key"] == 20260315

    # Verify the original records were not duplicated.

    assert (
        final_fact_df
        .filter("patient_key = 1")
        .count()
        == 1
    )

    assert (
        final_fact_df
        .filter("patient_key = 2")
        .count()
        == 1
    )

    # Verify all episode IDs are unique.

    assert (
        final_fact_df
        .filter("episode_id IS NULL")
        .count()
        == 0
    )

    assert (
        final_fact_df
        .groupBy("episode_id")
        .count()
        .filter("count > 1")
        .count()
        == 0
    )

    # Verify all expected episodes exist.

    assert (
        final_fact_df
        .filter("episode_id = 'E001'")
        .count()
        == 1
    )

    assert (
        final_fact_df
        .filter("episode_id = 'E002'")
        .count()
        == 1
    )

    assert (
        final_fact_df
        .filter("episode_id = 'E003'")
        .count()
        == 1
    )