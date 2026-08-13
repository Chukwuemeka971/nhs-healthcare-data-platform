import os

from src.gold.dim_patient import build_dim_patient


def test_dim_patient_incremental_load(
    spark,
    patient_records,
    tmp_path,
    monkeypatch
):
    """
    Tests that new patients are added during
    an incremental Patient Dimension load.
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

    # ==========================================
    # PHASE 1: INITIAL LOAD
    # ==========================================

    initial_df = spark.createDataFrame(
        patient_records
    )

    (
        initial_df.write
        .format("delta")
        .mode("overwrite")
        .save(silver_path)
    )

    build_dim_patient(
        spark
    )

    initial_result = (
        spark.read
        .format("delta")
        .load(gold_path)
    )

    initial_count = initial_result.count()

    p001_key_before = (
        initial_result
        .filter("patient_id = 'P001'")
        .select("patient_key")
        .first()["patient_key"]
    )

    # ==========================================
    # PHASE 2: ADD NEW PATIENT
    # ==========================================

    new_record = {
        "patient_id": "P003",
        "patient_name": "James Wilson",
        "age": 52,
        "gender": "Male",
        "hospital": "City Hospital",
        "department": "Neurology",
        "ward": "Ward C",
        "consultant": "Dr Green",
        "admission_date": patient_records[0]["admission_date"],
        "admission_type": "Emergency"
    }

    updated_records = (
        patient_records
        + [new_record]
    )

    updated_df = spark.createDataFrame(
        updated_records
    )

    (
        updated_df.write
        .format("delta")
        .mode("overwrite")
        .save(silver_path)
    )

    # ==========================================
    # PHASE 3: INCREMENTAL LOAD
    # ==========================================

    build_dim_patient(
        spark
    )

    # ==========================================
    # PHASE 4: VALIDATE RESULTS
    # ==========================================

    final_result = (
        spark.read
        .format("delta")
        .load(gold_path)
    )

    # New patient was added
    assert final_result.count() == initial_count + 1

    # P003 exists
    assert (
        final_result
        .filter("patient_id = 'P003'")
        .count()
        == 1
    )

    # Existing patient retains original key
    p001_key_after = (
        final_result
        .filter("patient_id = 'P001'")
        .select("patient_key")
        .first()["patient_key"]
    )

    assert p001_key_after == p001_key_before

    # New patient gets a surrogate key
    p003_key = (
        final_result
        .filter("patient_id = 'P003'")
        .select("patient_key")
        .first()["patient_key"]
    )

    assert p003_key is not None