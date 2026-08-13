from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

from src.transform.delta_merge import merge_patient_records


def test_silver_initial_load_and_incremental_merge(
    spark,
    patient_dataframe,
    tmp_path
):
    """
    Tests the Silver Delta MERGE process.

    Verifies:

    1. The Silver table is created on the first load.
    2. Existing patient records are updated.
    3. Department changes are persisted.
    4. New patient records are inserted.
    5. Existing records are not duplicated.
    """

    # --------------------------------------------------
    # Silver test path
    # --------------------------------------------------

    silver_path = str(
        tmp_path / "patient_admissions"
    )

    # --------------------------------------------------
    # FIRST BATCH
    # --------------------------------------------------

    first_batch = (
        patient_dataframe
        .withColumn(
            "created_at",
            current_timestamp()
        )
        .withColumn(
            "updated_at",
            current_timestamp()
        )
        .withColumn(
            "pipeline_name",
            lit("healthcare_patient_pipeline")
        )
        .withColumn(
            "batch_id",
            lit("batch_001")
        )
    )

    # --------------------------------------------------
    # Initial Silver load
    # --------------------------------------------------

    merge_patient_records(
        spark,
        first_batch,
        silver_path
    )

    # --------------------------------------------------
    # Verify Silver Delta table exists
    # --------------------------------------------------

    assert DeltaTable.isDeltaTable(
        spark,
        silver_path
    )

    silver_df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    assert silver_df.count() == 2

    # --------------------------------------------------
    # SECOND BATCH
    #
    # IMPORTANT:
    # Both records are created from first_batch.
    # This guarantees identical schemas.
    # --------------------------------------------------

    # --------------------------------------------------
    # Existing patient: P001
    # --------------------------------------------------

    updated_patient = (
        first_batch
        .filter(
            "patient_id = 'P001'"
        )
        .withColumn(
            "patient_name",
            lit("John Smith Updated")
        )
        .withColumn(
            "age",
            lit(46)
        )
        .withColumn(
            "hospital",
            lit("Updated Hospital")
        )
        .withColumn(
            "department",
            lit("Neurology")
        )
        .withColumn(
            "updated_at",
            current_timestamp()
        )
        .withColumn(
            "pipeline_name",
            lit("healthcare_patient_pipeline")
        )
        .withColumn(
            "batch_id",
            lit("batch_002")
        )
    )

    # --------------------------------------------------
    # New patient: P003
    # --------------------------------------------------

    new_patient = (
        first_batch
        .filter(
            "patient_id = 'P002'"
        )
        .withColumn(
            "patient_id",
            lit("P003")
        )
        .withColumn(
            "patient_name",
            lit("David Brown")
        )
        .withColumn(
            "age",
            lit(52)
        )
        .withColumn(
            "gender",
            lit("Male")
        )
        .withColumn(
            "hospital",
            lit("New Hospital")
        )
        .withColumn(
            "department",
            lit("Neurology")
        )
        .withColumn(
            "ward",
            lit("Ward C")
        )
        .withColumn(
            "consultant",
            lit("Dr Green")
        )
        .withColumn(
            "admission_type",
            lit("Transfer")
        )
        .withColumn(
            "updated_at",
            current_timestamp()
        )
        .withColumn(
            "pipeline_name",
            lit("healthcare_patient_pipeline")
        )
        .withColumn(
            "batch_id",
            lit("batch_002")
        )
    )

    # --------------------------------------------------
    # Make absolutely sure both DataFrames have
    # exactly the same columns and order.
    # --------------------------------------------------

    second_batch = (
        updated_patient
        .select(*first_batch.columns)
        .unionByName(
            new_patient.select(*first_batch.columns)
        )
    )

    # --------------------------------------------------
    # Incremental MERGE
    # --------------------------------------------------

    merge_patient_records(
        spark,
        second_batch,
        silver_path
    )

    # --------------------------------------------------
    # Read updated Silver table
    # --------------------------------------------------

    silver_df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    # --------------------------------------------------
    # Verify total records
    # --------------------------------------------------

    assert silver_df.count() == 3

    # --------------------------------------------------
    # Verify P001 was updated
    # --------------------------------------------------

    updated_patient_df = (
        silver_df
        .filter(
            "patient_id = 'P001'"
        )
    )

    assert updated_patient_df.count() == 1

    # --------------------------------------------------
    # Verify patient name
    # --------------------------------------------------

    assert (
        updated_patient_df
        .filter(
            "patient_name = 'John Smith Updated'"
        )
        .count()
        == 1
    )

    # --------------------------------------------------
    # Verify age
    # --------------------------------------------------

    assert (
        updated_patient_df
        .filter(
            "age = 46"
        )
        .count()
        == 1
    )

    # --------------------------------------------------
    # Verify hospital
    # --------------------------------------------------

    assert (
        updated_patient_df
        .filter(
            "hospital = 'Updated Hospital'"
        )
        .count()
        == 1
    )

    # --------------------------------------------------
    # Verify department
    # --------------------------------------------------

    assert (
        updated_patient_df
        .filter(
            "department = 'Neurology'"
        )
        .count()
        == 1
    )

    # --------------------------------------------------
    # Verify P003 was inserted
    # --------------------------------------------------

    new_patient_df = (
        silver_df
        .filter(
            "patient_id = 'P003'"
        )
    )

    assert new_patient_df.count() == 1

    # --------------------------------------------------
    # Verify P003 values
    # --------------------------------------------------

    assert (
        new_patient_df
        .filter(
            "patient_name = 'David Brown'"
        )
        .count()
        == 1
    )

    assert (
        new_patient_df
        .filter(
            "hospital = 'New Hospital'"
        )
        .count()
        == 1
    )

    # --------------------------------------------------
    # Verify no duplicate P001
    # --------------------------------------------------

    assert (
        silver_df
        .filter(
            "patient_id = 'P001'"
        )
        .count()
        == 1
    )