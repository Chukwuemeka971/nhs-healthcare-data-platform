from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

from src.transform.delta_merge import merge_patient_records


def test_silver_incremental_merge(
    spark,
    patient_dataframe,
    tmp_path
):
    """
    Tests the incremental Silver Delta MERGE.

    Verifies that:

    1. An existing patient is updated.
    2. A new patient is inserted.
    3. Existing patients are not duplicated.
    4. Department changes are persisted.
    """

    # --------------------------------------------------
    # Temporary Silver path
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
    # Create initial Silver table
    # --------------------------------------------------

    merge_patient_records(
        spark,
        first_batch,
        silver_path
    )

    # --------------------------------------------------
    # Confirm initial records
    # --------------------------------------------------

    initial_df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    assert initial_df.count() == 2

    # --------------------------------------------------
    # SECOND BATCH
    #
    # P001 = existing patient → UPDATE
    # P003 = new patient → INSERT
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
            "hospital",
            lit("New Hospital")
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

    # Make sure both DataFrames have identical columns/order.
    second_batch = (
        updated_patient
        .select(*first_batch.columns)
        .unionByName(
            new_patient.select(*first_batch.columns)
        )
    )

    # --------------------------------------------------
    # Perform incremental MERGE
    # --------------------------------------------------

    merge_patient_records(
        spark,
        second_batch,
        silver_path
    )

    # --------------------------------------------------
    # Read Silver after MERGE
    # --------------------------------------------------

    result_df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    # --------------------------------------------------
    # Verify total records
    # --------------------------------------------------

    assert result_df.count() == 3

    # --------------------------------------------------
    # Verify existing patient P001 was updated
    # --------------------------------------------------

    p001 = (
        result_df
        .filter(
            "patient_id = 'P001'"
        )
    )

    assert p001.count() == 1

    assert (
        p001
        .filter(
            "patient_name = 'John Smith Updated'"
        )
        .count()
        == 1
    )

    assert (
        p001
        .filter(
            "age = 46"
        )
        .count()
        == 1
    )

    assert (
        p001
        .filter(
            "hospital = 'Updated Hospital'"
        )
        .count()
        == 1
    )

    # --------------------------------------------------
    # Verify department was updated
    # --------------------------------------------------

    assert (
        p001
        .filter(
            "department = 'Neurology'"
        )
        .count()
        == 1
    )

    # --------------------------------------------------
    # Verify new patient P003 was inserted
    # --------------------------------------------------

    p003 = (
        result_df
        .filter(
            "patient_id = 'P003'"
        )
    )

    assert p003.count() == 1

    assert (
        p003
        .filter(
            "patient_name = 'David Brown'"
        )
        .count()
        == 1
    )

    assert (
        p003
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
        result_df
        .filter(
            "patient_id = 'P001'"
        )
        .count()
        == 1
    )

    # --------------------------------------------------
    # Verify Delta table still exists
    # --------------------------------------------------

    assert DeltaTable.isDeltaTable(
        spark,
        silver_path
    )