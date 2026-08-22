from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

from src.transform.delta_merge import merge_patient_records


def test_silver_incremental_merge(
    spark,
    patient_dataframe,
    tmp_path,
):
    """
    Tests the incremental Silver Delta MERGE.

    Verifies that:

    1. An existing episode is updated.
    2. A new episode is inserted.
    3. Existing episodes are not duplicated.
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
            current_timestamp(),
        )
        .withColumn(
            "updated_at",
            current_timestamp(),
        )
        .withColumn(
            "pipeline_name",
            lit("healthcare_patient_pipeline"),
        )
        .withColumn(
            "batch_id",
            lit("batch_001"),
        )
    )

    # --------------------------------------------------
    # Create Initial Silver Table
    # --------------------------------------------------

    merge_patient_records(
        spark,
        first_batch,
        silver_path,
    )

    # --------------------------------------------------
    # Confirm Initial Records
    # --------------------------------------------------

    initial_df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    assert initial_df.count() == 2

    # Verify initial episodes.

    assert (
        initial_df
        .select("episode_id")
        .distinct()
        .count()
        == 2
    )

    # --------------------------------------------------
    # SECOND BATCH
    #
    # E001 = existing episode → UPDATE
    # E003 = new episode → INSERT
    # --------------------------------------------------

    updated_episode = (
        first_batch
        .filter(
            "episode_id = 'E001'"
        )
        .withColumn(
            "patient_name",
            lit("John Smith Updated"),
        )
        .withColumn(
            "date_of_birth",
            lit("1980-05-15").cast("date"),
        )
        .withColumn(
            "hospital",
            lit("Updated Hospital"),
        )
        .withColumn(
            "department",
            lit("Neurology"),
        )
        .withColumn(
            "updated_at",
            current_timestamp(),
        )
        .withColumn(
            "pipeline_name",
            lit("healthcare_patient_pipeline"),
        )
        .withColumn(
            "batch_id",
            lit("batch_002"),
        )
    )

    new_episode = (
        first_batch
        .filter(
            "episode_id = 'E002'"
        )
        .withColumn(
            "episode_id",
            lit("E003"),
        )
        .withColumn(
            "patient_id",
            lit("P003"),
        )
        .withColumn(
            "patient_name",
            lit("David Brown"),
        )
        .withColumn(
            "date_of_birth",
            lit("1974-03-15").cast("date"),
        )
        .withColumn(
            "gender",
            lit("Male"),
        )
        .withColumn(
            "hospital",
            lit("New Hospital"),
        )
        .withColumn(
            "department",
            lit("Neurology"),
        )
        .withColumn(
            "ward",
            lit("Ward C"),
        )
        .withColumn(
            "consultant",
            lit("Dr Green"),
        )
        .withColumn(
            "updated_at",
            current_timestamp(),
        )
        .withColumn(
            "pipeline_name",
            lit("healthcare_patient_pipeline"),
        )
        .withColumn(
            "batch_id",
            lit("batch_002"),
        )
    )

    # --------------------------------------------------
    # Create Second Batch
    # --------------------------------------------------

    second_batch = (
        updated_episode
        .select(*first_batch.columns)
        .unionByName(
            new_episode.select(
                *first_batch.columns
            )
        )
    )

    # --------------------------------------------------
    # Perform Incremental MERGE
    # --------------------------------------------------

    merge_patient_records(
        spark,
        second_batch,
        silver_path,
    )

    # --------------------------------------------------
    # Read Silver After MERGE
    # --------------------------------------------------

    result_df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    # --------------------------------------------------
    # Verify Total Records
    # --------------------------------------------------

    # E001 + E002 + E003

    assert result_df.count() == 3

    # --------------------------------------------------
    # Verify Existing Episode E001 Was Updated
    # --------------------------------------------------

    e001 = (
        result_df
        .filter(
            "episode_id = 'E001'"
        )
    )

    assert e001.count() == 1

    assert (
        e001
        .filter(
            "patient_name = 'John Smith Updated'"
        )
        .count()
        == 1
    )

    assert (
        e001
        .filter(
            "date_of_birth = DATE '1980-05-15'"
        )
        .count()
        == 1
    )

    assert (
        e001
        .filter(
            "hospital = 'Updated Hospital'"
        )
        .count()
        == 1
    )

    # --------------------------------------------------
    # Verify Department Was Updated
    # --------------------------------------------------

    assert (
        e001
        .filter(
            "department = 'Neurology'"
        )
        .count()
        == 1
    )

    # --------------------------------------------------
    # Verify New Episode E003 Was Inserted
    # --------------------------------------------------

    e003 = (
        result_df
        .filter(
            "episode_id = 'E003'"
        )
    )

    assert e003.count() == 1

    assert (
        e003
        .filter(
            "patient_id = 'P003'"
        )
        .count()
        == 1
    )

    assert (
        e003
        .filter(
            "patient_name = 'David Brown'"
        )
        .count()
        == 1
    )

    assert (
        e003
        .filter(
            "date_of_birth = DATE '1974-03-15'"
        )
        .count()
        == 1
    )

    assert (
        e003
        .filter(
            "hospital = 'New Hospital'"
        )
        .count()
        == 1
    )

    # --------------------------------------------------
    # Verify No Duplicate Episodes
    # --------------------------------------------------

    assert (
        result_df
        .groupBy("episode_id")
        .count()
        .filter("count > 1")
        .count()
        == 0
    )

    # Verify each original/new episode exists exactly once.

    assert (
        result_df
        .filter("episode_id = 'E001'")
        .count()
        == 1
    )

    assert (
        result_df
        .filter("episode_id = 'E002'")
        .count()
        == 1
    )

    assert (
        result_df
        .filter("episode_id = 'E003'")
        .count()
        == 1
    )

    # --------------------------------------------------
    # Verify Delta Table Still Exists
    # --------------------------------------------------

    assert DeltaTable.isDeltaTable(
        spark,
        silver_path,
    )