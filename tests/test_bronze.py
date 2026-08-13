import os

from delta.tables import DeltaTable

import src.load.load_bronze as load_bronze


def test_write_bronze(
    spark,
    patient_dataframe,
    tmp_path,
    monkeypatch
):
    """
    Tests that the Bronze writer:

    - Creates a Delta table.
    - Writes all incoming records.
    - Adds source_file metadata.
    - Adds ingestion_timestamp metadata.
    - Adds pipeline_name metadata.
    """

    # --------------------------------------------------
    # Test Bronze configuration
    # --------------------------------------------------

    bronze_path = os.path.join(
        str(tmp_path),
        "patient_admissions"
    )

    test_config = {
        "storage": {
            "bronze": str(tmp_path)
        },
        "datasets": {
            "patient_admissions": "patient_admissions"
        }
    }

    monkeypatch.setattr(
        load_bronze,
        "load_config",
        lambda: test_config
    )

    # --------------------------------------------------
    # Disable schema registry for this isolated test
    # --------------------------------------------------

    monkeypatch.setattr(
        load_bronze,
        "get_existing_schema",
        lambda spark: None
    )

    # --------------------------------------------------
    # Write Bronze
    # --------------------------------------------------

    filename = "test_patient_admissions.xlsx"

    load_bronze.write_bronze(
        patient_dataframe,
        filename,
        spark
    )

    # --------------------------------------------------
    # Verify Delta table exists
    # --------------------------------------------------

    assert DeltaTable.isDeltaTable(
        spark,
        bronze_path
    )

    # --------------------------------------------------
    # Read Bronze table
    # --------------------------------------------------

    bronze_df = (
        spark.read
        .format("delta")
        .load(bronze_path)
    )

    # --------------------------------------------------
    # Verify record count
    # --------------------------------------------------

    assert bronze_df.count() == 2

    # --------------------------------------------------
    # Verify Bronze metadata columns
    # --------------------------------------------------

    expected_columns = [
        "source_file",
        "ingestion_timestamp",
        "pipeline_name"
    ]

    for column_name in expected_columns:

        assert column_name in bronze_df.columns

    # --------------------------------------------------
    # Verify source file metadata
    # --------------------------------------------------

    assert (
        bronze_df
        .filter(
            bronze_df.source_file == filename
        )
        .count()
        == 2
    )

    # --------------------------------------------------
    # Verify pipeline name
    # --------------------------------------------------

    assert (
        bronze_df
        .filter(
            bronze_df.pipeline_name
            == "healthcare_patient_pipeline"
        )
        .count()
        == 2
    )

    # --------------------------------------------------
    # Verify ingestion timestamp
    # --------------------------------------------------

    assert (
        bronze_df
        .filter(
            bronze_df.ingestion_timestamp.isNotNull()
        )
        .count()
        == 2
    )