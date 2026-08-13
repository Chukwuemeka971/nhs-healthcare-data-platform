from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

from src.transform.delta_merge import merge_patient_records


def test_silver_initial_load(
    spark,
    patient_dataframe,
    tmp_path
):
    """
    Tests the initial Silver Delta load.

    Verifies that:

    1. The Silver Delta table is created.
    2. All incoming records are written.
    3. The expected Silver columns are present.
    """

    # --------------------------------------------------
    # Temporary Silver path
    # --------------------------------------------------

    silver_path = str(
        tmp_path / "patient_admissions"
    )

    # --------------------------------------------------
    # Prepare incoming Silver data
    # --------------------------------------------------

    silver_df = (
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
    # Execute initial Silver load
    # --------------------------------------------------

    merge_patient_records(
        spark,
        silver_df,
        silver_path
    )

    # --------------------------------------------------
    # Verify Delta table exists
    # --------------------------------------------------

    assert DeltaTable.isDeltaTable(
        spark,
        silver_path
    )

    # --------------------------------------------------
    # Read Silver table
    # --------------------------------------------------

    result_df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    # --------------------------------------------------
    # Verify record count
    # --------------------------------------------------

    assert result_df.count() == 2

    # --------------------------------------------------
    # Verify required columns
    # --------------------------------------------------

    expected_columns = [
        "patient_id",
        "patient_name",
        "age",
        "gender",
        "hospital",
        "department",
        "ward",
        "consultant",
        "admission_date",
        "admission_type",
        "created_at",
        "updated_at",
        "pipeline_name",
        "batch_id"
    ]

    for column_name in expected_columns:

        assert column_name in result_df.columns

    # --------------------------------------------------
    # Verify patient IDs
    # --------------------------------------------------

    patient_ids = {
        row["patient_id"]
        for row in result_df.select(
            "patient_id"
        ).collect()
    }

    assert patient_ids == {
        "P001",
        "P002"
    }