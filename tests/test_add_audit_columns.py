from src.transform.add_audit_columns import add_audit_columns


def test_add_audit_columns(
    patient_dataframe
):

    pipeline_name = "healthcare_patient_pipeline"
    batch_id = "20260808_054300"

    result_df = add_audit_columns(
        patient_dataframe,
        pipeline_name,
        batch_id
    )

    expected_columns = [
        "created_at",
        "updated_at",
        "pipeline_name",
        "batch_id"
    ]

    for column_name in expected_columns:

        assert column_name in result_df.columns

    assert result_df.count() == 2

    assert (
        result_df
        .filter(
            result_df.pipeline_name == pipeline_name
        )
        .count()
        == 2
    )

    assert (
        result_df
        .filter(
            result_df.batch_id == batch_id
        )
        .count()
        == 2
    )

    assert (
        result_df
        .filter(
            result_df.created_at.isNotNull()
        )
        .count()
        == 2
    )

    assert (
        result_df
        .filter(
            result_df.updated_at.isNotNull()
        )
        .count()
        == 2
    )