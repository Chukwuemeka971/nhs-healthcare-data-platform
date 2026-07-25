from delta.tables import DeltaTable


def merge_patient_records(
    spark,
    incoming_df,
    silver_path
):
    """
    Creates the Delta table on the first run.
    Performs MERGE on subsequent runs.
    """

    if not DeltaTable.isDeltaTable(spark, silver_path):

        (
            incoming_df.write
            .format("delta")
            .mode("overwrite")
            .save(silver_path)
        )

        return

    delta_table = DeltaTable.forPath(
        spark,
        silver_path
    )

    (
        delta_table.alias("target")
        .merge(
            incoming_df.alias("source"),
            "target.patient_id = source.patient_id"
        )
        .whenMatchedUpdate(
            set={
            "patient_name": "source.patient_name",
            "age": "source.age",
            "gender": "source.gender",
            "hospital": "source.hospital",
            "ward": "source.ward",
            "consultant": "source.consultant",
            "admission_date": "source.admission_date",
            "admission_type": "source.admission_type",
            "updated_at": "source.updated_at",
            "pipeline_name": "source.pipeline_name",
            "batch_id": "source.batch_id"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )