from delta.tables import DeltaTable

from pyspark.sql import DataFrame, SparkSession

from src.utils.logger import get_logger


logger = get_logger(__name__)


def merge_patient_records(
    spark: SparkSession,
    incoming_df: DataFrame,
    silver_path: str,
) -> None:
    """
    Performs an incremental MERGE into the Silver Delta table.

    If the table does not exist, it is created.

    Existing patient records are updated.

    New patient records are inserted.

    Args:
        spark:
            Active Spark session.

        incoming_df:
            Incoming patient admission records.

        silver_path:
            Path to the Silver Delta table.
    """

    logger.info(
        "Checking Silver Delta table."
    )

    if not DeltaTable.isDeltaTable(
        spark,
        silver_path,
    ):

        logger.info(
            "Creating Silver Delta table."
        )

        (
            incoming_df.write
            .format("delta")
            .mode("overwrite")
            .save(silver_path)
        )

        logger.info(
            "Silver Delta table created successfully."
        )

        return

    logger.info(
        "Merging patient records into Silver Delta table."
    )

    delta_table = DeltaTable.forPath(
        spark,
        silver_path,
    )

    (
        delta_table.alias("target")
        .merge(
            incoming_df.alias("source"),
            "target.patient_id = source.patient_id",
        )
        .whenMatchedUpdate(
            set={
                "patient_name": "source.patient_name",
                "age": "source.age",
                "gender": "source.gender",
                "hospital": "source.hospital",
                "department": "source.department",
                "ward": "source.ward",
                "consultant": "source.consultant",
                "admission_date": "source.admission_date",
                "admission_type": "source.admission_type",
                "updated_at": "source.updated_at",
                "pipeline_name": "source.pipeline_name",
                "batch_id": "source.batch_id",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(
        "Silver Delta merge completed successfully."
    )