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

    Grain:
        One row per episode_id.

    Supports:
        - Inserts
        - Updates
        - Approved schema evolution
    """

    logger.info(
        "Checking Silver Delta table."
    )

    # ==========================================
    # INITIAL LOAD
    # ==========================================

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

    # ==========================================
    # INCREMENTAL LOAD
    # ==========================================

    logger.info(
        "Merging episode records into Silver Delta table."
    )

    # Enable Delta schema evolution for MERGE
    spark.conf.set(
        "spark.databricks.delta.schema.autoMerge.enabled",
        "true",
    )

    delta_table = DeltaTable.forPath(
        spark,
        silver_path,
    )

    # Dynamically map incoming columns.
    # Day 1/2: existing columns only
    # Day 3: admission_source is included automatically

    update_columns = {
        column: f"source.{column}"
        for column in incoming_df.columns
    }

    (
        delta_table.alias("target")
        .merge(
            incoming_df.alias("source"),
            "target.episode_id = source.episode_id",
        )
        .whenMatchedUpdate(
            set=update_columns,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(
        "Silver Delta merge completed successfully."
    )