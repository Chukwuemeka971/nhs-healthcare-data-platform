import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    lit
)

from src.schema.compare import compare_schema
from src.schema.decision import evaluate_schema_changes
from src.schema.reader import get_existing_schema
from src.schema.registry import register_schema_change
from src.schema.report import log_schema_changes

from src.utils.config import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)
config = load_config()


def write_bronze(
    df: DataFrame,
    filename: str,
    spark: SparkSession
) -> None:
    """
    Writes data to the Bronze layer.

    Before writing, the function performs schema governance by:

    - Reading the existing schema.
    - Comparing schemas.
    - Logging schema changes.
    - Evaluating whether the schema is acceptable.
    - Registering approved schema changes.

    The function enriches the Bronze data with ingestion metadata
    before appending it to the Bronze Delta table.

    Args:
        df:
            Spark DataFrame to write.

        filename:
            Source file name.

        spark:
            Active Spark session.

    Raises:
        Exception:
            If schema validation fails.
    """

    dataset_name = config["datasets"]["patient_admissions"]

    bronze_path = os.path.join(
        config["storage"]["bronze"],
        dataset_name
    )

    logger.info(
        f"Writing Bronze data to {bronze_path}"
    )

    # ----------------------------------------------------
    # Schema Governance
    # ----------------------------------------------------

    existing_schema = get_existing_schema(
        spark
    )

    if existing_schema is not None:

        changes = compare_schema(
            existing_schema,
            df.schema
        )

        log_schema_changes(
            changes
        )

        decision = evaluate_schema_changes(
            changes
        )

        if not decision["approved"]:

            logger.error(
                f"Schema rejected: {decision['reason']}"
            )

            raise Exception(
                f"Schema rejected: {decision['reason']}"
            )

        if (
            changes["new_columns"]
            or changes["removed_columns"]
            or changes["type_changes"]
        ):

            register_schema_change(
                changes
            )

    # ----------------------------------------------------
    # Bronze Metadata
    # ----------------------------------------------------

    bronze_df = (
        df
        .withColumn(
            "source_file",
            lit(filename)
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "pipeline_name",
            lit("healthcare_patient_pipeline")
        )
    )

    # ----------------------------------------------------
    # Write Bronze
    # ----------------------------------------------------

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(bronze_path)
    )

    logger.info(
        "Bronze layer written successfully."
    )