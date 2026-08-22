import os

from delta.tables import DeltaTable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
)

from src.config.config import load_config
from src.schema.compare import compare_schema
from src.schema.decision import evaluate_schema_changes
from src.schema.reader import get_existing_schema
from src.schema.registry import register_schema_change
from src.schema.report import log_schema_changes
from src.utils.logger import get_logger


logger = get_logger(__name__)


# ==========================================================
# BRONZE IDEMPOTENCY
# ==========================================================

def bronze_file_exists(
    spark: SparkSession,
    bronze_path: str,
    filename: str,
) -> bool:
    """
    Checks whether a source file has already been written
    to the Bronze Delta table.

    This protects Bronze from duplicate writes when a
    pipeline fails after Bronze has completed and the
    pipeline is later retried.

    Returns:
        True:
            The source file already exists in Bronze.

        False:
            The source file does not exist in Bronze.
    """

    logger.info(
        "Checking whether file already exists in Bronze: %s",
        filename,
    )

    # Bronze table does not exist yet
    if not DeltaTable.isDeltaTable(
        spark,
        bronze_path,
    ):

        logger.info(
            "Bronze Delta table does not exist yet."
        )

        return False

    # Read Bronze and check for the source file
    file_exists = (
        spark.read
        .format("delta")
        .load(bronze_path)
        .filter(
            col("source_file") == filename
        )
        .limit(1)
        .count()
        > 0
    )

    if file_exists:

        logger.info(
            "File already exists in Bronze: %s",
            filename,
        )

    else:

        logger.info(
            "File does not exist in Bronze: %s",
            filename,
        )

    return file_exists


# ==========================================================
# WRITE BRONZE
# ==========================================================

def write_bronze(
    df: DataFrame,
    filename: str,
    spark: SparkSession,
) -> None:
    """
    Writes data to the Bronze layer.

    Before writing, the function performs:

    - Bronze file-level idempotency checks.
    - Schema governance.
    - Schema comparison.
    - Schema change logging.
    - Schema change evaluation.
    - Schema change registration.
    - Bronze metadata enrichment.
    - Delta append with schema evolution.

    If the source file already exists in Bronze, the
    Bronze write is skipped.

    This allows the pipeline to safely retry after a
    downstream failure without duplicating Bronze data.

    Args:
        df:
            Incoming Spark DataFrame.

        filename:
            Source file name.

        spark:
            Active Spark session.

    Raises:
        Exception:
            If schema validation fails.
    """

    # ----------------------------------------------------
    # Configuration
    # ----------------------------------------------------

    config = load_config()

    dataset_name = (
        config["datasets"]["patient_admissions"]
    )

    bronze_path = os.path.join(
        config["storage"]["bronze"],
        dataset_name,
    )

    logger.info(
        "Preparing Bronze write to %s",
        bronze_path,
    )

    # ----------------------------------------------------
    # Bronze Idempotency Check
    # ----------------------------------------------------

    if bronze_file_exists(
        spark,
        bronze_path,
        filename,
    ):

        logger.warning(
            "Bronze data already exists for file: %s. "
            "Skipping Bronze write.",
            filename,
        )

        return

    # ----------------------------------------------------
    # Schema Governance
    # ----------------------------------------------------

    logger.info(
        "Starting schema governance."
    )

    existing_schema = get_existing_schema(
        spark
    )

    if existing_schema is not None:

        changes = compare_schema(
            existing_schema,
            df.schema,
        )

        log_schema_changes(
            changes
        )

        decision = evaluate_schema_changes(
            changes
        )

        if not decision["approved"]:

            reason = decision["reason"]

            logger.error(
                "Schema rejected: %s",
                reason,
            )

            raise Exception(
                f"Schema rejected: {reason}"
            )

        has_schema_changes = any(
            [
                changes["new_columns"],
                changes["removed_columns"],
                changes["type_changes"],
            ]
        )

        if has_schema_changes:

            register_schema_change(
                changes
            )

            logger.info(
                "Schema changes registered successfully."
            )

        else:

            logger.info(
                "No schema changes detected."
            )

    else:

        logger.info(
            "No existing Bronze schema found. "
            "Initial schema will be created."
        )

    # ----------------------------------------------------
    # Bronze Metadata
    # ----------------------------------------------------

    logger.info(
        "Adding Bronze ingestion metadata."
    )

    bronze_df = (
        df
        .withColumn(
            "source_file",
            lit(filename),
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp(),
        )
        .withColumn(
            "pipeline_name",
            lit("healthcare_patient_pipeline"),
        )
    )

    # ----------------------------------------------------
    # Write Bronze
    # ----------------------------------------------------

    logger.info(
        "Writing new data to Bronze."
    )

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .option(
            "mergeSchema",
            "true",
        )
        .save(bronze_path)
    )

    logger.info(
        "Bronze layer written successfully "
        "for file: %s",
        filename,
    )