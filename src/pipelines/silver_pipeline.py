import os
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from src.config.config import load_config
from src.load.load_quarantine import write_quarantine
from src.transform.add_audit_columns import add_audit_columns
from src.transform.delta_merge import merge_patient_records
from src.utils.logger import get_logger
from src.validate.validation import validate_required_columns
from src.validate.validation_runner import run_quality_checks

from src.schema.compare import compare_schema
from src.schema.decision import evaluate_schema_changes
from src.schema.reader import get_existing_schema
from src.schema.registry import register_schema_change
from src.schema.report import log_schema_changes

logger = get_logger(__name__)


def build_silver_layer(
    spark: SparkSession,
    patients: DataFrame,
) -> DataFrame:
    
    """
    Executes the complete Silver layer.

    Responsibilities:
    - Validate schema
    - Run data quality rules
    - Add audit columns
    - Merge into Silver
    - Write quarantine records
    """

    logger.info(
        "Starting Silver layer."
    )

    config = load_config()

    # -----------------------------------------
    # Schema Evolution
    # -----------------------------------------

    existing_schema = get_existing_schema(
        spark
    )

    if existing_schema is None:

        logger.info(
            "No existing Silver schema found. "
            "Initial load approved."
        )

    else:

        changes = compare_schema(
            existing_schema,
            patients.schema,
        )

        log_schema_changes(
            changes
        )

        decision = evaluate_schema_changes(
            changes
        )

        if not decision["approved"]:

            logger.error(
                "Schema validation failed: %s",
                decision["reason"],
            )

            raise ValueError(
                f"Schema validation failed: "
                f"{decision['reason']}"
            )

        logger.info(
            "Schema approved: %s",
            decision["reason"],
        )

        if changes["new_columns"]:

            register_schema_change(
                changes
            )

    # -----------------------------------------
    # Schema Validation
    # -----------------------------------------

    validate_required_columns(
        patients
    )

    # -----------------------------------------
    # Data Quality
    # -----------------------------------------

    valid_df, invalid_records = (
        run_quality_checks(
            patients
        )
    )

    # -----------------------------------------
    # Audit Columns
    # -----------------------------------------

    batch_id = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    valid_df = add_audit_columns(
        valid_df,
        "healthcare_patient_pipeline",
        batch_id,
    )

    # -----------------------------------------
    # Silver Path
    # -----------------------------------------

    dataset_name = (
        config["datasets"][
            "patient_admissions"
        ]
    )

    silver_path = os.path.join(
        config["storage"]["silver"],
        dataset_name,
    )

    # -----------------------------------------
    # Cache valid records
    # -----------------------------------------

    valid_df.cache()

    try:

        logger.info(
            "Valid records: %d",
            valid_df.count(),
        )

        # -----------------------------------------
        # Silver Merge
        # -----------------------------------------

        merge_patient_records(
            spark,
            valid_df,
            silver_path,
        )

        logger.info(
            "Silver layer updated successfully."
        )

        # -----------------------------------------
        # Quarantine
        # -----------------------------------------

        rule_names = [
            "missing_patient_id",
            "missing_hospital",
            "duplicate_episode_id",
            "future_admission_date",
            "invalid_admission_type",
        ]

        for rule_name, invalid_df in zip(
            rule_names,
            invalid_records,
        ):

            if not invalid_df.rdd.isEmpty():

                invalid_count = invalid_df.count()

                logger.warning(
                    "%d records failed %s",
                    invalid_count,
                    rule_name,
                )

                write_quarantine(
                    invalid_df,
                    rule_name,
                )

        logger.info(
            "Silver layer completed successfully."
        )

        return valid_df

    finally:

        valid_df.unpersist()