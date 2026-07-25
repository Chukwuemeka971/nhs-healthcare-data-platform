import os
from datetime import datetime

from src.config.config import load_config
from src.extract.extract_patients import extract_patient_data
from src.load.load_bronze import write_bronze
from src.load.load_quarantine import write_quarantine
from src.transform.add_audit_columns import add_audit_columns
from src.transform.delta_merge import merge_patient_records
from src.utils.logger import get_logger
from src.utils.spark import get_spark
from src.validate.validation import validate_required_columns
from src.validate.validation_runner import run_quality_checks
from src.watermark.watermark import (
    read_watermark,
    update_watermark
)
from src.load.archive_file import archive_file

logger = get_logger(__name__)
config = load_config()


def main():

    logger.info("Healthcare pipeline started.")

    spark = get_spark("Healthcare Pipeline")

    # Extract patient data and filename
    patients, filename = extract_patient_data()

    # Read watermark
    watermark = read_watermark()

    # Skip if the file has already been processed
    if (
        watermark is not None
        and watermark.get("last_processed_file") == filename
    ):
        logger.info(f"{filename} has already been processed.")
        return

    patient_count = patients.count()

    logger.info(f"Extracted records: {patient_count}")

    # Write raw data to Bronze
    write_bronze(patients)

    # Validate required columns
    validate_required_columns(patients)

    # Run data quality checks
    valid_df, invalid_records = run_quality_checks(patients)

    # Add audit columns
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    valid_df = add_audit_columns(
        valid_df,
        "healthcare_patient_pipeline",
        batch_id
    )

    # Cache because it is used multiple times
    valid_df = valid_df.cache()

    valid_count = valid_df.count()

    logger.info(f"Valid records: {valid_count}")

    # Silver path
    dataset_name = config["datasets"]["patient_admissions"]

    silver_path = os.path.join(
        config["storage"]["silver"],
        dataset_name
    )

    logger.info("Merging records into Silver Delta table.")

    merge_patient_records(
        spark,
        valid_df,
        silver_path
    )

    logger.info("Silver Delta table updated successfully.")

    # Quarantine invalid records
    rule_names = [
        "missing_patient_id",
        "missing_hospital",
        "future_admission_date",
        "invalid_admission_type",
        "duplicate_patient_id"
    ]

    for rule_name, invalid_df in zip(rule_names, invalid_records):

        invalid_count = invalid_df.count()

        if invalid_count > 0:

            logger.info(
                f"{invalid_count} records failed {rule_name}"
            )

            write_quarantine(
                invalid_df,
                rule_name
            )

    # Update watermark after successful processing
    update_watermark(
        filename,
        datetime.now().isoformat()
    )

    logger.info(
        f"Watermark updated. Last processed file: {filename}"
    )

    # Archive source file
    archive_file(filename)

    logger.info(f"{filename} archived successfully.")

    # Release cached DataFrame
    valid_df.unpersist()

    logger.info("Healthcare pipeline completed successfully.")


if __name__ == "__main__":
    main()