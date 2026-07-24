from pyspark.sql import functions as F

from src.utils.logger import get_logger
from src.extract.extract_patients import extract_patient_data
from src.validate.validation import validate_required_columns
from src.validate.validation_runner import run_quality_checks
from src.load.load_bronze import write_bronze
from src.load.load_silver import write_silver
from src.load.load_quarantine import write_quarantine
from src.watermark.watermark import read_watermark, update_watermark
from src.transform.incremental import filter_new_records

logger = get_logger(__name__)


def main():

    logger.info("Healthcare pipeline started.")

    # Read the watermark from the previous successful run
    watermark = read_watermark()

    # Extract patient data
    patients = extract_patient_data()
    logger.info(f"Extracted records: {patients.count()}")

    # Store raw data in Bronze
    write_bronze(patients)

    # Filter only new records
    patients = filter_new_records(patients, watermark)
    logger.info(f"Records after incremental filter: {patients.count()}")

    # Stop if there is nothing new to process
    if patients.count() == 0:
        logger.info("No new records found. Pipeline finished.")
        return

    # Validate required columns
    validate_required_columns(patients)

    # Run data quality rules
    valid_df, invalid_records = run_quality_checks(patients)
    logger.info(f"Valid records: {valid_df.count()}")

    # Write valid records to the Silver layer
    write_silver(valid_df)

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
    latest_date = (
        valid_df
        .select(F.max("admission_date"))
        .collect()[0][0]
    )

    if latest_date is not None:

        update_watermark(str(latest_date))

        logger.info(
            f"Watermark updated to {latest_date}"
        )

    else:

        logger.info(
            "No valid records processed. Watermark not updated."
        )


if __name__ == "__main__":
    main()                                                             