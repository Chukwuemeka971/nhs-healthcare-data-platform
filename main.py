from src.utils.logger import get_logger
from src.extract.extract_patients import extract_patient_data
from src.validate.validation import validate_required_columns
from src.validate.validation_runner import run_quality_checks
from src.load.load_quarantine import write_quarantine


logger = get_logger(__name__)

def main():
    logger.info("Healthcare pipeline started.")
    patients = extract_patient_data()
    validate_required_columns(patients)
    valid_df, invalid_records = run_quality_checks(patients)
    print("Valid records:", valid_df.count())

    rule_names = [
    "missing_patient_id",
    "missing_hospital",
    "future_admission_date",
    "invalid_admission_type",
    "duplicate_patient_id"]

    for rule_name, invalid_df in zip(rule_names, invalid_records):

        invalid_count = invalid_df.count()

        if invalid_count > 0:

            logger.info(f"{invalid_count} records failed {rule_name}")

        write_quarantine(invalid_df,rule_name)

if __name__ == "__main__":
    main()
