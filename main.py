from src.utils.logger import get_logger
from src.extract.extract_patients import extract_patient_data
from src.validate.validation import validate_required_columns
from src.validate.validation_runner import run_quality_checks


logger = get_logger(__name__)

def main():
    logger.info("Healthcare pipeline started.")
    patients = extract_patient_data()
    validate_required_columns(patients)
    valid_df, invalid_records = run_quality_checks(patients)
    print("Valid records:", valid_df.count())
    for invalid in invalid_records:
         print("Invalid records:", invalid.count())
    logger.info("Extraction successful.")


if __name__ == "__main__":
    main()
