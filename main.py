from src.utils.logger import get_logger
from src.extract.extract_patients import extract_patient_data
from src.validate.validation import validate_required_columns
from src.validate.quality_rules import check_missing_patient_id


logger = get_logger(__name__)

def main():
    logger.info("Healthcare pipeline started.")
    patients = extract_patient_data()
    validate_required_columns(patients)
    valid_df, invalid_df = check_missing_patient_id(patients)
    print("Valid:", valid_df.count())
    print("Invalid:", invalid_df.count())
    logger.info("Extraction successful.")

if __name__ == "__main__":
    main()
