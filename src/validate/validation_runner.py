from src.validate.quality_rules import (
    check_missing_patient_id, 
    check_future_admission_date, 
    check_missing_hospital, 
    check_admission_type,
    check_duplicates
)

RULES = [
    check_missing_patient_id,
    check_future_admission_date,
    check_missing_hospital,
    check_admission_type,
    check_duplicates
]
def run_quality_checks(df):
    invalid_records = []
    current_df = df
    for rule in RULES:
        current_df, invalid_df = rule(current_df)
        invalid_records.append(invalid_df)
    return current_df, invalid_records