from pyspark.sql.functions import col

def validate_required_columns(df):
    REQUIRED_COLUMNS = [
    "patient_id",
    "patient_name",
    "age",
    "gender",
    "hospital",
    "ward",
    "consultant",
    "admission_date",
    "admission_type"
    ]

    missing = [
        column for column in REQUIRED_COLUMNS if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required columns: {missing}"
        )
    return df
   
