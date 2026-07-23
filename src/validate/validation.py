from pyspark.sql.functions import col

def validate_required_columns(df):
    required_columns = [
        "patient_id",
        "hospital",
        "dob",
        "admission_date",
        "admission_type",
    ]

    missing = [
        column for column in required_columns if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required columns: {missing}"
        )
    return df
   
