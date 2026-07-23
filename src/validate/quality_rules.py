from pyspark.sql.functions import col

def check_missing_patient_id(df):
    invalid_df = df.filter(col("patient_id").isNull())
    valid_df = df.filter(col("patient_id").isNotNull())
    return valid_df, invalid_df