from pyspark.sql.functions import col, current_date

#==========================================================================
#Checking for missing patient id
#==========================================================================
def check_missing_patient_id(df):
    invalid_df = df.filter(col("patient_id").isNull())
    valid_df = df.filter(col("patient_id").isNotNull())
    return valid_df, invalid_df


#============================================================================
#Invaid admission type
#============================================================================
VALID_TYPES = ["Emergency","Elective","Transfer","Maternity"]

def check_admission_type(df):
    invalid_df = df.filter(~col("admission_type").isin(VALID_TYPES))
    valid_df = df.filter(col("admission_type").isin(VALID_TYPES))
    return valid_df, invalid_df


#============================================================================
#Future Admission Dates
#============================================================================
def check_future_admission_date(df):
    invalid_df = df.filter(col("admission_date") > current_date())
    valid_df = df.filter(col("admission_date") <= current_date())
    return valid_df, invalid_df


#============================================================================
# Missing hospital
#============================================================================
def check_missing_hospital(df):
    invalid_df = df.filter(col("hospital").isNull())
    valid_df = df.filter(col("hospital").isNotNull())
    return valid_df, invalid_df


#===========================================================================
#Checking for dupiicates
#===========================================================================
def check_duplicates(df):
    duplicate_ids = (
        df.groupBy("patient_id")
        .count()
        .filter(col("count") > 1)
        .select("patient_id")
    )
    
    invalid_df = (
        df.join(duplicate_ids, on = "patient_id", how = "inner")
    )

    valid_df = (
        df.join(duplicate_ids, on = "patient_id", how = "left_anti")
    )

    return valid_df, invalid_df
