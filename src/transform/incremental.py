from pyspark.sql.functions import col


def filter_new_records(df, watermark):

    if watermark is None:

        return df

    return df.filter(
        col("admission_date") >
        watermark["last_processed_date"]
    )