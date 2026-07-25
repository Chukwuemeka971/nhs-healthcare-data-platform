from pyspark.sql.functions import (
    current_timestamp,
    lit
)


def add_audit_columns(df,pipeline_name,batch_id):

    return (
        df
        .withColumn("created_at",current_timestamp())
        .withColumn( "updated_at",current_timestamp())
        .withColumn("pipeline_name",lit(pipeline_name))
        .withColumn("batch_id",lit(batch_id))
    )