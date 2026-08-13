from src.utils.spark import get_spark
from src.gold.fact_patient_admissions import (
    build_fact_patient_admissions
)

spark = get_spark("Fact Table Test")

fact_df = build_fact_patient_admissions(spark)

fact_df.show(20, truncate=False)

fact_df.printSchema()

spark.stop()