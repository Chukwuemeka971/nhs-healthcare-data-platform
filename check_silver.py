from src.utils.spark import get_spark
from src.utils.config import load_config
import os

spark = get_spark("Check Silver")

# Call the function
config = load_config()

silver_path = os.path.join(
    config["storage"]["silver"],
    config["datasets"]["patient_admissions"]
)

silver_df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

'''
silver_df.printSchema()

silver_df.select(
    "patient_id",
    "patient_name",
    "hospital",
    "ward",
    "created_at",
    "updated_at",
    "pipeline_name",
    "batch_id"
).show(5, truncate=False)

silver_df.count()

silver_df.filter(
    silver_df.patient_id == "P100001"
).show(truncate=False)

silver_df.filter(
    silver_df.patient_id == "P100070"
).show(truncate=False)
'''

silver_df.filter(
    silver_df.patient_id == "P100001"
).select(
    "patient_id",
    "created_at",
    "updated_at",
    "batch_id"
).show(truncate=False)