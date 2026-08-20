from src.utils.spark import get_spark
from src.config.config import load_config
import os

spark = get_spark("Silver Verification")

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

print("Total Silver records:", silver_df.count())

silver_df.select(
    "patient_id",
    "patient_name",
    "hospital",
    "department",
    "admission_date",
    "admission_type",
    "batch_id"
).orderBy(
    "patient_id"
).show(
    100,
    truncate=False
)


duplicate_df = (
    silver_df
    .groupBy("patient_id")
    .count()
    .filter("count > 1")
)

print(
    "Duplicate patient IDs:",
    duplicate_df.count()
)

duplicate_df.show(
    truncate=False
)
spark.stop()