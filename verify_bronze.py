   

from src.utils.spark import get_spark
from src.config.config import load_config
import os

spark = get_spark("Bronze Verification")

config = load_config()

bronze_path = os.path.join(
    config["storage"]["bronze"],
    config["datasets"]["patient_admissions"]
)

bronze_df = (
    spark.read
    .format("delta")
    .load(bronze_path)
)

bronze_df.printSchema()

spark.stop()