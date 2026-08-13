import os
from src.utils.spark import get_spark
from src.gold.fact_patient_admissions import (
    build_fact_patient_admissions
)
from src.utils.config import load_config
config = load_config()
spark = get_spark("Fact Table Test")
from src.quality.fact_quality import (
    check_null_surrogate_keys,
    check_row_count
)

fact_df = build_fact_patient_admissions(spark)
silver_path = os.path.join(
    config["storage"]["silver"],
    config["datasets"]["patient_admissions"]
)

silver_df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

check_null_surrogate_keys(fact_df)

check_row_count(
    silver_df,
    fact_df
)