from src.utils.spark import get_spark
from src.extract.extract_postgres import (
    extract_patient_admissions,
)

from src.config.config import load_config

config = load_config()

print("\nPOSTGRES CONFIG BEING USED:")
print(config["postgres"])

spark = get_spark(
    "PostgreSQL Connection Test"
)


try:

    patient_df = extract_patient_admissions(
        spark
    )

    print("\nPostgreSQL connection successful.\n")

    patient_df.show(
        truncate=False
    )

    patient_df.printSchema()


finally:

    spark.stop()

    print(
        "\nSpark session stopped."
    )