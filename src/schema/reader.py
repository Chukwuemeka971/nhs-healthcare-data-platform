import os

from delta.tables import DeltaTable

from src.config.config import load_config

config = load_config()


def get_existing_schema(spark):
    """
    Returns the existing Bronze schema.

    Returns:
        StructType | None
    """

    dataset_name = config["datasets"]["patient_admissions"]

    bronze_path = os.path.join(
        config["storage"]["bronze"],
        dataset_name
    )

    if not DeltaTable.isDeltaTable(spark, bronze_path):
        return None

    df = spark.read.format("delta").load(bronze_path)

    return df.schema