import os

from delta.tables import DeltaTable

from src.config.config import load_config


BRONZE_METADATA_COLUMNS = {
    "source_file",
    "ingestion_timestamp",
    "pipeline_name",
}


def get_existing_schema(spark):
    """
    Returns the existing Bronze business schema.

    Bronze metadata columns are excluded because they are
    added by the pipeline and do not belong to the source schema.

    Returns:
        StructType | None
    """

    config = load_config()

    dataset_name = config["datasets"]["patient_admissions"]

    bronze_path = os.path.join(
        config["storage"]["bronze"],
        dataset_name,
    )

    if not DeltaTable.isDeltaTable(
        spark,
        bronze_path,
    ):
        return None

    df = (
        spark.read
        .format("delta")
        .load(bronze_path)
    )

    business_columns = [
        field
        for field in df.schema.fields
        if field.name not in BRONZE_METADATA_COLUMNS
    ]

    return df.schema.__class__(
        business_columns
    )