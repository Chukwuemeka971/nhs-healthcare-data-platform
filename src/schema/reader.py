import os

from delta.tables import DeltaTable

from src.config.config import load_config


SILVER_AUDIT_COLUMNS = {
    "created_at",
    "updated_at",
    "pipeline_name",
    "batch_id",
}


def get_existing_schema(spark):
    """
    Returns the existing Silver business schema.

    Silver audit columns are excluded because they are
    added by the pipeline and do not belong to the source schema.

    Returns:
        StructType | None
    """

    config = load_config()

    dataset_name = (
        config["datasets"]["patient_admissions"]
    )

    silver_path = os.path.join(
        config["storage"]["silver"],
        dataset_name,
    )

    if not DeltaTable.isDeltaTable(
        spark,
        silver_path,
    ):
        return None

    df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    business_columns = [
        field
        for field in df.schema.fields
        if field.name not in SILVER_AUDIT_COLUMNS
    ]

    return df.schema.__class__(
        business_columns
    )