import os

from src.utils.logger import get_logger
from src.utils.config import load_config

from src.schema.reader import get_existing_schema
from src.schema.compare import compare_schema
from src.schema.report import log_schema_changes
from src.schema.decision import evaluate_schema_changes

logger = get_logger(__name__)
config = load_config()


def write_bronze(df, spark):

    dataset_name = config["datasets"]["patient_admissions"]

    bronze_path = os.path.join(
        config["storage"]["bronze"],
        dataset_name
    )

    logger.info(f"Writing Bronze data to {bronze_path}")

    existing_schema = get_existing_schema(spark)

    if existing_schema is not None:

        changes = compare_schema(
            existing_schema,
            df.schema
        )

        log_schema_changes(changes)

        decision = evaluate_schema_changes(changes)

        if not decision["approved"]:
            raise Exception(
                f"Schema rejected: {decision['reason']}"
            )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(bronze_path)
    )

    logger.info("Bronze write completed.")