import os

from src.utils.logger import get_logger
from src.config.config import load_config
from pyspark.sql.functions import lit

logger = get_logger(__name__)


def write_quarantine(df, rule_name):

    config = load_config()

    quarantine_path = os.path.join(config["storage"]["quarantine"],rule_name)

    logger.info( f"Writing quarantine records to {quarantine_path}")

    df = df.withColumn("failed_rule",lit(rule_name))

    (
        df.write
        .mode("overwrite")
        .parquet(quarantine_path)
    )

    logger.info("Quarantine write completed.")