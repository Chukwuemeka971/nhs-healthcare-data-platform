from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import initcap, trim
from pyspark.sql.functions import col, max, row_number, lit
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from src.utils.logger import get_logger
from src.config.config import load_config

logger = get_logger(__name__)


def read_silver(spark):
    """
    Read the Silver admissions data.
    """
    config = load_config()

    silver_path = (
        config["storage"]["silver"]
        + "/"
        + config["datasets"]["patient_admissions"]
    )

    logger.info("Reading Silver data from %s", silver_path)

    silver_df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    return silver_df

    
def extract_hospitals(silver_df):
    """
    Extract distinct hospitals from the Silver layer.
    """
    logger.info("Extracting unique hospitals.")

    hospital_df = (
        silver_df
        .select(
            initcap(trim("hospital")).alias("hospital")
        )
        .filter("hospital IS NOT NULL")
        .distinct()
    )

    return hospital_df



def read_dim_hospital(spark):
    """
    Read the existing Hospital Dimension if it exists.
    """
    config = load_config()

    gold_path = (
        config["storage"]["gold"]
        + "/dim_hospital"
    )

    if DeltaTable.isDeltaTable(spark, gold_path):

        logger.info("Existing Hospital Dimension found.")

        return (
            spark.read
            .format("delta")
            .load(gold_path)
        )

    logger.info("No existing Hospital Dimension found. Initial load.")

    return None



def generate_hospital_keys(hospital_df, existing_dim):
    """
    Generate surrogate keys for new hospitals.

    Parameters:
        hospital_df (DataFrame): Distinct hospitals extracted from Silver.
        existing_dim (DataFrame | None): Existing Hospital Dimension.

    Returns:
        DataFrame: New hospitals with surrogate keys.
    """

    # First pipeline run
    if existing_dim is None:

        logger.info("Initial load detected. Generating hospital keys from 1.")

        window = Window.orderBy("hospital")

        hospital_df = (
            hospital_df
            .withColumn(
                "hospital_key",
                row_number().over(window)
            )
            .select("hospital_key", "hospital")
        )

        return hospital_df

    logger.info("Incremental load detected.")

    # Get current maximum surrogate key
    max_key = (
        existing_dim
        .select(max("hospital_key"))
        .collect()[0][0]
    )

    # Find hospitals that do not already exist
    new_hospitals = (
        hospital_df.alias("silver")
        .join(
            existing_dim.alias("gold"),
            col("silver.hospital") == col("gold.hospital"),
            "left_anti"
        )
    )

    # No new hospitals
    if new_hospitals.isEmpty():

        logger.info("No new hospitals found.")

        return None

    logger.info("Assigning surrogate keys to new hospitals.")

    window = Window.orderBy("hospital")

    new_hospitals = (
        new_hospitals
        .withColumn(
            "hospital_key",
            row_number().over(window) + lit(max_key)
        )
        .select("hospital_key", "hospital")
    )

    return new_hospitals



def write_dim_hospital(spark, hospital_df):
    """
    Write the Hospital Dimension to the Gold layer.

    Performs an initial load if the table does not exist,
    otherwise performs an incremental Delta MERGE.
    """

    if hospital_df is None:
        logger.info("No new hospitals to load.")
        return

    config = load_config()

    gold_path = (
        config["storage"]["gold"]
        + "/dim_hospital"
    )

    # Initial Load
    if not DeltaTable.isDeltaTable(spark, gold_path):

        logger.info("Creating Hospital Dimension.")

        (
            hospital_df.write
            .format("delta")
            .mode("overwrite")
            .save(gold_path)
        )

        logger.info("Hospital Dimension created successfully.")

        return

    # Incremental Load
    logger.info("Updating Hospital Dimension.")

    delta_table = DeltaTable.forPath(
        spark,
        gold_path
    )

    (
        delta_table.alias("target")
        .merge(
            hospital_df.alias("source"),
            "target.hospital = source.hospital"
        )
        .whenMatchedUpdate(
            set={
                "hospital": "source.hospital"
            }
        )
        .whenNotMatchedInsert(
            values={
                "hospital_key": "source.hospital_key",
                "hospital": "source.hospital"
            }
        )
        .execute()
    )

    logger.info("Hospital Dimension updated successfully.")



def build_dim_hospital(spark):

    silver_df = read_silver(spark)

    hospital_df = extract_hospitals(silver_df)

    existing_dim = read_dim_hospital(spark)

    hospital_df = generate_hospital_keys(
        hospital_df,
        existing_dim
    )

    write_dim_hospital(
        spark,
        hospital_df
    )

    


