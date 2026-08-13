from delta.tables import DeltaTable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col,
    initcap,
    lit,
    max as spark_max,
    row_number,
    trim,
)

from src.config.config import load_config
from src.utils.logger import get_logger


logger = get_logger(__name__)
config = load_config()


silver_path = (
    f"{config['storage']['silver']}/"
    f"{config['datasets']['patient_admissions']}"
)

gold_path = (
    f"{config['storage']['gold']}/"
    f"{config['datasets']['dim_hospital']}"
)


def read_silver(
    spark: SparkSession
) -> DataFrame:
    """
    Reads the Silver patient admissions table.
    """

    logger.info(
        "Reading Silver patient admissions."
    )

    return (
        spark.read
        .format("delta")
        .load(silver_path)
    )


def extract_hospitals(
    silver_df: DataFrame
) -> DataFrame:
    """
    Extracts distinct hospitals from
    the Silver layer.
    """

    logger.info(
        "Extracting unique hospitals."
    )

    return (
        silver_df
        .select(
            initcap(
                trim(col("hospital"))
            ).alias("hospital")
        )
        .filter(
            col("hospital").isNotNull()
        )
        .distinct()
    )


def read_dim_hospital(
    spark: SparkSession
) -> DataFrame | None:
    """
    Reads the Hospital Dimension.

    Returns:
        Existing dimension or None.
    """

    if DeltaTable.isDeltaTable(
        spark,
        gold_path
    ):

        logger.info(
            "Existing Hospital Dimension found."
        )

        return (
            spark.read
            .format("delta")
            .load(gold_path)
        )

    logger.info(
        "Hospital Dimension does not exist."
    )

    return None


def generate_hospital_keys(
    hospital_df: DataFrame,
    existing_dim: DataFrame | None
) -> DataFrame | None:
    """
    Generates surrogate keys for new hospitals.
    """

    window = Window.orderBy(
        "hospital"
    )

    # ------------------------------------------
    # Initial Load
    # ------------------------------------------

    if existing_dim is None:

        logger.info(
            "Initial Hospital Dimension load."
        )

        return (
            hospital_df
            .withColumn(
                "hospital_key",
                row_number().over(window)
            )
            .select(
                "hospital_key",
                "hospital"
            )
        )

    # ------------------------------------------
    # Incremental Load
    # ------------------------------------------

    logger.info(
        "Incremental Hospital Dimension load."
    )

    new_hospitals = (
        hospital_df.alias("silver")
        .join(
            existing_dim.select(
                "hospital"
            ).alias("gold"),
            col("silver.hospital")
            == col("gold.hospital"),
            "left_anti"
        )
    )

    # ------------------------------------------
    # Get Current Maximum Key
    # ------------------------------------------

    max_key = (
        existing_dim
        .agg(
            spark_max("hospital_key")
            .alias("max_key")
        )
        .first()["max_key"]
    ) or 0

    # ------------------------------------------
    # Assign Keys to New Hospitals
    #
    # If no new hospitals exist, the returned
    # DataFrame is simply empty.
    # ------------------------------------------

    return (
        new_hospitals
        .withColumn(
            "hospital_key",
            row_number().over(window)
            + lit(max_key)
        )
        .select(
            "hospital_key",
            "hospital"
        )
    )


def write_dim_hospital(
    spark: SparkSession,
    hospital_df: DataFrame | None
) -> None:
    """
    Writes the Hospital Dimension.
    """

    if hospital_df is None:

        logger.info(
            "No Hospital Dimension data to write."
        )

        return

    if not DeltaTable.isDeltaTable(
        spark,
        gold_path
    ):

        logger.info(
            "Creating Hospital Dimension."
        )

        (
            hospital_df.write
            .format("delta")
            .mode("overwrite")
            .save(gold_path)
        )

        logger.info(
            "Hospital Dimension created successfully."
        )

        return

    logger.info(
        "Updating Hospital Dimension."
    )

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
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(
        "Hospital Dimension updated successfully."
    )


def build_dim_hospital(
    spark: SparkSession
) -> None:
    """
    Builds the Hospital Dimension.
    """

    logger.info(
        "Starting Hospital Dimension build."
    )

    silver_df = read_silver(
        spark
    )

    hospital_df = extract_hospitals(
        silver_df
    )

    existing_dim = read_dim_hospital(
        spark
    )

    hospital_df = generate_hospital_keys(
        hospital_df,
        existing_dim
    )

    write_dim_hospital(
        spark,
        hospital_df
    )

    logger.info(
        "Hospital Dimension build completed successfully."
    )