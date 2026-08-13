from delta.tables import DeltaTable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col,
    current_timestamp,
    dense_rank,
    lit,
    max as spark_max,
)

from src.config.config import load_config
from src.utils.logger import get_logger


logger = get_logger(__name__)
config = load_config()


silver_path = (
    f"{config['storage']['silver']}/"
    f"{config['datasets']['patient_admissions']}"
)

gold_dim_patient_path = (
    f"{config['storage']['gold']}/"
    f"{config['datasets']['dim_patient']}"
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


def extract_patients(
    df: DataFrame
) -> DataFrame:
    """
    Extracts unique patients from the
    Silver patient admissions table.

    Grain:
        One row per patient.
    """

    logger.info(
        "Extracting unique patients."
    )

    return (
        df
        .select(
            "patient_id",
            "patient_name",
            "age",
            "gender"
        )
        .filter(
            col("patient_id").isNotNull()
        )
        .dropDuplicates(
            ["patient_id"]
        )
    )


def read_dim_patient(
    spark: SparkSession
) -> DataFrame | None:
    """
    Reads the existing Patient Dimension.

    Returns:
        Existing dimension DataFrame
        or None if it does not exist.
    """

    if DeltaTable.isDeltaTable(
        spark,
        gold_dim_patient_path
    ):

        logger.info(
            "Existing Patient Dimension found."
        )

        return (
            spark.read
            .format("delta")
            .load(gold_dim_patient_path)
        )

    logger.info(
        "Patient Dimension does not exist."
    )

    return None


def generate_patient_keys(
    new_df: DataFrame,
    existing_df: DataFrame | None
) -> DataFrame:
    """
    Generates surrogate keys for
    new patient records.
    """

    logger.info(
        "Generating patient surrogate keys."
    )

    window = Window.orderBy(
        "patient_id"
    )

    # ------------------------------------------
    # Initial Load
    # ------------------------------------------

    if existing_df is None:

        logger.info(
            "Initial Patient Dimension load."
        )

        return (
            new_df
            .withColumn(
                "patient_key",
                dense_rank().over(window)
            )
            .select(
                "patient_key",
                "patient_id",
                "patient_name",
                "age",
                "gender"
            )
            .withColumn(
                "created_at",
                current_timestamp()
            )
            .withColumn(
                "updated_at",
                current_timestamp()
            )
        )

    # ------------------------------------------
    # Incremental Load
    # ------------------------------------------

    logger.info(
        "Incremental Patient Dimension load."
    )

    new_patients = (
        new_df.alias("new")
        .join(
            existing_df.select(
                "patient_id"
            ).alias("existing"),
            col("new.patient_id")
            == col("existing.patient_id"),
            "left_anti"
        )
    )

    max_key = (
        existing_df
        .agg(
            spark_max("patient_key")
            .alias("max_key")
        )
        .first()["max_key"]
    ) or 0

    return (
        new_patients
        .withColumn(
            "patient_key",
            dense_rank().over(window)
            + lit(max_key)
        )
        .select(
            "patient_key",
            "patient_id",
            "patient_name",
            "age",
            "gender"
        )
        .withColumn(
            "created_at",
            current_timestamp()
        )
        .withColumn(
            "updated_at",
            current_timestamp()
        )
    )


def write_dim_patient(
    spark: SparkSession,
    df: DataFrame
) -> None:
    """
    Writes the Patient Dimension.
    """

    logger.info(
        "Writing Patient Dimension."
    )

    # ------------------------------------------
    # Initial Load
    # ------------------------------------------

    if not DeltaTable.isDeltaTable(
        spark,
        gold_dim_patient_path
    ):

        logger.info(
            "Creating Patient Dimension."
        )

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(gold_dim_patient_path)
        )

        logger.info(
            "Patient Dimension created successfully."
        )

        return

    # ------------------------------------------
    # Incremental Load
    # ------------------------------------------

    logger.info(
        "Updating Patient Dimension."
    )

    delta_table = DeltaTable.forPath(
        spark,
        gold_dim_patient_path
    )

    (
        delta_table.alias("target")
        .merge(
            df.alias("source"),
            "target.patient_id = source.patient_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(
        "Patient Dimension updated successfully."
    )


def build_dim_patient(
    spark: SparkSession
) -> None:
    """
    Builds the Patient Dimension.
    """

    logger.info(
        "Starting Patient Dimension build."
    )

    silver_df = read_silver(
        spark
    )

    patient_df = extract_patients(
        silver_df
    )

    existing_df = read_dim_patient(
        spark
    )

    patient_df = generate_patient_keys(
        patient_df,
        existing_df
    )

    write_dim_patient(
        spark,
        patient_df
    )

    logger.info(
        "Patient Dimension build completed successfully."
    )