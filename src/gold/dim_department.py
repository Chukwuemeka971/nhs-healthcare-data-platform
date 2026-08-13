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

gold_dim_hospital_path = (
    f"{config['storage']['gold']}/"
    f"{config['datasets']['dim_hospital']}"
)

gold_dim_department_path = (
    f"{config['storage']['gold']}/"
    f"{config['datasets']['dim_department']}"
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


def extract_departments(
    df: DataFrame
) -> DataFrame:
    """
    Extracts unique hospital and department
    combinations from the Silver layer.
    """

    logger.info(
        "Extracting unique departments."
    )

    return (
        df
        .select(
            "hospital",
            "department"
        )
        .filter(
            col("hospital").isNotNull()
            & col("department").isNotNull()
        )
        .distinct()
    )


def read_dim_hospital(
    spark: SparkSession
) -> DataFrame:
    """
    Reads the Hospital Dimension.
    """

    logger.info(
        "Reading Hospital Dimension."
    )

    return (
        spark.read
        .format("delta")
        .load(gold_dim_hospital_path)
    )


def attach_hospital_keys(
    department_df: DataFrame,
    hospital_df: DataFrame
) -> DataFrame:
    """
    Attaches hospital surrogate keys to
    each department.
    """

    logger.info(
        "Attaching hospital surrogate keys."
    )

    return (
        department_df.alias("dept")
        .join(
            hospital_df.select(
                "hospital_key",
                "hospital"
            ).alias("hospital"),
            col("dept.hospital")
            == col("hospital.hospital"),
            "left"
        )
        .select(
            col("hospital.hospital_key").alias(
                "hospital_key"
            ),
            col("dept.hospital").alias(
                "hospital"
            ),
            col("dept.department").alias(
                "department"
            )
        )
        .filter(
            col("hospital_key").isNotNull()
        )
    )


def read_dim_department(
    spark: SparkSession
) -> DataFrame | None:
    """
    Reads the existing Department Dimension.

    Returns:
        Existing dimension or None.
    """

    if DeltaTable.isDeltaTable(
        spark,
        gold_dim_department_path
    ):

        logger.info(
            "Existing Department Dimension found."
        )

        return (
            spark.read
            .format("delta")
            .load(gold_dim_department_path)
        )

    logger.info(
        "Department Dimension does not exist."
    )

    return None


def generate_department_keys(
    new_df: DataFrame,
    existing_df: DataFrame | None
) -> DataFrame:
    """
    Generates surrogate keys for new departments.
    """

    logger.info(
        "Generating department surrogate keys."
    )

    window = Window.orderBy(
        "hospital_key",
        "department"
    )

    # ------------------------------------------
    # Initial Load
    # ------------------------------------------

    if existing_df is None:

        logger.info(
            "Initial Department Dimension load."
        )

        return (
            new_df
            .withColumn(
                "department_key",
                dense_rank().over(window)
            )
            .select(
                "department_key",
                "hospital_key",
                "department"
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
        "Incremental Department Dimension load."
    )

    new_departments = (
        new_df.alias("new")
        .join(
            existing_df.select(
                "hospital_key",
                "department"
            ).alias("existing"),
            (
                (
                    col("new.hospital_key")
                    == col("existing.hospital_key")
                )
                &
                (
                    col("new.department")
                    == col("existing.department")
                )
            ),
            "left_anti"
        )
    )

    # ------------------------------------------
    # Get Current Maximum Key
    # ------------------------------------------

    max_key = (
        existing_df
        .agg(
            spark_max("department_key")
            .alias("max_key")
        )
        .first()["max_key"]
    ) or 0

    # ------------------------------------------
    # Assign Keys to New Departments
    #
    # If no new departments exist, an empty
    # DataFrame is returned.
    # ------------------------------------------

    return (
        new_departments
        .withColumn(
            "department_key",
            dense_rank().over(window)
            + lit(max_key)
        )
        .select(
            "department_key",
            "hospital_key",
            "department"
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


def write_dim_department(
    spark: SparkSession,
    df: DataFrame
) -> None:
    """
    Writes the Department Dimension.
    """

    logger.info(
        "Writing Department Dimension."
    )

    # ------------------------------------------
    # Initial Load
    # ------------------------------------------

    if not DeltaTable.isDeltaTable(
        spark,
        gold_dim_department_path
    ):

        logger.info(
            "Creating Department Dimension."
        )

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(gold_dim_department_path)
        )

        logger.info(
            "Department Dimension created successfully."
        )

        return

    # ------------------------------------------
    # Incremental Load
    # ------------------------------------------

    logger.info(
        "Updating Department Dimension."
    )

    delta_table = DeltaTable.forPath(
        spark,
        gold_dim_department_path
    )

    (
        delta_table.alias("target")
        .merge(
            df.alias("source"),
            """
            target.hospital_key = source.hospital_key
            AND target.department = source.department
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(
        "Department Dimension updated successfully."
    )


def build_dim_department(
    spark: SparkSession
) -> None:
    """
    Builds the Department Dimension.
    """

    logger.info(
        "Starting Department Dimension build."
    )

    silver_df = read_silver(
        spark
    )

    department_df = extract_departments(
        silver_df
    )

    hospital_df = read_dim_hospital(
        spark
    )

    department_df = attach_hospital_keys(
        department_df,
        hospital_df
    )

    existing_df = read_dim_department(
        spark
    )

    department_df = generate_department_keys(
        department_df,
        existing_df
    )

    write_dim_department(
        spark,
        department_df
    )

    logger.info(
        "Department Dimension build completed successfully."
    )