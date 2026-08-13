import os

from delta.tables import DeltaTable

from pyspark.sql import (
    DataFrame,
    SparkSession,
    Window,
)

from pyspark.sql.functions import (
    col,
    lit,
    max as spark_max,
    row_number,
)

from src.config.config import load_config
from src.utils.logger import get_logger


logger = get_logger(__name__)
config = load_config()


silver_path = os.path.join(
    config["storage"]["silver"],
    config["datasets"]["patient_admissions"],
)

gold_path = os.path.join(
    config["storage"]["gold"],
    config["datasets"]["fact_patient_admissions"],
)

patient_path = os.path.join(
    config["storage"]["gold"],
    config["datasets"]["dim_patient"],
)

hospital_path = os.path.join(
    config["storage"]["gold"],
    config["datasets"]["dim_hospital"],
)

department_path = os.path.join(
    config["storage"]["gold"],
    config["datasets"]["dim_department"],
)

date_path = os.path.join(
    config["storage"]["gold"],
    config["datasets"]["dim_date"],
)


# ==========================================================
# READ FUNCTIONS
# ==========================================================

def read_silver(
    spark: SparkSession,
) -> DataFrame:

    logger.info(
        "Reading Silver patient admissions."
    )

    return (
        spark.read
        .format("delta")
        .load(silver_path)
    )


def read_dim_patient(
    spark: SparkSession,
) -> DataFrame:

    logger.info(
        "Reading Patient Dimension."
    )

    return (
        spark.read
        .format("delta")
        .load(patient_path)
    )


def read_dim_hospital(
    spark: SparkSession,
) -> DataFrame:

    logger.info(
        "Reading Hospital Dimension."
    )

    return (
        spark.read
        .format("delta")
        .load(hospital_path)
    )


def read_dim_department(
    spark: SparkSession,
) -> DataFrame:

    logger.info(
        "Reading Department Dimension."
    )

    return (
        spark.read
        .format("delta")
        .load(department_path)
    )


def read_dim_date(
    spark: SparkSession,
) -> DataFrame:

    logger.info(
        "Reading Date Dimension."
    )

    return (
        spark.read
        .format("delta")
        .load(date_path)
    )


def read_fact_patient_admissions(
    spark: SparkSession,
) -> DataFrame | None:

    logger.info(
        "Reading Fact Patient Admissions."
    )

    if DeltaTable.isDeltaTable(
        spark,
        gold_path,
    ):

        logger.info(
            "Existing Fact Patient Admissions found."
        )

        return (
            spark.read
            .format("delta")
            .load(gold_path)
        )

    logger.info(
        "Fact Patient Admissions does not exist."
    )

    return None


# ==========================================================
# DIMENSION KEY LOOKUPS
# ==========================================================

def attach_patient_key(
    silver_df: DataFrame,
    dim_patient_df: DataFrame,
) -> DataFrame:

    logger.info(
        "Attaching patient surrogate keys."
    )

    return (
        silver_df.alias("s")
        .join(
            dim_patient_df.select(
                "patient_id",
                "patient_key",
            ).alias("p"),
            col("s.patient_id")
            == col("p.patient_id"),
            "left",
        )
        .select(
            col("p.patient_key"),
            col("s.*"),
        )
    )


def attach_hospital_key(
    fact_df: DataFrame,
    dim_hospital_df: DataFrame,
) -> DataFrame:

    logger.info(
        "Attaching hospital surrogate keys."
    )

    return (
        fact_df.alias("f")
        .join(
            dim_hospital_df.select(
                "hospital",
                "hospital_key",
            ).alias("h"),
            col("f.hospital")
            == col("h.hospital"),
            "left",
        )
        .select(
            col("f.*"),
            col("h.hospital_key"),
        )
    )


def attach_department_key(
    fact_df: DataFrame,
    dim_department_df: DataFrame,
) -> DataFrame:

    logger.info(
        "Attaching department surrogate keys."
    )

    return (
        fact_df.alias("f")
        .join(
            dim_department_df.select(
                "hospital_key",
                "department",
                "department_key",
            ).alias("d"),
            (
                col("f.hospital_key")
                == col("d.hospital_key")
            )
            &
            (
                col("f.department")
                == col("d.department")
            ),
            "left",
        )
        .select(
            col("f.*"),
            col("d.department_key"),
        )
    )


def attach_date_key(
    fact_df: DataFrame,
    dim_date_df: DataFrame,
) -> DataFrame:

    logger.info(
        "Attaching date surrogate keys."
    )

    return (
        fact_df.alias("f")
        .join(
            dim_date_df.select(
                "full_date",
                "date_key",
            ).alias("d"),
            col("f.admission_date")
            == col("d.full_date"),
            "left",
        )
        .select(
            col("f.*"),
            col("d.date_key"),
        )
    )


def validate_dimension_keys(
    fact_df: DataFrame,
) -> DataFrame:
    """
    Keeps only records with successfully
    resolved dimension surrogate keys.
    """

    logger.info(
        "Validating dimension surrogate keys."
    )

    return (
        fact_df
        .filter(
            col("patient_key").isNotNull()
            & col("hospital_key").isNotNull()
            & col("department_key").isNotNull()
            & col("date_key").isNotNull()
        )
    )


# ==========================================================
# INCREMENTAL FACT LOGIC
# ==========================================================

def find_new_fact_records(
    new_fact_df: DataFrame,
    existing_fact_df: DataFrame | None,
) -> DataFrame:

    if existing_fact_df is None:

        logger.info(
            "Initial Fact Patient Admissions load."
        )

        return new_fact_df

    logger.info(
        "Finding new fact records."
    )

    return (
        new_fact_df.alias("new")
        .join(
            existing_fact_df.select(
                "patient_key",
                "hospital_key",
                "department_key",
                "date_key",
            ).alias("existing"),
            (
                col("new.patient_key")
                == col("existing.patient_key")
            )
            &
            (
                col("new.hospital_key")
                == col("existing.hospital_key")
            )
            &
            (
                col("new.department_key")
                == col("existing.department_key")
            )
            &
            (
                col("new.date_key")
                == col("existing.date_key")
            ),
            "left_anti",
        )
    )


def generate_new_fact_keys(
    new_fact_df: DataFrame,
    existing_fact_df: DataFrame | None,
) -> DataFrame:

    logger.info(
        "Generating fact surrogate keys."
    )

    if existing_fact_df is None:

        max_key = 0

    else:

        max_key = (
            existing_fact_df
            .agg(
                spark_max("fact_key")
                .alias("max_key")
            )
            .first()["max_key"]
        ) or 0

    window = Window.orderBy(
        "patient_key",
        "date_key",
        "hospital_key",
        "department_key",
    )

    return (
        new_fact_df
        .withColumn(
            "fact_key",
            row_number().over(window)
            + lit(max_key),
        )
    )


def build_fact_table(
    fact_df: DataFrame,
) -> DataFrame:

    logger.info(
        "Building final Fact Patient Admissions structure."
    )

    return (
        fact_df.select(
            "fact_key",
            "patient_key",
            "hospital_key",
            "department_key",
            "date_key",
            "ward",
            "admission_type",
            "created_at",
            "updated_at",
            "pipeline_name",
            "batch_id",
        )
    )


# ==========================================================
# WRITE FUNCTIONS
# ==========================================================

def merge_fact_patient_admissions(
    spark: SparkSession,
    new_fact_df: DataFrame,
) -> None:

    logger.info(
        "Writing Fact Patient Admissions."
    )

    # ------------------------------------------
    # Initial Load
    # ------------------------------------------

    if not DeltaTable.isDeltaTable(
        spark,
        gold_path,
    ):

        logger.info(
            "Creating Fact Patient Admissions table."
        )

        (
            new_fact_df.write
            .format("delta")
            .mode("overwrite")
            .save(gold_path)
        )

        logger.info(
            "Fact Patient Admissions table created successfully."
        )

        return

    # ------------------------------------------
    # Incremental Load
    # ------------------------------------------

    logger.info(
        "Merging Fact Patient Admissions."
    )

    delta_table = DeltaTable.forPath(
        spark,
        gold_path,
    )

    (
        delta_table.alias("target")
        .merge(
            new_fact_df.alias("source"),
            """
            target.patient_key = source.patient_key
            AND target.hospital_key = source.hospital_key
            AND target.department_key = source.department_key
            AND target.date_key = source.date_key
            """,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(
        "Fact Patient Admissions merge completed successfully."
    )


# ==========================================================
# BUILD FACT TABLE
# ==========================================================

def build_fact_patient_admissions(
    spark: SparkSession,
) -> DataFrame:

    logger.info(
        "Starting Fact Patient Admissions build."
    )

    # ------------------------------------------
    # Read Source and Dimensions
    # ------------------------------------------

    silver_df = read_silver(
        spark
    )

    patient_df = read_dim_patient(
        spark
    )

    hospital_df = read_dim_hospital(
        spark
    )

    department_df = read_dim_department(
        spark
    )

    date_df = read_dim_date(
        spark
    )

    # ------------------------------------------
    # Attach Dimension Keys
    # ------------------------------------------

    fact_df = attach_patient_key(
        silver_df,
        patient_df,
    )

    fact_df = attach_hospital_key(
        fact_df,
        hospital_df,
    )

    fact_df = attach_department_key(
        fact_df,
        department_df,
    )

    fact_df = attach_date_key(
        fact_df,
        date_df,
    )

    # ------------------------------------------
    # Validate Dimension Lookups
    # ------------------------------------------

    fact_df = validate_dimension_keys(
        fact_df
    )

    # ------------------------------------------
    # Read Existing Fact
    # ------------------------------------------

    existing_fact_df = (
        read_fact_patient_admissions(
            spark
        )
    )

    # ------------------------------------------
    # Identify New Facts
    # ------------------------------------------

    new_fact_df = find_new_fact_records(
        fact_df,
        existing_fact_df,
    )

    # ------------------------------------------
    # Generate Fact Keys
    # ------------------------------------------

    new_fact_df = generate_new_fact_keys(
        new_fact_df,
        existing_fact_df,
    )

    # ------------------------------------------
    # Final Fact Structure
    # ------------------------------------------

    new_fact_df = build_fact_table(
        new_fact_df
    )

    # ------------------------------------------
    # Write Fact Table
    # ------------------------------------------

    merge_fact_patient_admissions(
        spark,
        new_fact_df,
    )

    logger.info(
        "Fact Patient Admissions build completed successfully."
    )

    return new_fact_df