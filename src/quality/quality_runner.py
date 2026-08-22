import os

from delta.tables import DeltaTable

from src.config.config import load_config
from src.quality.dimension_quality import check_dimension_quality
from src.quality.fact_quality import check_fact_quality
from src.utils.logger import get_logger


logger = get_logger(__name__)


def read_delta_table(
    spark,
    table_path: str,
    table_name: str,
):
    """
    Reads a Delta table and raises an error
    if the table does not exist.
    """

    if not DeltaTable.isDeltaTable(
        spark,
        table_path,
    ):

        raise FileNotFoundError(
            f"Delta table not found: {table_name}"
        )

    return (
        spark.read
        .format("delta")
        .load(table_path)
    )


def run_gold_quality_checks(
    spark,
):
    """
    Runs data quality checks on all Gold
    dimension and fact tables.

    Raises:
        ValueError:
            If any Gold quality check fails.
    """

    logger.info(
        "Starting Gold layer quality checks."
    )

    config = load_config()

    gold_path = config["storage"]["gold"]

    dataset_name = (
        config["datasets"][
            "patient_admissions"
        ]
    )

    silver_path = os.path.join(
        config["storage"]["silver"],
        dataset_name,
    )

    # -----------------------------------
    # Read Silver
    # -----------------------------------

    silver_df = read_delta_table(
        spark,
        silver_path,
        "Silver patient admissions",
    )

    # -----------------------------------
    # Dimension Tables and Primary Keys
    # -----------------------------------

    dimension_tables = {
        "dim_patient": "patient_key",
        "dim_hospital": "hospital_key",
        "dim_department": "department_key",
        "dim_date": "date_key",
    }

    dimensions = {}

    results = []

    # -----------------------------------
    # Read Dimensions and Run Checks
    # -----------------------------------

    for table_name, primary_key in dimension_tables.items():

        table_path = os.path.join(
            gold_path,
            table_name,
        )

        dimension_df = read_delta_table(
            spark,
            table_path,
            table_name,
        )

        dimensions[table_name] = dimension_df

        result = check_dimension_quality(
            df=dimension_df,
            dimension_name=table_name,
            primary_key=primary_key,
        )

        results.append(result)

    # -----------------------------------
    # Read Fact Table
    # -----------------------------------

    fact_table_name = (
        "fact_patient_admissions"
    )

    fact_table_path = os.path.join(
        gold_path,
        fact_table_name,
    )

    fact_df = read_delta_table(
        spark,
        fact_table_path,
        fact_table_name,
    )

    # -----------------------------------
    # Run Fact Quality Checks
    # -----------------------------------

    fact_result = check_fact_quality(
        silver_df=silver_df,
        fact_df=fact_df,
        patient_df=dimensions["dim_patient"],
        hospital_df=dimensions["dim_hospital"],
        department_df=dimensions["dim_department"],
        date_df=dimensions["dim_date"],
    )

    results.append(
        fact_result
    )

    # -----------------------------------
    # Quality Summary
    # -----------------------------------

    failed_results = [
        result
        for result in results
        if result["status"] != "PASS"
    ]

    logger.info(
        "=" * 60
    )

    logger.info(
        "GOLD QUALITY CHECK SUMMARY"
    )

    logger.info(
        "=" * 60
    )

    for result in results:

        table_name = (
            result.get("dimension")
            or result.get("table")
        )

        logger.info(
            "%s → %s",
            table_name,
            result["status"],
        )

    logger.info(
        "=" * 60
    )

    # -----------------------------------
    # Fail Pipeline if Any Check Failed
    # -----------------------------------

    if failed_results:

        failed_tables = [
            result.get("dimension")
            or result.get("table")
            for result in failed_results
        ]

        logger.error(
            "Gold quality checks failed for: %s",
            ", ".join(failed_tables),
        )

        raise ValueError(
            "Gold quality validation failed for: "
            + ", ".join(failed_tables)
        )

    logger.info(
        "All Gold quality checks passed successfully."
    )

    return results