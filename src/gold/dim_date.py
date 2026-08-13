import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    dayofweek,
    month,
    quarter,
    weekofyear,
    when,
    year,
)

from src.config.config import load_config
from src.utils.logger import get_logger


logger = get_logger(__name__)
config = load_config()


gold_path = os.path.join(
    config["storage"]["gold"],
    config["datasets"]["dim_date"],
)


def create_date_dimension(
    spark: SparkSession,
) -> DataFrame:
    """
    Creates the Date Dimension and writes it
    to the Gold layer.

    The Date Dimension is static and is
    regenerated on each pipeline run.
    """

    logger.info(
        "Creating Date Dimension."
    )

    start_date = "2020-01-01"
    end_date = "2035-12-31"

    date_df = spark.sql(
        f"""
        SELECT explode(
            sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
            )
        ) AS full_date
        """
    )

    date_df = (
        date_df
        .withColumn(
            "date_key",
            date_format(
                col("full_date"),
                "yyyyMMdd",
            ).cast("int"),
        )
        .withColumn(
            "day",
            dayofmonth(col("full_date")),
        )
        .withColumn(
            "month",
            month(col("full_date")),
        )
        .withColumn(
            "month_name",
            date_format(
                col("full_date"),
                "MMMM",
            ),
        )
        .withColumn(
            "quarter",
            quarter(col("full_date")),
        )
        .withColumn(
            "year",
            year(col("full_date")),
        )
        .withColumn(
            "week_of_year",
            weekofyear(col("full_date")),
        )
        .withColumn(
            "day_of_week",
            dayofweek(col("full_date")),
        )
        .withColumn(
            "day_name",
            date_format(
                col("full_date"),
                "EEEE",
            ),
        )
        .withColumn(
            "is_weekend",
            when(
                dayofweek(
                    col("full_date")
                ).isin(1, 7),
                "Yes",
            ).otherwise(
                "No"
            ),
        )
        .select(
            "date_key",
            "full_date",
            "day",
            "month",
            "month_name",
            "quarter",
            "year",
            "week_of_year",
            "day_of_week",
            "day_name",
            "is_weekend",
        )
    )

    logger.info(
        "Writing Date Dimension to %s",
        gold_path,
    )

    (
        date_df.write
        .format("delta")
        .mode("overwrite")
        .save(gold_path)
    )

    logger.info(
        "Date Dimension created successfully."
    )

    return date_df