from src.utils.spark import get_spark


def test_spark_connection():

    spark = None

    try:

        spark = get_spark(
            "Spark Connection Test"
        )

        result = (
            spark.range(10)
            .count()
        )

        assert result == 10

    finally:

        if spark is not None:
            spark.stop()