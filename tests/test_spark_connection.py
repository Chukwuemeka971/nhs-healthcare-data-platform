from src.utils.spark import get_spark


def test_spark_connection():

    """
    Tests that Spark can be created and execute
    a simple DataFrame operation.
    """

    spark = get_spark(
        "Spark Connection Test"
    )

    result = (
        spark.range(10)
        .count()
    )

    assert result == 10