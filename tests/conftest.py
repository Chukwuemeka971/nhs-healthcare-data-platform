import pytest

from src.utils.spark import get_spark
from tests.fixtures import create_patient_records


@pytest.fixture(scope="session")
def spark():
    """
    Creates one Spark session for the entire pytest session.
    """

    spark_session = get_spark(
        "Healthcare Pipeline Tests"
    )

    yield spark_session

    try:
        spark_session.stop()
    except Exception:
        pass


@pytest.fixture
def patient_records():
    """
    Provides reusable patient records for tests.
    """

    return create_patient_records()


@pytest.fixture
def patient_dataframe(
    spark,
    patient_records
):
    """
    Creates a Spark DataFrame from the test patient records.
    """

    return spark.createDataFrame(
        patient_records
    )