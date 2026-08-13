import glob
import os

import pandas as pd

from pyspark.sql import DataFrame, SparkSession

from src.config.config import load_config
from src.metadata.processed_files import already_processed
from src.utils.logger import get_logger


logger = get_logger(__name__)


def extract_patient_data(
    spark: SparkSession
) -> tuple[DataFrame | None, str | None]:
    """
    Extracts the most recent unprocessed patient admission Excel file
    from the landing layer and converts it to a Spark DataFrame.

    Args:
        spark:
            Active Spark session.

    Returns:
        tuple:
            - Spark DataFrame or None
            - Source filename or None

    Raises:
        FileNotFoundError:
            If no Excel files are found.

        Exception:
            If extraction fails.
    """

    try:

        logger.info(
            "Starting patient data extraction."
        )

        config = load_config()

        landing_folder = (
            config["storage"]["landing"]
        )

        excel_files = glob.glob(
            os.path.join(
                landing_folder,
                "*.xlsx"
            )
        )

        if not excel_files:

            raise FileNotFoundError(
                "No Excel files found in the landing folder."
            )

        logger.info(
            "Found %d Excel file(s).",
            len(excel_files)
        )

        landing_path = max(
            excel_files,
            key=os.path.getmtime
        )

        filename = os.path.basename(
            landing_path
        )

        logger.info(
            "Selected most recent file: %s",
            filename
        )

        if already_processed(filename):

            logger.warning(
                "Skipping already processed file: %s",
                filename
            )

            return None, None

        pdf = pd.read_excel(
            landing_path
        )

        logger.info(
            "Read %d records from Excel.",
            len(pdf)
        )

        df = spark.createDataFrame(
            pdf
        )

        logger.info(
            "Successfully converted Excel to Spark DataFrame."
        )

        return df, filename

    except Exception as error:

        logger.exception(
            "Patient extraction failed: %s",
            error
        )

        raiseimport glob
import os

import pandas as pd

from pyspark.sql import DataFrame, SparkSession

from src.config.config import load_config
from src.metadata.processed_files import already_processed
from src.utils.logger import get_logger


logger = get_logger(__name__)


def extract_patient_data(
    spark: SparkSession
) -> tuple[DataFrame | None, str | None]:
    """
    Extracts the most recent unprocessed patient admission Excel file
    from the landing layer and converts it to a Spark DataFrame.

    Args:
        spark:
            Active Spark session.

    Returns:
        tuple:
            - Spark DataFrame or None
            - Source filename or None

    Raises:
        FileNotFoundError:
            If no Excel files are found.

        Exception:
            If extraction fails.
    """

    try:

        logger.info(
            "Starting patient data extraction."
        )

        config = load_config()

        landing_folder = (
            config["storage"]["landing"]
        )

        excel_files = glob.glob(
            os.path.join(
                landing_folder,
                "*.xlsx"
            )
        )

        if not excel_files:

            raise FileNotFoundError(
                "No Excel files found in the landing folder."
            )

        logger.info(
            "Found %d Excel file(s).",
            len(excel_files)
        )

        landing_path = max(
            excel_files,
            key=os.path.getmtime
        )

        filename = os.path.basename(
            landing_path
        )

        logger.info(
            "Selected most recent file: %s",
            filename
        )

        if already_processed(filename):

            logger.warning(
                "Skipping already processed file: %s",
                filename
            )

            return None, None

        pdf = pd.read_excel(
            landing_path
        )

        logger.info(
            "Read %d records from Excel.",
            len(pdf)
        )

        df = spark.createDataFrame(
            pdf
        )

        logger.info(
            "Successfully converted Excel to Spark DataFrame."
        )

        return df, filename

    except Exception as error:

        logger.exception(
            "Patient extraction failed: %s",
            error
        )

        raiseimport glob
import os

import pandas as pd

from pyspark.sql import DataFrame, SparkSession

from src.config.config import load_config
from src.metadata.processed_files import already_processed
from src.utils.logger import get_logger


logger = get_logger(__name__)


def extract_patient_data(
    spark: SparkSession
) -> tuple[DataFrame | None, str | None]:
    """
    Extracts the most recent unprocessed patient admission Excel file
    from the landing layer and converts it to a Spark DataFrame.

    Args:
        spark:
            Active Spark session.

    Returns:
        tuple:
            - Spark DataFrame or None
            - Source filename or None

    Raises:
        FileNotFoundError:
            If no Excel files are found.

        Exception:
            If extraction fails.
    """

    try:

        logger.info(
            "Starting patient data extraction."
        )

        config = load_config()

        landing_folder = (
            config["storage"]["landing"]
        )

        excel_files = glob.glob(
            os.path.join(
                landing_folder,
                "*.xlsx"
            )
        )

        if not excel_files:

            raise FileNotFoundError(
                "No Excel files found in the landing folder."
            )

        logger.info(
            "Found %d Excel file(s).",
            len(excel_files)
        )

        landing_path = max(
            excel_files,
            key=os.path.getmtime
        )

        filename = os.path.basename(
            landing_path
        )

        logger.info(
            "Selected most recent file: %s",
            filename
        )

        if already_processed(filename):

            logger.warning(
                "Skipping already processed file: %s",
                filename
            )

            return None, None

        pdf = pd.read_excel(
            landing_path
        )

        logger.info(
            "Read %d records from Excel.",
            len(pdf)
        )

        df = spark.createDataFrame(
            pdf
        )

        logger.info(
            "Successfully converted Excel to Spark DataFrame."
        )

        return df, filename

    except Exception as error:

        logger.exception(
            "Patient extraction failed: %s",
            error
        )

        raise