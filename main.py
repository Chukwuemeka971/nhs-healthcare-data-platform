from src.orchestration.pipeline_runner import run_pipeline
from src.utils.logger import get_logger
from src.utils.spark import get_spark

logger = get_logger(__name__)


def main():

    spark = None

    try:

        spark = get_spark(
            "Healthcare Pipeline"
        )

        run_pipeline(
            spark
        )

    except Exception as error:

        logger.exception(
            "Pipeline failed: %s",
            error
        )

        raise

    finally:

        if spark is not None:

            spark.stop()

            logger.info(
                "Spark session stopped."
            )


if __name__ == "__main__":

    main()