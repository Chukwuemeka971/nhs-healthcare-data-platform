from src.quality.quality_runner import run_gold_quality_checks
from src.utils.spark import get_spark


spark = get_spark(
    "Test Gold Quality"
)

try:

    results = run_gold_quality_checks(
        spark
    )

    print("\nQUALITY RESULTS")

    for result in results:

        print(result)

finally:

    spark.stop()