from src.utils.spark import get_spark
from src.extract.extract_patients import extract_patient_data
from src.pipelines.bronze_pipeline import build_bronze_layer

spark = get_spark("Bronze Test")

patients, filename = extract_patient_data()

if patients is not None:
    build_bronze_layer(
        spark,
        patients,
        filename
    )

spark.stop()                                       