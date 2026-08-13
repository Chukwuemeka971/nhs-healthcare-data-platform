"""
from src.utils.spark import get_spark
from src.gold.dim_hospital import read_dim_hospital
from src.gold.dim_patient import read_dim_patient
from src.gold.dim_department import read_dim_department
from pyspark.sql.functions import col

from src.utils.spark import get_spark
from src.gold.dim_date import create_date_dimension



spark = get_spark("Verify Gold")

df_hosp = read_dim_hospital(spark)
df_dep = read_dim_department(spark)
df_patient = read_dim_patient(spark)

df_hosp.show(truncate=False)
df_dep.show(truncate = False)
df_patient.show(truncate=False)

from src.gold.dim_date import create_date_dimension
from src.utils.spark import get_spark

spark = get_spark("Date Dimension Test")

create_date_dimension(spark)
"""

"""
date_df = create_date_dimension(spark)

date_df.show(10, truncate=False)
date_df.printSchema()
"""


from src.utils.spark import get_spark
from src.gold.dim_date import create_date_dimension
from src.gold.fact_patient_admissions import(
    read_silver,
    attach_patient_key,
    read_dim_patient,
    read_dim_hospital,
    attach_hospital_key,
    attach_department_key,
    read_dim_department,
    read_dim_date,
    attach_date_key
)  

spark = get_spark("Date Dimension")


silver_df = read_silver(spark)
patient_df = read_dim_patient(spark)
hospital_df = read_dim_hospital(spark)
department_df = read_dim_department(spark)
date_df = read_dim_date(spark)

fact_df = attach_patient_key(
    silver_df,
    patient_df
)

fact_df = attach_hospital_key(
    fact_df,
    hospital_df
)

"""
fact_df.select(
    "patient_key",
    "hospital_key",
    "hospital"
).show(10, truncate=False)
"""



fact_df = attach_department_key(
    fact_df,
    department_df
)

fact_df = attach_date_key(
    fact_df,
    date_df
)

fact_df.select(
    "patient_key",
    "hospital_key",
    "department_key",
    "date_key",
    "admission_date"
).show(20, truncate=False)

spark.stop()