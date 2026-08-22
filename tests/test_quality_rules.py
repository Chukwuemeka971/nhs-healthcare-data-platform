from datetime import date, timedelta

from src.validate.quality_rules import (
    check_admission_type,
    check_duplicate_episode_ids,
    check_future_admission_date,
    check_missing_hospital,
    check_missing_patient_id,
)


def test_missing_patient_id(patient_dataframe):

    valid_df, invalid_df = check_missing_patient_id(
        patient_dataframe
    )

    assert valid_df.count() == 2
    assert invalid_df.count() == 0


def test_missing_patient_id_is_detected(
    spark,
    patient_records,
):

    patient_records[0]["patient_id"] = None

    df = spark.createDataFrame(
        patient_records
    )

    valid_df, invalid_df = check_missing_patient_id(
        df
    )

    assert valid_df.count() == 1
    assert invalid_df.count() == 1


def test_missing_hospital_is_detected(
    spark,
    patient_records,
):

    patient_records[0]["hospital"] = None

    df = spark.createDataFrame(
        patient_records
    )

    valid_df, invalid_df = check_missing_hospital(
        df
    )

    assert valid_df.count() == 1
    assert invalid_df.count() == 1


def test_invalid_admission_type_is_detected(
    spark,
    patient_records,
):

    patient_records[0]["admission_type"] = "Invalid Type"

    df = spark.createDataFrame(
        patient_records
    )

    valid_df, invalid_df = check_admission_type(
        df
    )

    assert valid_df.count() == 1
    assert invalid_df.count() == 1


def test_future_admission_date_is_detected(
    spark,
    patient_records,
):

    patient_records[0]["admission_date"] = (
        date.today() + timedelta(days=10)
    )

    df = spark.createDataFrame(
        patient_records
    )

    valid_df, invalid_df = (
        check_future_admission_date(df)
    )

    assert valid_df.count() == 1
    assert invalid_df.count() == 1


def test_duplicate_episode_id_is_detected(
    spark,
    patient_records,
):

    duplicate_record = (
        patient_records[0].copy()
    )

    patient_records.append(
        duplicate_record
    )

    df = spark.createDataFrame(
        patient_records
    )

    valid_df, invalid_df = check_duplicate_episode_ids(
        df
    )

    assert valid_df.count() == 1
    assert invalid_df.count() == 2