import pytest

from src.validate.validation import (
    validate_required_columns
)


def test_required_columns_exist(
    patient_dataframe
):

    df = validate_required_columns(
        patient_dataframe
    )

    assert df.count() == 2