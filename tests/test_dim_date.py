import os

from src.gold.dim_date import create_date_dimension


def test_dim_date_creation(
    spark,
    tmp_path,
    monkeypatch,
):
    """
    Tests that the Date Dimension is created
    with the expected structure and values.
    """

    gold_path = os.path.join(
        str(tmp_path),
        "gold",
        "dim_date",
    )

    # ------------------------------------------
    # Patch Gold path
    # ------------------------------------------

    monkeypatch.setattr(
        "src.gold.dim_date.gold_path",
        gold_path,
    )

    # ------------------------------------------
    # Build Date Dimension
    # ------------------------------------------

    result_df = create_date_dimension(
        spark
    )

    # ------------------------------------------
    # Read persisted Delta table
    # ------------------------------------------

    saved_df = (
        spark.read
        .format("delta")
        .load(gold_path)
    )

    # ------------------------------------------
    # Validate expected date range
    # ------------------------------------------

    assert result_df.count() == 5844

    assert saved_df.count() == 5844

    # ------------------------------------------
    # Validate key generation
    # ------------------------------------------

    sample = (
        saved_df
        .filter(
            "full_date = '2026-01-10'"
        )
        .first()
    )

    assert sample["date_key"] == 20260110

    # ------------------------------------------
    # Validate required columns
    # ------------------------------------------

    expected_columns = {
        "date_key",
        "full_date",
        "day",
        "month",
        "month_name",
        "quarter",
        "year",
        "week_of_year",
        "day_of_week",
        "day_name",
        "is_weekend",
    }

    assert expected_columns.issubset(
        set(saved_df.columns)
    )

    # ------------------------------------------
    # Validate weekend logic
    # ------------------------------------------

    saturday = (
        saved_df
        .filter(
            "full_date = '2026-01-10'"
        )
        .first()
    )

    assert saturday["is_weekend"] == "Yes"