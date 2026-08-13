from src.gold.dim_hospital import (
    extract_hospitals,
    generate_hospital_keys,
)


def test_dim_hospital_initial_load(
    spark,
):
    """
    Tests the initial Hospital Dimension load.

    Verifies that:

    1. Distinct hospitals are extracted.
    2. Surrogate keys are generated.
    3. Keys start at 1.
    4. The expected hospital records are produced.
    """

    # --------------------------------------------------
    # Create test Silver data
    # --------------------------------------------------

    silver_df = spark.createDataFrame(
        [
            ("P001", "Royal Hospital", "Cardiology"),
            ("P002", "Royal Hospital", "Emergency"),
            ("P003", "City Hospital", "Neurology"),
        ],
        [
            "patient_id",
            "hospital",
            "department",
        ],
    )

    # --------------------------------------------------
    # Extract unique hospitals
    # --------------------------------------------------

    hospital_df = extract_hospitals(
        silver_df
    )

    # --------------------------------------------------
    # Generate surrogate keys
    # --------------------------------------------------

    hospital_df = generate_hospital_keys(
        hospital_df,
        None,
    )

    # --------------------------------------------------
    # Collect once
    # --------------------------------------------------

    rows = hospital_df.collect()

    # --------------------------------------------------
    # Assertions
    # --------------------------------------------------

    assert len(rows) == 2

    assert "hospital_key" in hospital_df.columns
    assert "hospital" in hospital_df.columns

    keys = sorted(
        row["hospital_key"]
        for row in rows
    )

    assert keys == [1, 2]

    hospitals = {
        row["hospital"]
        for row in rows
    }

    assert hospitals == {
        "Royal Hospital",
        "City Hospital",
    }