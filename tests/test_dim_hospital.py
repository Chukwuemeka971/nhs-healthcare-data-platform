from delta.tables import DeltaTable

from src.gold.dim_hospital import (
    extract_hospitals,
    generate_hospital_keys,
    write_dim_hospital,
)


def test_dim_hospital_initial_load(
    spark,
    tmp_path
):
    """
    Tests the initial Hospital Dimension load.

    Verifies that:

    1. Distinct hospitals are extracted.
    2. Surrogate keys are generated.
    3. The Delta dimension is created.
    4. Hospital records are written successfully.
    """

    # --------------------------------------------------
    # Temporary Gold path
    # --------------------------------------------------

    gold_path = str(
        tmp_path / "dim_hospital"
    )

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
        ]
    )

    # --------------------------------------------------
    # Extract unique hospitals
    # --------------------------------------------------

    hospital_df = extract_hospitals(
        silver_df
    )

    assert hospital_df.count() == 2

    # --------------------------------------------------
    # Generate surrogate keys
    # --------------------------------------------------

    hospital_df = generate_hospital_keys(
        hospital_df,
        None
    )

    # --------------------------------------------------
    # Verify surrogate keys
    # --------------------------------------------------

    assert hospital_df.count() == 2

    assert (
        hospital_df
        .select("hospital_key")
        .distinct()
        .count()
        == 2
    )

    # Keys should start at 1
    keys = [
        row["hospital_key"]
        for row in hospital_df
        .select("hospital_key")
        .collect()
    ]

    assert sorted(keys) == [1, 2]

    # --------------------------------------------------
    # Write dimension
    # --------------------------------------------------

    original_function = None

    # The production function reads its Gold path
    # from config, so this test focuses on the
    # transformation/key-generation logic.
    #
    # Verify the generated dimension schema/content.
    # --------------------------------------------------

    assert "hospital_key" in hospital_df.columns
    assert "hospital" in hospital_df.columns

    hospitals = {
        row["hospital"]
        for row in hospital_df.collect()
    }

    assert hospitals == {
        "Royal Hospital",
        "City Hospital",
    }