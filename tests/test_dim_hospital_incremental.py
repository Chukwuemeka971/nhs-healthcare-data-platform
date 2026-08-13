from src.gold.dim_hospital import (
    extract_hospitals,
    generate_hospital_keys,
)


def test_dim_hospital_incremental_load(
    spark
):
    """
    Tests incremental Hospital Dimension processing.

    Verifies that:

    1. Existing hospitals are detected.
    2. Existing hospitals are not returned as new records.
    3. A new hospital receives a new surrogate key.
    4. The new surrogate key continues from the existing maximum key.
    """

    # --------------------------------------------------
    # Existing Gold Hospital Dimension
    # --------------------------------------------------

    existing_dim = spark.createDataFrame(
        [
            (1, "Royal Hospital"),
            (2, "City Hospital"),
        ],
        [
            "hospital_key",
            "hospital",
        ]
    )

    # --------------------------------------------------
    # New Silver batch
    #
    # Royal Hospital = existing
    # City Hospital  = existing
    # New Hospital   = new
    # --------------------------------------------------

    silver_df = spark.createDataFrame(
        [
            ("P001", "Royal Hospital", "Cardiology"),
            ("P002", "City Hospital", "Emergency"),
            ("P003", "New Hospital", "Neurology"),
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

    # --------------------------------------------------
    # Generate incremental hospital keys
    # --------------------------------------------------

    new_hospitals = generate_hospital_keys(
        hospital_df,
        existing_dim
    )

    # --------------------------------------------------
    # Collect result once
    # --------------------------------------------------

    rows = new_hospitals.collect()

    # --------------------------------------------------
    # Verify only one new hospital was returned
    # --------------------------------------------------

    assert len(rows) == 1

    # --------------------------------------------------
    # Verify new hospital and surrogate key
    # --------------------------------------------------

    new_hospital = rows[0]

    assert new_hospital["hospital"] == "New Hospital"
    assert new_hospital["hospital_key"] == 3