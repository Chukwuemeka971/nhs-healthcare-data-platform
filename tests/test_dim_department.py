from src.gold.dim_department import (
    extract_departments,
    attach_hospital_keys,
    generate_department_keys,
)


def test_dim_department_initial_load(
    spark
):
    """
    Tests the initial Department Dimension transformation.

    Verifies that:

    1. Unique hospital/department combinations are extracted.
    2. Hospital surrogate keys are correctly attached.
    3. Department surrogate keys are generated.
    4. Department keys are unique.
    5. The initial department keys start from 1.
    """

    # --------------------------------------------------
    # Silver patient admission data
    # --------------------------------------------------

    silver_df = spark.createDataFrame(
        [
            (
                "P001",
                "Royal Hospital",
                "Cardiology"
            ),
            (
                "P002",
                "Royal Hospital",
                "Emergency"
            ),
            (
                "P003",
                "City Hospital",
                "Neurology"
            ),
            (
                "P004",
                "City Hospital",
                "Neurology"
            ),
        ],
        [
            "patient_id",
            "hospital",
            "department",
        ]
    )

    # --------------------------------------------------
    # Existing Hospital Dimension
    # --------------------------------------------------

    hospital_df = spark.createDataFrame(
        [
            (
                1,
                "Royal Hospital"
            ),
            (
                2,
                "City Hospital"
            ),
        ],
        [
            "hospital_key",
            "hospital",
        ]
    )

    # --------------------------------------------------
    # Extract unique departments
    # --------------------------------------------------

    department_df = extract_departments(
        silver_df
    )

    extracted_rows = department_df.collect()

    assert len(extracted_rows) == 3

    extracted_departments = {
        (
            row["hospital"],
            row["department"]
        )
        for row in extracted_rows
    }

    assert extracted_departments == {
        ("Royal Hospital", "Cardiology"),
        ("Royal Hospital", "Emergency"),
        ("City Hospital", "Neurology"),
    }

    # --------------------------------------------------
    # Attach hospital surrogate keys
    # --------------------------------------------------

    department_df = attach_hospital_keys(
        department_df,
        hospital_df
    )

    attached_rows = department_df.collect()

    # --------------------------------------------------
    # Verify hospital keys were attached
    # --------------------------------------------------

    assert all(
        row["hospital_key"] is not None
        for row in attached_rows
    )

    # --------------------------------------------------
    # Verify hospital/department relationships
    # --------------------------------------------------

    royal_departments = {
        row["department"]
        for row in attached_rows
        if row["hospital"] == "Royal Hospital"
    }

    assert royal_departments == {
        "Cardiology",
        "Emergency",
    }

    city_departments = {
        row["department"]
        for row in attached_rows
        if row["hospital"] == "City Hospital"
    }

    assert city_departments == {
        "Neurology",
    }

    # Verify correct hospital surrogate keys.

    royal_hospital_keys = {
        row["hospital_key"]
        for row in attached_rows
        if row["hospital"] == "Royal Hospital"
    }

    assert royal_hospital_keys == {1}

    city_hospital_keys = {
        row["hospital_key"]
        for row in attached_rows
        if row["hospital"] == "City Hospital"
    }

    assert city_hospital_keys == {2}

    # --------------------------------------------------
    # Generate Department surrogate keys
    # --------------------------------------------------

    department_df = generate_department_keys(
        department_df,
        None
    )

    final_rows = department_df.collect()

    # --------------------------------------------------
    # Verify final department records
    # --------------------------------------------------

    assert len(final_rows) == 3

    assert "department_key" in department_df.columns
    assert "hospital_key" in department_df.columns
    assert "department" in department_df.columns
    assert "created_at" in department_df.columns
    assert "updated_at" in department_df.columns

    # --------------------------------------------------
    # Verify department keys
    # --------------------------------------------------

    keys = [
        row["department_key"]
        for row in final_rows
    ]

    assert len(set(keys)) == 3
    assert sorted(keys) == [1, 2, 3]