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

    # --------------------------------------------------
    # Verify unique hospital/department combinations
    #
    # Expected:
    #
    # Royal Hospital → Cardiology
    # Royal Hospital → Emergency
    # City Hospital  → Neurology
    #
    # Total = 3
    # --------------------------------------------------

    assert department_df.count() == 3

    # --------------------------------------------------
    # Attach hospital surrogate keys
    # --------------------------------------------------

    department_df = attach_hospital_keys(
        department_df,
        hospital_df
    )

    # --------------------------------------------------
    # Verify hospital keys were attached
    # --------------------------------------------------

    assert (
        department_df
        .filter(
            "hospital_key IS NULL"
        )
        .count()
        == 0
    )

    # --------------------------------------------------
    # Verify Royal Hospital departments
    # --------------------------------------------------

    royal_departments = {
        row["department"]
        for row in (
            department_df
            .filter(
                "hospital = 'Royal Hospital'"
            )
            .collect()
        )
    }

    assert royal_departments == {
        "Cardiology",
        "Emergency",
    }

    # --------------------------------------------------
    # Verify City Hospital department
    # --------------------------------------------------

    city_departments = {
        row["department"]
        for row in (
            department_df
            .filter(
                "hospital = 'City Hospital'"
            )
            .collect()
        )
    }

    assert city_departments == {
        "Neurology",
    }

    # --------------------------------------------------
    # Generate Department surrogate keys
    # --------------------------------------------------

    department_df = generate_department_keys(
        department_df,
        None
    )

    # --------------------------------------------------
    # Verify department records
    # --------------------------------------------------

    assert department_df.count() == 3

    # --------------------------------------------------
    # Verify department_key exists
    # --------------------------------------------------

    assert (
        "department_key"
        in department_df.columns
    )

    # --------------------------------------------------
    # Verify department keys are unique
    # --------------------------------------------------

    assert (
        department_df
        .select("department_key")
        .distinct()
        .count()
        == 3
    )

    # --------------------------------------------------
    # Verify initial keys start at 1
    # --------------------------------------------------

    keys = [
        row["department_key"]
        for row in (
            department_df
            .select("department_key")
            .collect()
        )
    ]

    assert sorted(keys) == [1, 2, 3]

    # --------------------------------------------------
    # Verify expected hospital keys
    # --------------------------------------------------

    royal_hospital_keys = {
        row["hospital_key"]
        for row in (
            department_df
            .filter(
                "hospital = 'Royal Hospital'"
            )
            .collect()
        )
    }

    assert royal_hospital_keys == {1}

    city_hospital_keys = {
        row["hospital_key"]
        for row in (
            department_df
            .filter(
                "hospital = 'City Hospital'"
            )
            .collect()
        )
    }

    assert city_hospital_keys == {2}

    # --------------------------------------------------
    # Verify audit columns were generated
    # --------------------------------------------------

    assert "created_at" in department_df.columns
    assert "updated_at" in department_df.columns