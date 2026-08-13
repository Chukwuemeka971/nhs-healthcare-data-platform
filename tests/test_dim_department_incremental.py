from src.gold.dim_department import (
    extract_departments,
    attach_hospital_keys,
    generate_department_keys,
)


def test_dim_department_incremental_load(
    spark
):
    """
    Tests incremental Department Dimension processing.

    Verifies that:

    1. Existing hospital/department combinations are ignored.
    2. A new department within an existing hospital is detected.
    3. The new department receives the next surrogate key.
    4. Department names belonging to different hospitals are
       treated as separate combinations.
    """

    # --------------------------------------------------
    # Existing Gold Hospital Dimension
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
    # Existing Gold Department Dimension
    #
    # Hospital 1:
    #   Cardiology
    #   Emergency
    #
    # Hospital 2:
    #   Neurology
    # --------------------------------------------------

    existing_department_df = spark.createDataFrame(
        [
            (
                1,
                1,
                "Cardiology"
            ),
            (
                2,
                1,
                "Emergency"
            ),
            (
                3,
                2,
                "Neurology"
            ),
        ],
        [
            "department_key",
            "hospital_key",
            "department",
        ]
    )

    # --------------------------------------------------
    # New Silver batch
    #
    # Existing:
    #   Royal Hospital → Cardiology
    #   Royal Hospital → Emergency
    #   City Hospital  → Neurology
    #
    # New:
    #   City Hospital  → ICU
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
                "ICU"
            ),
        ],
        [
            "patient_id",
            "hospital",
            "department",
        ]
    )

    # --------------------------------------------------
    # Extract unique departments
    # --------------------------------------------------

    department_df = extract_departments(
        silver_df
    )

    assert department_df.count() == 4

    # --------------------------------------------------
    # Attach hospital surrogate keys
    # --------------------------------------------------

    department_df = attach_hospital_keys(
        department_df,
        hospital_df
    )

    # --------------------------------------------------
    # Verify all departments received hospital keys
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
    # Generate incremental department keys
    # --------------------------------------------------

    new_departments = generate_department_keys(
        department_df,
        existing_department_df
    )

    # --------------------------------------------------
    # Only ICU should be new
    # --------------------------------------------------

    assert new_departments.count() == 1

    # --------------------------------------------------
    # Read the new department
    # --------------------------------------------------

    new_department = (
        new_departments
        .collect()[0]
    )

    # --------------------------------------------------
    # Verify department name
    # --------------------------------------------------

    assert (
        new_department["department"]
        == "ICU"
    )

    # --------------------------------------------------
    # Verify correct hospital
    # --------------------------------------------------

    assert (
        new_department["hospital_key"]
        == 2
    )

    # --------------------------------------------------
    # Verify new surrogate key
    # --------------------------------------------------

    assert (
        new_department["department_key"]
        == 4
    )

    # --------------------------------------------------
    # Verify existing departments were ignored
    # --------------------------------------------------

    departments = {
        row["department"]
        for row in new_departments.collect()
    }

    assert departments == {
        "ICU"
    }

    # --------------------------------------------------
    # Verify hospital/department combination
    # --------------------------------------------------

    combinations = {
        (
            row["hospital_key"],
            row["department"]
        )
        for row in new_departments.collect()
    }

    assert combinations == {
        (2, "ICU")
    }

    # --------------------------------------------------
    # Verify new surrogate key is unique
    # --------------------------------------------------

    assert (
        new_departments
        .select("department_key")
        .distinct()
        .count()
        == 1
    )