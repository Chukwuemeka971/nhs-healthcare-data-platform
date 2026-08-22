from pyspark.sql.types import StructType


def compare_schema(
    existing_schema: StructType,
    incoming_schema: StructType,
):
    """
    Compare two Spark schemas and detect:

    - New columns
    - Removed columns
    - Data type changes
    """

    existing = {
        field.name: field.dataType.simpleString()
        for field in existing_schema.fields
    }

    incoming = {
        field.name: field.dataType.simpleString()
        for field in incoming_schema.fields
    }

    new_columns = [
        column
        for column in incoming
        if column not in existing
    ]

    removed_columns = [
        column
        for column in existing
        if column not in incoming
    ]

    type_changes = []

    for column in existing.keys() & incoming.keys():

        if existing[column] != incoming[column]:

            type_changes.append(
                {
                    "column": column,
                    "old_type": existing[column],
                    "new_type": incoming[column],
                }
            )

    return {
        "new_columns": new_columns,
        "removed_columns": removed_columns,
        "type_changes": type_changes,
    }