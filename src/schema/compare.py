from pyspark.sql.types import StructType


def compare_schema(existing_schema: StructType,
                   incoming_schema: StructType):
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
        col
        for col in incoming
        if col not in existing
    ]

    removed_columns = [
        col
        for col in existing
        if col not in incoming
    ]

    type_changes = []

    for col in existing.keys() & incoming.keys():
        if existing[col] != incoming[col]:
            type_changes.append(
                {
                    "column": col,
                    "old_type": existing[col],
                    "new_type": incoming[col]
                }
            )

    return {
        "new_columns": new_columns,
        "removed_columns": removed_columns,
        "type_changes": type_changes
    }