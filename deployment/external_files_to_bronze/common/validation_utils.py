from pyspark.sql.functions import (
    col,
    count,
    length
)


def check_duplicates(df, column_names):
    """
    Checks duplicate business keys.
    Supports single or composite keys.
    """

    if isinstance(column_names, str):
        column_names = [column_names]

    duplicate_count = (
        df.groupBy(*column_names)
          .agg(count("*").alias("count"))
          .filter(col("count") > 1)
          .count()
    )

    return duplicate_count


def check_nulls(df, column_name):
    """
    Checks for NULL values in the given column.
    """

    null_count = (
        df.filter(col(column_name).isNull())
          .count()
    )

    return null_count


def check_column_length(df, column_name, expected_length):
    """
    Checks whether column values match the expected length.
    """

    invalid_count = (
        df.filter(
            length(col(column_name)) != expected_length
        ).count()
    )

    return invalid_count


def check_allowed_values(df, column_name, allowed_values):
    """
    Checks whether column values belong to the allowed list.
    """

    invalid_count = (
        df.filter(
            ~col(column_name).isin(allowed_values)
        ).count()
    )

    return invalid_count


def check_numeric_range(df, column_name, minimum=None, maximum=None):
    """
    Checks numeric values against minimum and/or maximum limits.
    """

    if minimum is not None and maximum is not None:
        return df.filter(
            (col(column_name) < minimum) |
            (col(column_name) > maximum)
        ).count()

    elif minimum is not None:
        return df.filter(
            col(column_name) < minimum
        ).count()

    elif maximum is not None:
        return df.filter(
            col(column_name) > maximum
        ).count()

    return 0

def check_required_when_status(
    df,
    status_column,
    required_column,
    status_values
):
    """
    Checks whether a column is NULL for specific status values.
    """

    invalid_count = (
        df.filter(
            (col(status_column).isin(status_values)) &
            (col(required_column).isNull())
        ).count()
    )

    return invalid_count

def check_timestamp_order(
    df,
    earlier_column,
    later_column,
    status_column=None,
    status_values=None
):
    """
    Checks whether the earlier timestamp occurs after the later timestamp.
    NULL values are ignored.

    Optionally validates only for specific status values.
    """

    condition = (
        col(earlier_column).isNotNull() &
        col(later_column).isNotNull() &
        (col(earlier_column) > col(later_column))
    )

    if status_column and status_values:
        condition = (
            col(status_column).isin(status_values) &
            condition
        )

    invalid_count = (
        df.filter(condition)
          .count()
    )

    return invalid_count


def check_row_count(expected_count, actual_count):
    """
    Compares expected and actual row counts.
    """

    return expected_count == actual_count

def check_numeric_greater_than(df, column_name, value):
    """
    Checks whether numeric values are greater than the given value.
    """

    invalid_count = (
        df.filter(
            col(column_name) <= value
        ).count()
    )

    return invalid_count