"""Lint-time stand-ins for dbt cross-database macros (rendered as Trino SQL for sqlfluff)."""


def datediff(first_date, second_date, datepart):
    return f"date_diff('{datepart}', {first_date}, {second_date})"


def concat(fields):
    return f"concat({', '.join(fields)})"
