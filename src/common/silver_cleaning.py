from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Mapping


def normalize_nullable_string(value: object) -> str | None:
    """Trim a scalar value and return None for blank strings."""
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def normalize_code_value(value: object) -> str | None:
    """Return an uppercase trimmed code value or None."""
    normalized = normalize_nullable_string(value)
    return normalized.upper() if normalized else None


def normalize_title_value(value: object) -> str | None:
    """Return a title-cased trimmed string or None."""
    normalized = normalize_nullable_string(value)
    return normalized.title() if normalized else None


def normalize_severity_value(value: object) -> str | None:
    """Normalize severity labels to High/Low style casing."""
    normalized = normalize_title_value(value)
    return normalized if normalized in {"High", "Low"} else normalized


def parse_decimal_value(value: object, scale: str = "0.01") -> Decimal | None:
    """Parse a scalar into a quantized Decimal or None."""
    normalized = normalize_nullable_string(value)
    if normalized is None:
        return None
    try:
        decimal_value = Decimal(normalized)
    except (InvalidOperation, TypeError):
        return None
    return decimal_value.quantize(Decimal(scale), rounding=ROUND_HALF_UP)


def parse_date_value(value: object) -> date | None:
    """Parse an ISO yyyy-mm-dd date string or return None."""
    normalized = normalize_nullable_string(value)
    if normalized is None:
        return None
    try:
        return date.fromisoformat(normalized)
    except ValueError:
        return None


def build_quality_flags(flag_map: Mapping[str, bool]) -> list[str]:
    """Return stable quality-flag names for all truthy entries."""
    return [flag_name for flag_name, enabled in sorted(flag_map.items()) if enabled]


def spark_trim_to_null(column):
    """Return a Spark expression that trims text and converts blanks to NULL."""
    from pyspark.sql import functions as F

    trimmed = F.trim(column.cast("string"))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def spark_normalize_code(column):
    """Return a Spark expression that canonicalizes code-like strings."""
    from pyspark.sql import functions as F

    return F.upper(spark_trim_to_null(column))


def spark_normalize_title(column):
    """Return a Spark expression that title-cases free-text labels."""
    from pyspark.sql import functions as F

    return F.initcap(spark_trim_to_null(column))


def spark_normalize_severity(column):
    """Return a Spark expression that normalizes severity labels."""
    return spark_normalize_title(column)


def spark_decimal_or_null(column, precision: int, scale: int):
    """Return a Spark expression that casts values to DECIMAL or NULL."""
    return spark_trim_to_null(column).cast(f"decimal({precision},{scale})")


def spark_date_or_null(column, fmt: str = "yyyy-MM-dd"):
    """Return a Spark expression that parses values into DateType or NULL."""
    from pyspark.sql import functions as F

    return F.to_date(spark_trim_to_null(column), fmt)


def spark_quality_flags(flag_expressions: Mapping[str, object]):
    """Return a Spark array<string> with all active quality flags."""
    from pyspark.sql import functions as F

    if not flag_expressions:
        return F.array().cast("array<string>")

    flags = F.array(
        *[
            F.when(expression, F.lit(flag_name)).otherwise(F.lit(None).cast("string"))
            for flag_name, expression in sorted(flag_expressions.items())
        ]
    ).cast("array<string>")
    return F.filter(
        flags,
        lambda flag: flag.isNotNull(),
    )


__all__ = [
    "build_quality_flags",
    "normalize_code_value",
    "normalize_nullable_string",
    "normalize_severity_value",
    "normalize_title_value",
    "parse_date_value",
    "parse_decimal_value",
    "spark_date_or_null",
    "spark_decimal_or_null",
    "spark_normalize_code",
    "spark_normalize_severity",
    "spark_normalize_title",
    "spark_quality_flags",
    "spark_trim_to_null",
]
