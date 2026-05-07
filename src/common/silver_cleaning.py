from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re
from typing import Final, Mapping


_ACRONYM_TITLE_TOKENS: Final[tuple[str, ...]] = ("MD", "ENT", "OB/GYN", "RN", "PA", "DO", "NP")


def _acronym_token_pattern(acronym: str) -> str:
    return rf"(?<![A-Za-z]){re.escape(acronym.title())}(?![A-Za-z])"


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
    if not normalized:
        return None
    titled = normalized.title()
    for acronym in _ACRONYM_TITLE_TOKENS:
        titled = re.sub(_acronym_token_pattern(acronym), acronym, titled)
    return titled


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


def parse_bool_value(value: object) -> bool | None:
    """Parse common boolean label values or return None."""
    normalized = normalize_code_value(value)
    if normalized in {"1", "TRUE", "YES", "Y"}:
        return True
    if normalized in {"0", "FALSE", "NO", "N"}:
        return False
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

    normalized = F.initcap(spark_trim_to_null(column))
    for acronym in _ACRONYM_TITLE_TOKENS:
        normalized = F.regexp_replace(normalized, _acronym_token_pattern(acronym), acronym)
    return normalized


def spark_normalize_severity(column):
    """Return a Spark expression that normalizes severity labels."""
    return spark_normalize_title(column)


def spark_decimal_or_null(column, precision: int, scale: int):
    """Return a Spark expression that casts values to DECIMAL or NULL."""
    from pyspark.sql import functions as F

    normalized = spark_trim_to_null(column)
    decimal_type = f"decimal({precision},{scale})"
    if hasattr(normalized, "try_cast"):
        return normalized.try_cast(decimal_type)

    integer_digits = precision - scale
    if scale == 0:
        numeric_pattern = rf"^[+-]?\d{{1,{precision}}}$"
    elif integer_digits > 0:
        numeric_pattern = (
            rf"^[+-]?((\d{{1,{integer_digits}}}(\.\d{{0,{scale}}})?)|(\.\d{{1,{scale}}}))$"
        )
    else:
        numeric_pattern = rf"^[+-]?((0(\.\d{{0,{scale}}})?)|(\.\d{{1,{scale}}}))$"

    return F.when(
        normalized.rlike(numeric_pattern),
        normalized.cast(decimal_type),
    ).otherwise(F.lit(None).cast(decimal_type))


def spark_date_or_null(column, fmt: str = "yyyy-MM-dd"):
    """Return a Spark expression that parses values into DateType or NULL."""
    from pyspark.sql import functions as F

    normalized = spark_trim_to_null(column)
    try_to_timestamp = getattr(F, "try_to_timestamp", None)
    if try_to_timestamp is not None:
        if fmt:
            return try_to_timestamp(normalized, F.lit(fmt)).cast("date")
        return try_to_timestamp(normalized).cast("date")
    return F.to_date(normalized, fmt)


def spark_bool_or_null(column):
    """Return a Spark expression that parses common boolean label values."""
    from pyspark.sql import functions as F

    normalized = F.upper(spark_trim_to_null(column))
    return (
        F.when(normalized.isin("1", "TRUE", "YES", "Y"), F.lit(True))
        .when(normalized.isin("0", "FALSE", "NO", "N"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


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
    "normalize_title_value",
    "parse_bool_value",
    "parse_date_value",
    "parse_decimal_value",
    "spark_bool_or_null",
    "spark_date_or_null",
    "spark_decimal_or_null",
    "spark_normalize_code",
    "spark_normalize_severity",
    "spark_normalize_title",
    "spark_quality_flags",
    "spark_trim_to_null",
]
