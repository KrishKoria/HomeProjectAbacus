from __future__ import annotations

import math

import pandas as pd


def format_currency(value: float | int | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "N/A"
    return f"${value:,.0f}"


def format_integer(value: float | int | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "0"
    return f"{int(value):,}"


def format_ratio(value: float | int | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "N/A"
    return f"{value:.2f}x"


def format_timestamp(value) -> str:
    if value is None or value is pd.NaT:
        return "Unavailable"
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.tz_convert("Asia/Calcutta").strftime("%b %d, %Y %I:%M %p IST")


__all__ = [
    "format_currency",
    "format_integer",
    "format_ratio",
    "format_timestamp",
]
