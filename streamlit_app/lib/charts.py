from __future__ import annotations

import altair as alt
import pandas as pd


PRIMARY = "#0F5C6E"
ACCENT = "#C9863B"
INK = "#18333A"
SOFT = "#6C7B80"
CORAL = "#A34A37"


def _base_chart(data: pd.DataFrame, title: str) -> alt.Chart:
    return (
        alt.Chart(data, title=alt.TitleParams(title, anchor="start", color=INK, font="Fraunces", fontSize=18))
        .configure_axis(
            labelColor=SOFT,
            titleColor=INK,
            gridColor="#DCCFBC",
            domainColor="#C9B9A1",
            tickColor="#C9B9A1",
        )
        .configure_view(strokeWidth=0)
    )


def line_chart(data: pd.DataFrame, x: str, y: str, title: str) -> alt.Chart:
    return (
        _base_chart(data, title)
        .mark_line(point=alt.OverlayMarkDef(filled=True, color=ACCENT, size=60), color=PRIMARY, strokeWidth=3)
        .encode(x=alt.X(f"{x}:T"), y=alt.Y(f"{y}:Q"))
    )


def bar_chart(data: pd.DataFrame, x: str, y: str, title: str, horizontal: bool = False) -> alt.Chart:
    if horizontal:
        return (
            _base_chart(data, title)
            .mark_bar(color=PRIMARY, cornerRadiusEnd=6)
            .encode(x=alt.X(f"{y}:Q"), y=alt.Y(f"{x}:N", sort="-x"))
        )
    return (
        _base_chart(data, title)
        .mark_bar(color=PRIMARY, cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
        .encode(x=alt.X(f"{x}:N", sort="-y"), y=alt.Y(f"{y}:Q"))
    )


def severity_bar_chart(data: pd.DataFrame, x: str, y: str, color: str, title: str) -> alt.Chart:
    return (
        _base_chart(data, title)
        .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
        .encode(
            x=alt.X(f"{x}:N", sort="-y"),
            y=alt.Y(f"{y}:Q"),
            color=alt.Color(
                f"{color}:N",
                scale=alt.Scale(domain=["High", "Low", "Unknown"], range=[CORAL, PRIMARY, "#A0A9AA"]),
                legend=None,
            ),
        )
    )


__all__ = [
    "bar_chart",
    "line_chart",
    "severity_bar_chart",
]
