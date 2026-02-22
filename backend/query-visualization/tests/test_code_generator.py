from __future__ import annotations

import pandas as pd

from src.agent.code_generator import generate_chart


def test_generate_chart_bar_with_agg() -> None:
    df = pd.DataFrame(
        {
            "year": [2020, 2020, 2021, 2021],
            "rate": [0.1, 0.3, 0.2, 0.4],
        }
    )
    chart_spec = {"chart_type": "bar", "x": "year", "y": "rate", "agg": "avg"}

    result = generate_chart(chart_spec, df)

    assert result["figure_json"] is not None
    y_values = result["figure_json"]["data"][0]["y"]
    assert len(y_values) == 2


def test_generate_chart_bar_with_secondary_group_pattern() -> None:
    df = pd.DataFrame(
        {
            "age_group": ["18-39", "18-39", "40-64", "40-64"],
            "gender": ["M", "F", "M", "F"],
            "survival_status": ["alive", "dead", "alive", "dead"],
            "cnt": [12, 4, 9, 6],
        }
    )
    chart_spec = {
        "chart_type": "bar_grouped",
        "x": "age_group",
        "y": "cnt",
        "group": "gender",
        "secondary_group": "survival_status",
        "bar_mode": "group",
        "agg": "sum",
    }

    result = generate_chart(chart_spec, df)

    assert result["figure_json"] is not None
    traces = result["figure_json"].get("data", [])
    assert traces
    has_pattern = any(
        isinstance(trace, dict)
        and isinstance(trace.get("marker"), dict)
        and isinstance(trace.get("marker", {}).get("pattern"), dict)
        for trace in traces
    )
    assert has_pattern


def test_generate_chart_bar_with_categorical_y_falls_back_to_count() -> None:
    df = pd.DataFrame(
        {
            "gender": ["M", "F", "M", "F", "M"],
            "age_group": ["18-39", "18-39", "40-64", "65+", "65+"],
        }
    )
    chart_spec = {"chart_type": "bar_basic", "x": "gender", "y": "age_group"}

    result = generate_chart(chart_spec, df)

    assert result["figure_json"] is not None
    layout = result["figure_json"].get("layout", {})
    y_title = (
        (layout.get("yaxis") or {}).get("title") or {}
        if isinstance(layout, dict)
        else {}
    )
    assert y_title.get("text") == "count"


def test_generate_chart_bar_ignores_high_cardinality_numeric_color_group() -> None:
    df = pd.DataFrame(
        {
            "gender": ["M", "F", "M", "F", "M", "F"],
            "cnt": [10, 12, 8, 9, 11, 7],
            "mortality_count": [101, 202, 303, 404, 505, 606],
        }
    )
    chart_spec = {
        "chart_type": "bar_grouped",
        "x": "gender",
        "y": "cnt",
        "group": "mortality_count",
        "agg": "sum",
    }

    result = generate_chart(chart_spec, df)

    assert result["figure_json"] is not None
    layout = result["figure_json"].get("layout", {})
    assert "coloraxis" not in layout


def test_generate_chart_bar_with_discrete_numeric_y_falls_back_to_count() -> None:
    df = pd.DataFrame(
        {
            "gender": ["M", "F", "M", "F", "M", "F", "M", "F"],
            "age_group_code": [0, 0, 1, 1, 2, 2, 3, 3],
        }
    )
    chart_spec = {"chart_type": "bar_basic", "x": "gender", "y": "age_group_code"}

    result = generate_chart(chart_spec, df)

    assert result["figure_json"] is not None
    layout = result["figure_json"].get("layout", {})
    y_title = (
        (layout.get("yaxis") or {}).get("title") or {}
        if isinstance(layout, dict)
        else {}
    )
    assert y_title.get("text") == "count"


def test_generate_chart_pyramid_has_visible_marker_colors() -> None:
    df = pd.DataFrame(
        {
            "age_group": ["0-17", "0-17", "18-39", "18-39", "40-64", "40-64"],
            "gender": ["F", "M", "F", "M", "F", "M"],
            "cnt": [8, 6, 14, 12, 11, 9],
        }
    )
    chart_spec = {
        "chart_type": "pyramid",
        "x": "age_group",
        "y": "cnt",
        "group": "gender",
        "agg": "sum",
    }

    result = generate_chart(chart_spec, df)

    assert result["figure_json"] is not None
    traces = result["figure_json"].get("data", [])
    assert len(traces) == 2
    colors = [
        ((trace.get("marker") or {}).get("color") if isinstance(trace, dict) else None)
        for trace in traces
    ]
    assert all(color for color in colors)
    assert all(str(color).strip().lower() not in {"white", "#fff", "#ffffff"} for color in colors)
