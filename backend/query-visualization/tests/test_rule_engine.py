from __future__ import annotations

import pandas as pd

from src.agent.chart_rule_engine import _infer_context_flags, plan_analyses


def _load_df() -> pd.DataFrame:
    return pd.read_csv("tests/fixtures/sample.csv")


def test_plan_analyses_trend() -> None:
    df = _load_df()
    intent_info = {
        "analysis_intent": "trend",
        "primary_outcome": "mortality_rate",
        "time_var": "icu_admit_month",
        "group_var": "gender",
    }

    plans = plan_analyses(intent_info, df)
    chart_types = {p["chart_spec"]["chart_type"] for p in plans}

    assert "line" in chart_types
    assert "box" in chart_types


def test_plan_analyses_distribution() -> None:
    df = _load_df()
    intent_info = {
        "analysis_intent": "distribution",
        "primary_outcome": "age",
        "time_var": None,
        "group_var": "gender",
    }

    plans = plan_analyses(intent_info, df)
    chart_types = {p["chart_spec"]["chart_type"] for p in plans}

    assert "hist" in chart_types
    assert "box" in chart_types


def test_plan_analyses_proportion() -> None:
    df = pd.DataFrame(
        {
            "admit_year": [2020, 2021, 2022],
            "readmit_30d_rate": [0.10, 0.12, 0.11],
        }
    )
    intent_info = {
        "analysis_intent": "proportion",
        "primary_outcome": "readmit_30d_rate",
        "time_var": "admit_year",
        "group_var": None,
    }

    plans = plan_analyses(intent_info, df)
    chart_types = {p["chart_spec"]["chart_type"] for p in plans}

    assert "line" in chart_types


def test_infer_context_flags_requires_numeric_post_days_pattern() -> None:
    flags = _infer_context_flags("퇴원 후 상태를 보여줘", ["admittime", "dischtime"])
    assert flags["post_days"] is False

    flags_with_days = _infer_context_flags("퇴원 후 30일 재입원율 추세", ["admittime", "dischtime"])
    assert flags_with_days["post_days"] is True


def test_plan_analyses_blocks_identifier_group_for_distribution() -> None:
    df = pd.DataFrame(
        {
            "subject_id": [1, 2, 3, 4],
            "age": [65, 72, 58, 60],
        }
    )
    intent_info = {
        "analysis_intent": "distribution",
        "primary_outcome": "age",
        "time_var": None,
        "group_var": "subject_id",
    }
    plans = plan_analyses(intent_info, df)
    for plan in plans:
        spec = plan.get("chart_spec", {})
        assert spec.get("group") != "subject_id"


def test_plan_analyses_skips_bar_when_y_is_constant() -> None:
    df = pd.DataFrame(
        {
            "gender": ["M", "F", "M", "F"],
            "cnt": [1, 1, 1, 1],
        }
    )
    intent_info = {
        "analysis_intent": "comparison",
        "primary_outcome": "cnt",
        "time_var": None,
        "group_var": "gender",
    }

    plans = plan_analyses(intent_info, df)
    chart_types = {str(p.get("chart_spec", {}).get("chart_type", "")).lower() for p in plans}

    assert not any(chart_type.startswith("bar") for chart_type in chart_types)
    assert "box" in chart_types


def test_plan_analyses_keeps_bar_when_y_is_not_constant() -> None:
    df = pd.DataFrame(
        {
            "gender": ["M", "F", "M", "F"],
            "cnt": [1, 2, 1, 3],
        }
    )
    intent_info = {
        "analysis_intent": "comparison",
        "primary_outcome": "cnt",
        "time_var": None,
        "group_var": "gender",
    }

    plans = plan_analyses(intent_info, df)
    chart_types = {str(p.get("chart_spec", {}).get("chart_type", "")).lower() for p in plans}

    assert any(chart_type.startswith("bar") for chart_type in chart_types)


def test_plan_analyses_complex_multisplit_bar_slots() -> None:
    df = pd.DataFrame(
        {
            "age_group": ["18-39", "18-39", "40-64", "40-64"],
            "gender": ["M", "F", "M", "F"],
            "survival_status": ["alive", "dead", "alive", "dead"],
            "cnt": [12, 4, 9, 6],
        }
    )
    intent_info = {
        "analysis_intent": "comparison",
        "primary_outcome": "cnt",
        "time_var": None,
        "group_var": "gender",
        "user_query": "연령별 사망 생존을 성별분포를 나눠서 막대그래프로 보여줘",
    }

    plans = plan_analyses(intent_info, df)
    has_multisplit_bar = any(
        str((plan.get("chart_spec") or {}).get("chart_type", "")).startswith("bar")
        and (plan.get("chart_spec") or {}).get("x") == "age_group"
        and (plan.get("chart_spec") or {}).get("group") == "gender"
        and (plan.get("chart_spec") or {}).get("secondary_group") == "survival_status"
        for plan in plans
    )
    assert has_multisplit_bar
