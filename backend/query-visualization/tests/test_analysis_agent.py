from __future__ import annotations

import pandas as pd

from src.agent.analysis_agent import _is_korean_insight_text, analyze_and_visualize


def _load_df() -> pd.DataFrame:
    return pd.read_csv("tests/fixtures/sample.csv")


def test_analysis_agent_trend() -> None:
    df = _load_df()
    user_query = "icu_admit_month 별 mortality_rate 추세 보여줘"
    sql = "SELECT icu_admit_month, mortality_rate, age, gender FROM sample"

    result = analyze_and_visualize(user_query, sql, df)

    assert result.analyses
    chart_types = {a.chart_spec.chart_type for a in result.analyses}

    assert chart_types.intersection({"line", "bar", "box", "hist"})


def test_analysis_agent_empty_rows_uses_fallback_insight() -> None:
    df = pd.DataFrame(columns=["age", "gender"])
    result = analyze_and_visualize("빈 결과도 처리해줘", "SELECT age, gender FROM sample WHERE 1=0", df)

    assert result.insight
    assert result.total_latency_ms is not None
    assert isinstance(result.stage_latency_ms, dict)


def test_analysis_agent_skips_visualization_without_numeric_columns() -> None:
    df = pd.DataFrame(
        {
            "diagnosis_name": ["Hypertension", "Diabetes", "COPD"],
            "gender_label": ["M", "F", "M"],
        }
    )

    result = analyze_and_visualize(
        "진단별 환자 목록 보여줘",
        "SELECT diagnosis_name, gender_label FROM sample",
        df,
    )

    assert result.analyses == []
    assert any("no_numeric_columns" in reason for reason in result.failure_reasons)
    assert result.fallback_stage == "no_numeric_columns"
    assert result.insight


def test_analysis_agent_skips_visualization_with_single_column(monkeypatch) -> None:
    df = pd.DataFrame({"average_admissions": [2396.8, 2397.0, 2397.2]})
    monkeypatch.setattr(
        "src.agent.analysis_agent._llm_generate_insight",
        lambda *args, **kwargs: "쿼리 결과 기반 요약 테스트",
    )

    result = analyze_and_visualize(
        "평균 입원 건수 보여줘",
        "SELECT average_admissions FROM sample",
        df,
    )

    assert result.analyses == []
    assert any("insufficient_columns" in reason for reason in result.failure_reasons)
    assert result.fallback_stage == "insufficient_columns"
    assert "쿼리 결과 기반 요약 테스트" in str(result.insight)


def test_is_korean_insight_text_detects_language_mix() -> None:
    assert _is_korean_insight_text("사망률이 높은 군에서 재입원율도 높게 관찰됩니다.")
    assert _is_korean_insight_text("사망률은 12.4%이며 MORTALITY_RATE 컬럼 기준으로 증가합니다.")
    assert not _is_korean_insight_text("High mortality rates are associated with palliative care.")
