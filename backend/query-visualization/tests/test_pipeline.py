from __future__ import annotations

import pandas as pd

from src.agent.analysis_agent import analyze_and_visualize


def test_pipeline_intent_plan_chart() -> None:
    # 샘플 데이터
    df = pd.DataFrame(
        {
            "icu_admit_month": ["2024-01", "2024-02", "2024-03"],
            "mortality_rate": [0.12, 0.10, 0.15],
            "gender": ["M", "F", "M"],
        }
    )

    user_query = "icu_admit_month 별 mortality_rate 추세 보여줘"
    sql = "SELECT icu_admit_month, mortality_rate, gender FROM sample"

    result = analyze_and_visualize(user_query, sql, df)

    # intent -> plan -> chart 생성 결과가 비어있지 않아야 함
    assert result.analyses

    # 최소 1개는 figure_json이 있어야 함
    assert any(a.figure_json for a in result.analyses)
