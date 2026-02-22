from __future__ import annotations

from src.agent import retrieval


def test_build_query_text_reflects_user_analysis_and_sql() -> None:
    schema = {
        "columns": ["YEAR", "READMISSION_RATE"],
        "dtypes": {"YEAR": "int64", "READMISSION_RATE": "float64"},
    }
    text = retrieval._build_query_text(
        "재입원율 추세를 보여줘",
        schema,
        sql=(
            "SELECT YEAR, AVG(READMISSION_RATE) AS READMISSION_RATE "
            "FROM ADMISSIONS GROUP BY YEAR ORDER BY YEAR"
        ),
        analysis_query="연도별 변화와 이상치 포인트를 함께 설명해줘",
    )

    assert "User query:\n재입원율 추세를 보여줘" in text
    assert "Analysis focus query:\n연도별 변화와 이상치 포인트를 함께 설명해줘" in text
    assert "SQL query:\nSELECT YEAR, AVG(READMISSION_RATE) AS READMISSION_RATE" in text
    assert "- tables: ['ADMISSIONS']" in text
    assert "- aggregates: ['avg']" in text
    assert "- group_by: ['YEAR']" in text
    assert "- order_by: ['YEAR']" in text


def test_summarize_sql_handles_empty_sql() -> None:
    summary = retrieval._summarize_sql("")

    assert summary["tables"] == []
    assert summary["aggregates"] == []
    assert summary["group_by"] == []
    assert summary["order_by"] == []
    assert summary["has_where"] is False
    assert summary["has_having"] is False
