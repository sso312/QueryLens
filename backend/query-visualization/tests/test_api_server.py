from __future__ import annotations

from fastapi.testclient import TestClient

from src.api.server import MAX_ROWS, app


client = TestClient(app)


def test_visualize_rejects_large_rows() -> None:
    payload = {
        "user_query": "월별 추세",
        "sql": "SELECT * FROM sample",
        "rows": [{"x": i} for i in range(MAX_ROWS + 1)],
    }
    response = client.post("/visualize", json=payload)
    assert response.status_code == 413
    body = response.json()
    assert body["detail"]["code"] == "ROWS_LIMIT_EXCEEDED"


def test_visualize_ignores_client_analysis_query(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_analyze_and_visualize(
        user_query: str,
        sql: str,
        df,
        *,
        analysis_query=None,
        request_id=None,
    ):
        captured["analysis_query"] = analysis_query
        return {
            "sql": sql,
            "table_preview": [],
            "analyses": [],
            "insight": "ok",
            "fallback_used": False,
            "failure_reasons": [],
            "attempt_count": 1,
            "request_id": request_id,
            "stage_latency_ms": {},
        }

    monkeypatch.setattr(
        "src.agent.analysis_agent.analyze_and_visualize",
        _fake_analyze_and_visualize,
    )

    payload = {
        "user_query": "연령별 사망 생존을 성별분포로 막대그래프",
        "analysis_query": "assistant 답변 컨텍스트",
        "sql": "SELECT gender, age_group, mortality_count FROM sample",
        "rows": [{"gender": "M", "age_group": "65+", "mortality_count": 10}],
    }
    response = client.post("/visualize", json=payload)

    assert response.status_code == 200
    assert captured.get("analysis_query", "sentinel") is None
