from app.api.routes import cohort


class _StubSettings:
    openai_api_key = "test-key"
    expert_model = "gpt-4o-mini"
    llm_max_output_tokens_expert = 400
    llm_max_output_tokens = 300


def _sample_confidence() -> dict:
    return {
        "metrics": [
            {
                "metric": "readmission_rate",
                "label": "재입원율",
                "difference": -1.23,
                "ci": [-2.0, -0.4],
                "p_value": 0.01,
                "effect_size": 0.2,
                "effect_size_type": "cohen_h",
                "significant": True,
            }
        ]
    }


def _sample_subgroups() -> dict:
    return {
        "age": [
            {
                "key": "age_65_74",
                "label": "65-74세",
                "delta": {
                    "readmission_rate": -0.5,
                    "mortality_rate": -0.2,
                    "avg_los_days": -0.1,
                },
            }
        ],
        "gender": [],
        "comorbidity": [],
    }


def _sample_survival() -> list[dict]:
    return [
        {"time": 0, "current": 100.0, "simulated": 100.0},
        {"time": 30, "current": 76.2, "simulated": 78.0},
        {"time": 90, "current": 49.0, "simulated": 51.2},
        {"time": 180, "current": 28.8, "simulated": 31.0},
    ]


def _sample_metrics(current_patient_count: int = 1200, simulated_patient_count: int = 980) -> tuple[dict, dict]:
    current = {
        "patient_count": current_patient_count,
        "readmission_rate": 12.4,
        "readmission_7d_rate": 5.1,
        "mortality_rate": 8.2,
        "avg_los_days": 9.8,
        "long_stay_rate": 22.0,
        "icu_admission_rate": 31.5,
        "er_admission_rate": 64.0,
    }
    simulated = {
        "patient_count": simulated_patient_count,
        "readmission_rate": 11.1,
        "readmission_7d_rate": 4.7,
        "mortality_rate": 7.8,
        "avg_los_days": 9.1,
        "long_stay_rate": 20.2,
        "icu_admission_rate": 29.8,
        "er_admission_rate": 61.4,
    }
    return current, simulated


def test_generate_simulation_insight_uses_llm(monkeypatch):
    class _StubLLMClient:
        def chat(self, **kwargs):
            return {"content": '{"insight":"시뮬레이션 조건에서 재입원율과 사망률이 모두 개선되었습니다."}'}

    current_metrics, simulated_metrics = _sample_metrics()
    monkeypatch.setattr(cohort, "get_settings", lambda: _StubSettings())
    monkeypatch.setattr(cohort, "LLMClient", _StubLLMClient)

    insight, source = cohort._generate_simulation_insight(
        baseline_params=cohort.CohortParams(),
        simulated_params=cohort.CohortParams(age_threshold=70),
        current_metrics=current_metrics,
        simulated_metrics=simulated_metrics,
        confidence=_sample_confidence(),
        subgroups=_sample_subgroups(),
        survival=_sample_survival(),
    )

    assert source == "llm"
    assert "개선" in insight


def test_generate_simulation_insight_fallback_on_llm_failure(monkeypatch):
    class _StubLLMClient:
        def chat(self, **kwargs):
            raise RuntimeError("LLM unavailable")

    current_metrics, simulated_metrics = _sample_metrics()
    monkeypatch.setattr(cohort, "get_settings", lambda: _StubSettings())
    monkeypatch.setattr(cohort, "LLMClient", _StubLLMClient)

    insight, source = cohort._generate_simulation_insight(
        baseline_params=cohort.CohortParams(),
        simulated_params=cohort.CohortParams(age_threshold=75),
        current_metrics=current_metrics,
        simulated_metrics=simulated_metrics,
        confidence=_sample_confidence(),
        subgroups=_sample_subgroups(),
        survival=_sample_survival(),
    )

    assert source == "fallback"
    assert "분모 변화와 선택 편향" in insight
