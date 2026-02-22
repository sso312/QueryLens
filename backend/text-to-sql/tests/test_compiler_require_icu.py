from app.services.pdf_service import PDFCohortService


def test_accuracy_mode_require_icu_uses_inner_join():
    svc = PDFCohortService()
    intent = {"steps": []}
    policy = {
        "accuracy_mode": True,
        "require_icu": True,
        "episode_selector": "first",
        "episode_unit": "per_subject",
    }
    sql = svc.compile_oracle_sql(intent, population_policy=policy, schema_map={})["cohort_sql"]
    assert "LEFT JOIN" not in sql.upper()
