from app.services.pdf_service import PDFCohortService


def test_first_episode_per_subject_partition_key():
    svc = PDFCohortService()
    intent = {"steps": []}
    policy = {
        "accuracy_mode": True,
        "require_icu": True,
        "episode_selector": "first",
        "episode_unit": "per_subject",
    }
    sql = svc.compile_oracle_sql(intent, population_policy=policy, schema_map={})["cohort_sql"]
    assert "PARTITION BY p.subject_id" in sql
