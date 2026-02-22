from app.services.pdf_service import PDFCohortService


def test_measurement_required_compiles_to_meas_ok_cte():
    svc = PDFCohortService()
    intent = {
        "steps": [
            {
                "name": "req_measurements",
                "type": "measurement_required",
                "params": {"signals": ["HR", "BP_SYS"]},
                "window": "icu_discharge_last_24h",
            }
        ]
    }
    policy = {
        "accuracy_mode": True,
        "require_icu": True,
        "episode_selector": "first",
        "episode_unit": "per_subject",
    }
    schema_map = {"signal_map": {"HR": {"itemids": [220045]}, "BP_SYS": {"itemids": [220179]}}}
    sql = svc.compile_oracle_sql(intent, population_policy=policy, schema_map=schema_map)["cohort_sql"]
    assert "_meas_ok AS" in sql
    assert "GROUP BY c.stay_id" in sql
