from app.services.cohort_validate.validator import summarize_validation


def test_validation_summary_failed_if_invariant_failed():
    report = {
        "status": "failed",
        "invariants": [{"name": "require_icu", "passed": False}],
        "anomalies": [],
        "stepwise_counts": [],
        "negative_samples": [],
    }
    summary = summarize_validation(report)
    assert summary["validation_pass"] is False
    assert summary["invariants"]["failed"] == 1
