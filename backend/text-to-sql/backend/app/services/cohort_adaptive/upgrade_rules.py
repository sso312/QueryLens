from __future__ import annotations


def should_upgrade_to_accurate(*, ambiguity_count: int, evidence_coverage: float, risk_score: int) -> bool:
    if ambiguity_count > 2:
        return True
    if evidence_coverage < 0.8:
        return True
    return risk_score >= 4


def should_upgrade_to_strict(
    *,
    ambiguity_count: int,
    evidence_coverage: float,
    validator_failed: bool,
    measurement_required: bool,
    has_icd_shorthand: bool,
) -> bool:
    if validator_failed:
        return True
    if measurement_required:
        return True
    if has_icd_shorthand:
        return True
    if ambiguity_count > 3:
        return True
    return evidence_coverage < 0.7
