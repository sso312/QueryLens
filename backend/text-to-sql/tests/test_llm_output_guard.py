from app.services.cohort_sql_compiler.anti_patterns import find_anti_patterns


def test_chartevents_correlated_exists_detected():
    sql = """
    SELECT * FROM cohort p
    WHERE EXISTS (
      SELECT 1 FROM SSO.CHARTEVENTS m
      WHERE m.stay_id = p.stay_id
    )
    """
    found = find_anti_patterns(sql)
    codes = {str(v.get("code") or "") for v in found}
    assert "chartevents_correlated_exists" in codes
