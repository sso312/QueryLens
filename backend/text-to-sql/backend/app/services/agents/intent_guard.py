from __future__ import annotations

from typing import Any
import re

from app.services.agents.sql_postprocess import postprocess_sql

_INTENT_RATIO_RE = re.compile(
    r"(비율|비중|율|퍼센트|백분율|ratio|rate|proportion|percentage|pct)",
    re.IGNORECASE,
)
_INTENT_STRATIFY_RE = re.compile(
    r"(연도별|월별|주별|일별|분기별|별로|에\s*따른|비교|대비|vs|versus|according\s+to|stratif|subgroup|"
    r"\bby\s+(year|month|week|day|gender|sex|age|group|quartile|q[1-4]|service|department|diagnosis|procedure)\b)",
    re.IGNORECASE,
)
_INTENT_YEARLY_RE = re.compile(r"(연도별|년\s*별|yearly|by\s+year|annual)", re.IGNORECASE)
_INTENT_MONTHLY_RE = re.compile(r"(월별|monthly|by\s+month)", re.IGNORECASE)
_INTENT_QUARTILE_RE = re.compile(r"(사분위|quartile|q1\s*[-–~]\s*q4|q1|q2|q3|q4)", re.IGNORECASE)
_WINDOW_INTENT_RE = re.compile(
    r"(\d+\s*(일|주|개월|월|년)\s*(이내|후)|within\s+\d+\s*(day|week|month|year)|after\s+\d+\s*(day|week|month|year))",
    re.IGNORECASE,
)
_YEAR_BUCKET_RE = re.compile(
    r"EXTRACT\s*\(\s*YEAR\s+FROM|TO_CHAR\s*\([^)]*,\s*'YYYY'\s*\)",
    re.IGNORECASE,
)
_MONTH_BUCKET_RE = re.compile(
    r"TO_CHAR\s*\([^)]*,\s*'YYYY[-/]MM'\s*\)|TO_CHAR\s*\([^)]*,\s*'MM'\s*\)|EXTRACT\s*\(\s*MONTH\s+FROM",
    re.IGNORECASE,
)
_WINDOW_SQL_RE = re.compile(r"(INTERVAL|ADD_MONTHS|\+\s*\d+\s*(/|$)|BETWEEN\s+.+\s+AND\s+.+)", re.IGNORECASE)
_AGE_SEMANTIC_INTENT_RE = re.compile(
    r"(연령대|나이대|연령|나이|나잇대|세\b|aged?\b|age\s*(group|band|range)?\b)",
    re.IGNORECASE,
)
_YEAR_SEMANTIC_INTENT_RE = re.compile(
    r"(연도|년도|연도별|년별|year|yearly|annual|anchor[_\s]*year|anchor[_\s]*year[_\s]*group)",
    re.IGNORECASE,
)
_AGE_GROUP_INTENT_RE = re.compile(r"(연령대|나이대|age\s*(group|band|range)|연령\s*구간)", re.IGNORECASE)
_GENDER_INTENT_RE = re.compile(r"(성별|남성|여성|\bgender\b|\bsex\b)", re.IGNORECASE)
_EXTREMA_INTENT_RE = re.compile(
    r"(가장\s*(많|적)|최대|최소|최고|최저|most|least|highest|lowest|max|min|top\s*1|bottom\s*1)",
    re.IGNORECASE,
)
_SERVICE_INTENT_RE = re.compile(
    r"(진료과|서비스|과별|부서|department|service|curr_service|prev_service)",
    re.IGNORECASE,
)
_ADMISSION_TYPE_INTENT_RE = re.compile(
    r"(입원\s*유형|입원유형|admission\s*type|admission_type|emergency|urgent|elective)",
    re.IGNORECASE,
)
_DIAGNOSIS_PROCEDURE_INTENT_RE = re.compile(
    r"(진단|질환|병명|코드|icd|diagnos|procedure|시술|수술)",
    re.IGNORECASE,
)
_MORTALITY_INTENT_RE = re.compile(r"(사망|mortality|death|deceased|expire)", re.IGNORECASE)
_ICU_INTENT_RE = re.compile(r"(중환자실|\bicu\b)", re.IGNORECASE)
_FIRST_ICU_INTENT_RE = re.compile(
    r"(first\s+icu|first[-\s]*stay|initial\s+icu|index\s+icu|"
    r"첫\s*icu|첫번째\s*icu|최초\s*icu|처음\s*icu|첫\s*중환자실|최초\s*중환자실|처음\s*중환자실)",
    re.IGNORECASE,
)


def _intent_text_from_planner(planner_intent: dict[str, Any] | None) -> str:
    if not isinstance(planner_intent, dict) or not planner_intent:
        return ""
    parts: list[str] = []
    for key in ("cohort", "metric", "time", "grain", "comparison", "output_shape", "intent_summary"):
        value = planner_intent.get(key)
        if isinstance(value, str):
            text = value.strip()
            if text:
                parts.append(text)
        elif isinstance(value, list):
            for item in value:
                item_text = str(item).strip()
                if item_text:
                    parts.append(item_text)
    filters = planner_intent.get("filters")
    if isinstance(filters, list):
        for item in filters:
            text = str(item).strip()
            if text:
                parts.append(text)
    return " ".join(parts)


def _prefer_anchor_age_semantics(intent_text: str) -> bool:
    text = str(intent_text or "").strip()
    if not text:
        return False
    has_age_intent = bool(_AGE_SEMANTIC_INTENT_RE.search(text))
    has_year_intent = bool(_YEAR_SEMANTIC_INTENT_RE.search(text))
    return has_age_intent and not has_year_intent


def _is_ident_char(ch: str) -> bool:
    return ch.isalnum() or ch in {"_", "$", "#"}


def _token_at(text_upper: str, idx: int, token: str) -> bool:
    token_len = len(token)
    if text_upper[idx: idx + token_len] != token:
        return False
    prev = text_upper[idx - 1] if idx > 0 else " "
    nxt = text_upper[idx + token_len] if idx + token_len < len(text_upper) else " "
    if _is_ident_char(prev) or _is_ident_char(nxt):
        return False
    return True


def _find_final_select_clause(sql: str) -> str:
    core = str(sql or "").strip().rstrip(";")
    if not core:
        return ""

    upper = core.upper()
    depth = 0
    in_single = False
    last_select = -1
    i = 0
    while i < len(upper):
        ch = upper[i]
        if in_single:
            if ch == "'":
                if i + 1 < len(upper) and upper[i + 1] == "'":
                    i += 2
                    continue
                in_single = False
            i += 1
            continue
        if ch == "'":
            in_single = True
            i += 1
            continue
        if ch == "(":
            depth += 1
            i += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            i += 1
            continue
        if depth == 0 and _token_at(upper, i, "SELECT"):
            last_select = i
            i += len("SELECT")
            continue
        i += 1

    if last_select < 0:
        return ""

    depth = 0
    in_single = False
    i = last_select + len("SELECT")
    from_idx = -1
    while i < len(upper):
        ch = upper[i]
        if in_single:
            if ch == "'":
                if i + 1 < len(upper) and upper[i + 1] == "'":
                    i += 2
                    continue
                in_single = False
            i += 1
            continue
        if ch == "'":
            in_single = True
            i += 1
            continue
        if ch == "(":
            depth += 1
            i += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            i += 1
            continue
        if depth == 0 and _token_at(upper, i, "FROM"):
            from_idx = i
            break
        i += 1

    if from_idx < 0:
        return ""
    return core[last_select + len("SELECT"):from_idx].strip()


def detect_intent_alignment_issues(
    question: str,
    sql: str,
    *,
    planner_intent: dict[str, Any] | None = None,
) -> list[str]:
    q = str(question or "").strip()
    text = str(sql or "").strip()
    if not q or not text:
        return []

    upper = text.upper()
    issues: list[str] = []
    planner_text = _intent_text_from_planner(planner_intent)
    intent_text = " ".join(part for part in [q, planner_text] if part).strip()

    ratio_intent = bool(_INTENT_RATIO_RE.search(intent_text))
    quartile_intent = bool(_INTENT_QUARTILE_RE.search(intent_text))
    stratify_intent = bool(_INTENT_STRATIFY_RE.search(intent_text))
    yearly_intent = bool(_INTENT_YEARLY_RE.search(intent_text))
    monthly_intent = bool(_INTENT_MONTHLY_RE.search(intent_text))
    window_intent = bool(_WINDOW_INTENT_RE.search(intent_text))
    age_semantic_intent = _prefer_anchor_age_semantics(intent_text)
    service_intent = bool(_SERVICE_INTENT_RE.search(intent_text))
    admission_type_intent = bool(_ADMISSION_TYPE_INTENT_RE.search(intent_text))
    diagnosis_or_procedure_intent = bool(_DIAGNOSIS_PROCEDURE_INTENT_RE.search(intent_text))
    mortality_intent = bool(_MORTALITY_INTENT_RE.search(intent_text))
    icu_intent = bool(_ICU_INTENT_RE.search(intent_text))
    first_icu_intent = bool(_FIRST_ICU_INTENT_RE.search(intent_text))
    age_gender_extrema_intent = bool(
        _AGE_GROUP_INTENT_RE.search(intent_text)
        and _GENDER_INTENT_RE.search(intent_text)
        and _EXTREMA_INTENT_RE.search(intent_text)
    )

    if ratio_intent:
        has_ratio_expr = (
            "/" in upper
            or bool(re.search(r"\bAVG\s*\(", upper, re.IGNORECASE))
            or bool(re.search(r"\bRATE\b|\bRATIO\b|\bPCT\b", upper, re.IGNORECASE))
        )
        if not has_ratio_expr:
            issues.append("ratio_intent_without_ratio_expression")

    if quartile_intent:
        has_quartile_logic = "NTILE(" in upper or bool(re.search(r"\bQ[1-4]\b", upper, re.IGNORECASE))
        if not has_quartile_logic:
            issues.append("quartile_intent_without_ntile")

    if stratify_intent:
        has_grouping = "GROUP BY" in upper or "PARTITION BY" in upper
        if not has_grouping:
            issues.append("stratified_intent_without_grouping")

    if yearly_intent and not _YEAR_BUCKET_RE.search(text):
        issues.append("yearly_intent_without_year_bucket")

    if monthly_intent and not _MONTH_BUCKET_RE.search(text):
        issues.append("monthly_intent_without_month_bucket")

    if window_intent and not _WINDOW_SQL_RE.search(text):
        issues.append("window_intent_without_time_window_predicate")

    if age_semantic_intent:
        has_anchor_year_group = bool(re.search(r"\bANCHOR_YEAR_GROUP\b", upper, re.IGNORECASE))
        has_anchor_age = bool(re.search(r"\bANCHOR_AGE\b", upper, re.IGNORECASE))
        if has_anchor_year_group and not has_anchor_age:
            issues.append("age_intent_mapped_to_anchor_year_group")

    if age_gender_extrema_intent:
        final_select = _find_final_select_clause(text)
        has_age_projection = bool(
            re.search(r"\bAGE_GROUP\b|\bANCHOR_AGE\b", final_select, re.IGNORECASE)
            or re.search(r"\bCASE\b[\s\S]{0,300}\bANCHOR_AGE\b", final_select, re.IGNORECASE)
        )
        if not has_age_projection:
            issues.append("age_group_extrema_missing_age_projection")

    if service_intent and not admission_type_intent and not diagnosis_or_procedure_intent:
        has_services_ref = bool(re.search(r"\b(SERVICES|CURR_SERVICE|PREV_SERVICE)\b", upper, re.IGNORECASE))
        if not has_services_ref:
            issues.append("service_intent_without_services_reference")
        has_diag_proc_ref = bool(re.search(r"\b(DIAGNOSES_ICD|PROCEDURES_ICD)\b", upper, re.IGNORECASE))
        if has_diag_proc_ref and not has_services_ref:
            issues.append("service_intent_mapped_to_diagnosis_or_procedure")

    if admission_type_intent:
        has_admission_type_ref = bool(re.search(r"\bADMISSION_TYPE\b", upper, re.IGNORECASE))
        if not has_admission_type_ref:
            issues.append("admission_type_intent_without_admission_type_reference")

    if icu_intent and mortality_intent:
        has_icustays_ref = bool(re.search(r"\bICUSTAYS\b", upper, re.IGNORECASE))
        if not has_icustays_ref:
            issues.append("icu_mortality_intent_without_icustays_reference")
        has_hospital_expire_ref = bool(re.search(r"\bHOSPITAL_EXPIRE_FLAG\b", upper, re.IGNORECASE))
        has_death_alignment = bool(
            re.search(r"\bDEATHTIME\b", upper, re.IGNORECASE)
            and re.search(r"\bINTIME\b", upper, re.IGNORECASE)
            and re.search(r"\bOUTTIME\b", upper, re.IGNORECASE)
        )
        if has_hospital_expire_ref and not has_death_alignment:
            issues.append("icu_mortality_mapped_to_hospital_expire_flag_only")

    if icu_intent and not first_icu_intent:
        has_first_icu_window = bool(
            re.search(
                r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(\s*PARTITION\s+BY\s+[A-Za-z0-9_\.]*SUBJECT_ID\s+ORDER\s+BY\s+[A-Za-z0-9_\.]*INTIME",
                upper,
                re.IGNORECASE,
            )
        )
        has_first_icu_ref = bool(re.search(r"\b(FIRST_ICU|RN_FIRST_ICU)\b", upper, re.IGNORECASE))
        has_first_row_filter = bool(re.search(r"\bRN_FIRST_ICU\s*=\s*1\b", upper, re.IGNORECASE))
        has_generic_rn_filter = bool(re.search(r"\bRN\s*=\s*1\b", upper, re.IGNORECASE))
        if has_first_icu_window or has_first_icu_ref or has_first_row_filter or (has_generic_rn_filter and has_first_icu_window):
            issues.append("first_icu_forced_without_intent")

    return issues


def enforce_intent_alignment(
    question: str,
    sql: str,
    *,
    planner_intent: dict[str, Any] | None = None,
) -> tuple[str, list[str], list[str]]:
    before = detect_intent_alignment_issues(question, sql, planner_intent=planner_intent)
    if not before:
        return sql, [], []

    rewritten, rules = postprocess_sql(question, sql, profile="auto")
    after = detect_intent_alignment_issues(question, rewritten, planner_intent=planner_intent)
    before_set = set(before)
    after_set = set(after)
    improved = len(after) < len(before)
    no_regression = after_set.issubset(before_set)
    if rewritten.strip() != str(sql or "").strip() and improved and no_regression:
        return rewritten, rules, after
    return sql, [], before
