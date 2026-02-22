from __future__ import annotations

from typing import Any, Iterable
import json
import re

from app.core.config import get_settings
from app.services.runtime.column_value_store import load_column_value_rows
from app.services.runtime.diagnosis_map_store import match_diagnosis_mappings
from app.services.runtime.label_intent_store import load_label_intent_profiles, match_label_intent_profiles
from app.services.runtime.procedure_map_store import match_procedure_mappings
from app.services.runtime.sql_error_repair_store import find_learned_sql_fix, mark_learned_sql_fix_used
from app.services.runtime.sql_postprocess_rules_store import load_sql_postprocess_rules
from app.services.runtime.sql_schema_hints_store import load_sql_schema_hints

_COUNT_RE = re.compile(r"^Count rows in ([A-Za-z0-9_]+) \(sampled\)$", re.IGNORECASE)
_SAMPLE_RE = re.compile(r"^Show sample ([A-Za-z0-9_]+) rows with (.+)$", re.IGNORECASE)
_DISTINCT_RE = re.compile(
    r"^List distinct values of ([A-Za-z0-9_]+) in ([A-Za-z0-9_]+) \(sample\)$",
    re.IGNORECASE,
)
_SAMPLE_KO_TABLE_PAREN_RE = re.compile(r"\(([A-Za-z0-9_]+)\)")
_SAMPLE_KO_LIMIT_RE = re.compile(r"(?:샘플\s*([0-9][0-9,]*)\s*(?:건|개|명|행|줄)|([0-9][0-9,]*)\s*(?:건|개|명|행|줄)\s*샘플)")
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*$")
_LIMIT_RE = re.compile(r"\blimit\s+(\d+)\s*;?\s*$", re.IGNORECASE)
_FETCH_RE = re.compile(r"\bfetch\s+first\s+(\d+)\s+rows\s+only\s*;?\s*$", re.IGNORECASE)
_TOP_RE = re.compile(r"^\s*select\s+top\s+(\d+)\s+", re.IGNORECASE)
_WHERE_TRUE_RE = re.compile(r"\bwhere\s+true\b", re.IGNORECASE)
_AND_TRUE_RE = re.compile(r"\band\s+true\b", re.IGNORECASE)
_INTERVAL_YEAR_RE = re.compile(r"interval\s+'(\d+)\s*year[s]?'", re.IGNORECASE)
_INTERVAL_MONTH_RE = re.compile(r"interval\s+'(\d+)\s*month[s]?'", re.IGNORECASE)
_INTERVAL_DAY_RE = re.compile(r"interval\s+'(\d+)\s*day[s]?'", re.IGNORECASE)
_TO_DATE_RE = re.compile(r"TO_DATE\s*\(\s*([A-Za-z0-9_\\.]+)\s*,\s*'[^']+'\s*\)", re.IGNORECASE)
_HAVING_WHERE_RE = re.compile(r"\bHAVING\s+WHERE\b", re.IGNORECASE)
_HAVING_TRUE_RE = re.compile(r"\bHAVING\s+1\s*=\s*1\b", re.IGNORECASE)
_EXTRACT_DAY_RE = re.compile(r"EXTRACT\s*\(\s*DAY\s+FROM\s+([^)]+)\)", re.IGNORECASE)
_COUNT_ALIAS_RE = re.compile(
    r"(COUNT\s*\(\s*(?:DISTINCT\s+)?(?:\*|[A-Za-z0-9_\.]+)\s*\)\s+(?:AS\s+)?)"
    r"([A-Za-z_][A-Za-z0-9_$#]*)(?=\s*(?:,|FROM\b|WHERE\b|GROUP\b|ORDER\b|HAVING\b|$))",
    re.IGNORECASE,
)
_HOSPITAL_EXPIRE_RE = re.compile(r"\bHOSPITAL_EXPIRE_FLAG\s+IS\s+NOT\s+NULL\b", re.IGNORECASE)
_AGE_FROM_ANCHOR_RE = re.compile(
    r"EXTRACT\s*\(\s*YEAR\s+FROM\s+(?:CURRENT_DATE|SYSDATE)\s*\)\s*-\s*([A-Za-z0-9_\\.]*ANCHOR_YEAR)",
    re.IGNORECASE,
)
_AGE_FROM_BIRTHDATE_RE = re.compile(
    r"EXTRACT\s*\(\s*YEAR\s+FROM\s+(?:CURRENT_DATE|SYSDATE)\s*\)\s*-\s*EXTRACT\s*\(\s*YEAR\s+FROM\s+([A-Za-z0-9_\\.]*"
    r"(?:BIRTHDATE|DOB))\s*\)",
    re.IGNORECASE,
)
_ANCHOR_MINUS_BIRTH_EXTRACT_RE = re.compile(
    r"(?P<anchor>[A-Za-z0-9_\\.]*ANCHOR_YEAR)\s*-\s*EXTRACT\s*\(\s*YEAR\s+FROM\s+"
    r"(?P<birth>[A-Za-z0-9_\\.]*(?:BIRTHDATE|DOB))\s*\)",
    re.IGNORECASE,
)
_BIRTH_YEAR_DIFF_RE = re.compile(r"([A-Za-z0-9_\\.]*ANCHOR_YEAR)\s*-\s*([A-Za-z0-9_\\.]*BIRTH_YEAR)", re.IGNORECASE)
_BIRTH_YEAR_RE = re.compile(r"\bBIRTH_YEAR\b", re.IGNORECASE)
_ORDER_BY_BAD_COUNT_RE = re.compile(
    r"\border\s+by\s+(?:count\(\*\)\s+)?"
    r"(?:label_count|test_count|organism_count|transition_count|event_count|admission_count|patient_count|transfer_count|count)\b",
    re.IGNORECASE,
)
_FOR_UPDATE_RE = re.compile(r"\bFOR\s+UPDATE\b(?:\s+SKIP\s+LOCKED)?", re.IGNORECASE)
_TIME_WINDOW_RE = re.compile(
    r"\b([A-Za-z0-9_\\.]*TIME)\b\s*(>=|>)\s*(SYSDATE|CURRENT_DATE)"
    r"(?:\s*-\s*INTERVAL\s*'[^']+'\s*(DAY|MONTH|YEAR))?"
    r"(?:\s+AND\s+\1\s*<=\s*(SYSDATE|CURRENT_DATE)"
    r"(?:\s*-\s*INTERVAL\s*'[^']+'\s*(DAY|MONTH|YEAR))?)?",
    re.IGNORECASE,
)
_QUESTION_TIME_INTENT_RE = re.compile(
    r"(최근|지난|작년|올해|전년|기간|이내|이후|전후|입원\s*후|수술\s*후|\d+\s*(일|주|개월|달|월|년)|"
    r"\b(last|past|recent|today|yesterday|week|month|year|since|before|after|between|from|to|within)\b)",
    re.IGNORECASE,
)
_DIAGNOSIS_TITLE_FILTER_RE = re.compile(
    r"(?:UPPER|LOWER)?\s*\(\s*(?:[A-Za-z0-9_]+\.)?LONG_TITLE\s*\)\s*(?:LIKE|=)\s*"
    r"(?:(?:UPPER|LOWER)\s*\(\s*)?'[^']+'(?:\s*\))?"
    r"|(?:[A-Za-z0-9_]+\.)?LONG_TITLE\s*(?:LIKE|=)\s*"
    r"(?:(?:UPPER|LOWER)\s*\(\s*)?'[^']+'(?:\s*\))?",
    re.IGNORECASE,
)
_ICD_CODE_LIKE_RE = re.compile(
    r"(?P<lhs>(?:[A-Za-z0-9_]+\.)?ICD_CODE)\s+LIKE\s+(?P<quote>'?)(?P<prefix>[A-Za-z0-9]+)%(?P=quote)",
    re.IGNORECASE,
)
_TO_CHAR_BARE_FMT_RE = re.compile(
    r"TO_CHAR\s*\(\s*(?P<expr>[^,]+?)\s*,\s*(?P<fmt>YYYY|YYY|YY|Y|MM|MON|MONTH|DD|HH24|MI|SS)\s*\)",
    re.IGNORECASE,
)
_JOIN_ICD_TABLE_RE = re.compile(r"\bJOIN\s+(DIAGNOSES_ICD|PROCEDURES_ICD)\b", re.IGNORECASE)
_COUNT_DENOM_NULLIF_RE = re.compile(
    r"/\s*NULLIF\s*\(\s*COUNT\s*\(\s*(?!DISTINCT)(?P<den>\*|[A-Za-z0-9_\.]+)\s*\)\s*,\s*0\s*\)",
    re.IGNORECASE,
)
_COUNT_DENOM_RE = re.compile(
    r"/\s*COUNT\s*\(\s*(?!DISTINCT)(?P<den>\*|[A-Za-z0-9_\.]+)\s*\)",
    re.IGNORECASE,
)
_RATIO_INTENT_RE = re.compile(
    r"(비율|비중|율|퍼센트|백분율|ratio|rate|proportion|percentage|pct)",
    re.IGNORECASE,
)
_RATIO_ALIAS_RE = re.compile(r"(RATE|RATIO|PCT|PERCENT)", re.IGNORECASE)
_RATIO_DENOM_INTENT_RE = re.compile(
    r"(overall|total|all|out of|among|전체|총|분모|전체 대비|모수)",
    re.IGNORECASE,
)
_COUNT_ALIAS_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*$")
_DENOM_ALIAS_HINT_RE = re.compile(r"(TOTAL|TOT|DENOM|ALL|BASE|OVERALL)", re.IGNORECASE)
_LAB_INTENT_RE = re.compile(r"(\blab(?:oratory|s|events?)?\b|검사|검체)", re.IGNORECASE)
_CATEGORICAL_REWRITE_INTENT_RE = re.compile(
    r"(service|department|admission\s*type|discharge\s*location|admission\s*location|insurance|race|ethnicity|language|status|category|code|"
    r"유형|종류|구분|상태|범주|코드|진료과|서비스)",
    re.IGNORECASE,
)
_SERVICE_STRATIFY_INTENT_RE = re.compile(
    r"(진료과|서비스|과별|부서|department|service|curr_service|prev_service)",
    re.IGNORECASE,
)
_ADMISSION_TYPE_QUERY_INTENT_RE = re.compile(
    r"(입원\s*유형|입원유형|admission\s*type|admission_type|emergency|urgent|elective)",
    re.IGNORECASE,
)
_DIAG_PROC_QUERY_INTENT_RE = re.compile(
    r"(진단|질환|병명|코드|icd|diagnos|procedure|시술|수술)",
    re.IGNORECASE,
)
_MORTALITY_QUERY_INTENT_RE = re.compile(
    r"(사망|mortality|death|expire)",
    re.IGNORECASE,
)
_ICU_QUERY_INTENT_RE = re.compile(r"(중환자실|\bicu\b)", re.IGNORECASE)
_MONTHLY_TREND_INTENT_RE = re.compile(
    r"(월별|monthly|month[-\s]*by[-\s]*month|추세|trend|변화|변했|how\s+.*change|time\s+trend|시계열)",
    re.IGNORECASE,
)
_FIRST_ICU_INTENT_RE = re.compile(
    r"(first\s+icu|first[-\s]*stay|initial\s+icu|index\s+icu|"
    r"첫\s*icu|첫번째\s*icu|최초\s*icu|처음\s*icu|첫\s*중환자실|최초\s*중환자실|처음\s*중환자실)",
    re.IGNORECASE,
)
_CATEGORICAL_EQ_LITERAL_RE = re.compile(
    r"(?P<ref>(?:[A-Za-z_][A-Za-z0-9_$#]*\.)?[A-Za-z_][A-Za-z0-9_$#]*)\s*=\s*'(?P<lit>[^']+)'",
    re.IGNORECASE,
)
_TABLE_ALIAS_REF_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_$#]*)(?!\.)\b(?:\s+([A-Za-z_][A-Za-z0-9_$#]*))?",
    re.IGNORECASE,
)
_ITEMID_SCALAR_SUBQUERY_EQ_RE = re.compile(
    r"(?P<lhs>(?:[A-Za-z_][A-Za-z0-9_$#]*\.)?ITEMID)\s*=\s*\(",
    re.IGNORECASE,
)
_ITEMID_ICD_EQ_RE = re.compile(
    r"(?P<lhs>[A-Za-z_][A-Za-z0-9_$#]*)\.(?P<lcol>ITEMID|ICD_CODE)\s*=\s*"
    r"(?P<rhs>[A-Za-z_][A-Za-z0-9_$#]*)\.(?P<rcol>ITEMID|ICD_CODE)",
    re.IGNORECASE,
)
_SIMPLE_COLUMN_REF_RE = re.compile(
    r"^(?:(?P<prefix>[A-Za-z_][A-Za-z0-9_$#]*)\.)?(?P<col>[A-Za-z_][A-Za-z0-9_$#]*)$",
    re.IGNORECASE,
)
_COUNTLIKE_ALIAS_RE = re.compile(
    r"^(CNT|COUNT|N_|NUM_|.*_CNT|.*_COUNT|TOTAL_.*|.*_TOTAL)$",
    re.IGNORECASE,
)
_RAW_LABEL_LIKE_RE = re.compile(
    r"(?P<ref>(?:[A-Za-z_][A-Za-z0-9_$#]*\.)?LABEL)\s+"
    r"(?P<op>LIKE|NOT\s+LIKE)\s+"
    r"'(?P<lit>[^']*)'",
    re.IGNORECASE,
)

def _schema_hints() -> dict[str, Any]:
    return load_sql_schema_hints()


def _table_aliases() -> dict[str, str]:
    value = _schema_hints().get("table_aliases")
    return value if isinstance(value, dict) else {}


def _column_aliases() -> dict[str, str]:
    value = _schema_hints().get("column_aliases")
    return value if isinstance(value, dict) else {}


def _patients_only_cols() -> set[str]:
    value = _schema_hints().get("patients_only_cols")
    return value if isinstance(value, set) else set()


def _admissions_only_cols() -> set[str]:
    value = _schema_hints().get("admissions_only_cols")
    return value if isinstance(value, set) else set()


def _tables_with_subject_id() -> set[str]:
    value = _schema_hints().get("tables_with_subject_id")
    return value if isinstance(value, set) else set()


def _tables_with_hadm_id() -> set[str]:
    value = _schema_hints().get("tables_with_hadm_id")
    return value if isinstance(value, set) else set()


def _micro_only_cols() -> set[str]:
    value = _schema_hints().get("micro_only_cols")
    return value if isinstance(value, set) else set()


def _timestamp_cols() -> set[str]:
    value = _schema_hints().get("timestamp_cols")
    return value if isinstance(value, set) else set()

_HAS_ICU_RE = re.compile(r"\bHAS_ICU_STAY\b\s*=\s*(?:'Y'|1|TRUE)", re.IGNORECASE)
_ICU_STAY_RE = re.compile(r"\bICU_STAY\b\s*=\s*(?:'Y'|'YES'|1|TRUE)", re.IGNORECASE)
_ICUSTAYS_FLAG_RE = re.compile(r"\bICUSTAYS\b\s*=\s*(?:'Y'|'YES'|1|TRUE)", re.IGNORECASE)
_ICUSTAYS_NOT_NULL_RE = re.compile(r"\bICUSTAYS\b\s+IS\s+NOT\s+NULL", re.IGNORECASE)
_DIFF_RE = re.compile(r"([A-Za-z0-9_\\.]+)\s*-\s*([A-Za-z0-9_\\.]+)")
_TS_DIFF_RE = re.compile(r"TIMESTAMPDIFF\s*\(\s*DAY\s*,\s*([A-Za-z0-9_\\.]+)\s*,\s*([A-Za-z0-9_\\.]+)\s*\)", re.IGNORECASE)
_EXTRACT_YEAR_RE = re.compile(r"EXTRACT\s*\(\s*YEAR\s+FROM\s+([A-Za-z0-9_\\.]+)\s*\)", re.IGNORECASE)
_OUTER_ROWNUM_RE = re.compile(
    r"^\s*SELECT\s+\*\s+FROM\s*\((SELECT .*?)\)\s*WHERE\s+ROWNUM\s*<=\s*(\d+)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_ABS_YEAR_RE = re.compile(r"(?<!\d)(?:19|20|21)\d{2}(?!\d)")
_SYSDATE_YEAR_DIFF_RE = re.compile(
    r"\(\s*(?:SYSDATE|CURRENT_DATE)\s*-\s*(?:CAST\s*\(\s*)?([A-Za-z0-9_\\.]+)"
    r"(?:\s+AS\s+DATE\s*\))?\s*\)\s*/\s*365(?:\.25)?",
    re.IGNORECASE,
)
_ADD_MONTHS_PRED_RE = re.compile(
    r"([A-Za-z0-9_\\.]+)\s*(>=|>|<=|<)\s*ADD_MONTHS\s*\(\s*(?:SYSDATE|CURRENT_DATE)\s*,\s*[-+]?\d+\s*\*\s*12\s*\)",
    re.IGNORECASE,
)
_POST_WINDOW_KO_RE = re.compile(r"후\s*(\d+)\s*일|(\d+)\s*일\s*이내")
_POST_WINDOW_EN_RE = re.compile(r"(?:within|after)\s+(\d+)\s+day", re.IGNORECASE)
_TOP_N_EN_RE = re.compile(r"\btop\s+(\d+)\b", re.IGNORECASE)
_TOP_N_KO_RE = re.compile(r"(?:상위|탑)\s*(\d+)")
_TOP_N_KO_ONLY_RE = re.compile(r"(?<!\d)([0-9][0-9,]*)\s*(?:개|건|명|행|줄)\s*만")
_COUNT_BY_GENDER_EN_RE = re.compile(r"\bcount\b.*\bby\s+gender\b", re.IGNORECASE)
_COUNT_BY_GENDER_KO_RE = re.compile(r"성별.*(건수|건|수|카운트)|.*(건수|건|수|카운트).*성별")
_AGE_GROUP_INTENT_RE = re.compile(r"(연령대|나이대|age\s*(group|band|range)|연령\s*구간)", re.IGNORECASE)
_GENDER_INTENT_RE = re.compile(r"(성별|남성|여성|\bgender\b|\bsex\b)", re.IGNORECASE)
_EXTREMA_INTENT_RE = re.compile(
    r"(가장\s*(많|적)|최대|최소|최고|최저|most|least|highest|lowest|max|min|top\s*1|bottom\s*1)",
    re.IGNORECASE,
)
_DIAGNOSIS_INTENT_RE = re.compile(r"(진단|diagnos)", re.IGNORECASE)
_DEATHTIME_FROM_DISCHTIME_RE = re.compile(
    r"(?P<death>(?:[A-Za-z0-9_]+\.)?DEATHTIME)\s*<=\s*\(?\s*(?P<dis>(?:[A-Za-z0-9_]+\.)?DISCHTIME)\s*\+\s*INTERVAL\s*'(?P<days>\d+)'\s*DAY\s*\)?",
    re.IGNORECASE,
)
_ICD_VERSION_CODE_AND_RE = re.compile(
    r"(?P<ver_col>(?:[A-Za-z0-9_]+\.)?ICD_VERSION)\s*=\s*(?P<version>9|10)\s+AND\s+"
    r"(?P<code_col>(?:[A-Za-z0-9_]+\.)?ICD_CODE)\s+LIKE\s+(?P<quote>'?)(?P<prefix>[A-Za-z0-9]+)%(?P=quote)",
    re.IGNORECASE,
)
_ICD_CODE_VERSION_AND_RE = re.compile(
    r"(?P<code_col>(?:[A-Za-z0-9_]+\.)?ICD_CODE)\s+LIKE\s+(?P<quote>'?)(?P<prefix>[A-Za-z0-9]+)%(?P=quote)\s+AND\s+"
    r"(?P<ver_col>(?:[A-Za-z0-9_]+\.)?ICD_VERSION)\s*=\s*(?P<version>9|10)",
    re.IGNORECASE,
)
_COMORBIDITY_HINT_RE = re.compile(r"(동반|comorbid|co[-\s]*morbid|\+|\band\b|및|함께|with)", re.IGNORECASE)
_EXPLICIT_ICD_PREFIX_RE = re.compile(r"\b(?:[A-TV-Z][0-9][0-9A-Z]{1,3}|[0-9]{3})\b", re.IGNORECASE)
_ICD_CODE_HINT_RE = re.compile(r"(icd|진단\s*코드|진단코드|코드)", re.IGNORECASE)
_ADM_ICU_IN_RE = re.compile(
    r"(?P<adm>[A-Za-z0-9_]+)\.HADM_ID\s+IN\s*\(\s*SELECT\s+HADM_ID\s+FROM\s+ICUSTAYS\s*\)",
    re.IGNORECASE,
)


def _is_ident_char(ch: str) -> bool:
    return ch.isalnum() or ch in {"_", "$", "#"}


def _token_at(text_upper: str, idx: int, token: str) -> bool:
    length = len(token)
    if text_upper[idx: idx + length] != token:
        return False
    prev = text_upper[idx - 1] if idx > 0 else " "
    nxt = text_upper[idx + length] if idx + length < len(text_upper) else " "
    if _is_ident_char(prev) or _is_ident_char(nxt):
        return False
    return True


def _find_final_select_from_span(sql: str) -> tuple[str, int, int] | None:
    core = sql.strip().rstrip(";")
    if not core:
        return None
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
            i += 6
            continue
        i += 1
    if last_select < 0:
        return None

    depth = 0
    in_single = False
    i = last_select + 6
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
        return None
    return core, last_select, from_idx


def _find_first_top_level_keyword(sql: str, start_idx: int, keywords: tuple[str, ...]) -> int:
    if not sql or start_idx < 0 or start_idx >= len(sql):
        return -1
    upper = sql.upper()
    depth = 0
    in_single = False
    i = start_idx
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
        if depth == 0:
            for keyword in keywords:
                if _token_at(upper, i, keyword.upper()):
                    return i
        i += 1
    return -1


def _split_top_level_csv(text: str) -> list[str]:
    items: list[str] = []
    depth = 0
    in_single = False
    start = 0
    i = 0
    while i < len(text):
        ch = text[i]
        if in_single:
            if ch == "'":
                if i + 1 < len(text) and text[i + 1] == "'":
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
        if ch == "," and depth == 0:
            segment = text[start:i].strip()
            if segment:
                items.append(segment)
            start = i + 1
        i += 1
    tail = text[start:].strip()
    if tail:
        items.append(tail)
    return items


def _is_single_count_distinct_hadm_projection(sql: str) -> bool:
    span = _find_final_select_from_span(sql)
    if not span:
        return False
    core, select_idx, from_idx = span
    select_clause = core[select_idx + len("SELECT"):from_idx]
    items = _split_top_level_csv(select_clause)
    if len(items) != 1:
        return False
    item = items[0].strip()
    return bool(
        re.match(
            r"^\s*COUNT\s*\(\s*DISTINCT\s+(?:[A-Za-z0-9_]+\.)?HADM_ID\s*\)\s*(?:AS\s+[A-Za-z_][A-Za-z0-9_$#]*)?\s*$",
            item,
            re.IGNORECASE,
        )
    )


def _extract_select_alias(expr: str) -> str | None:
    trimmed = expr.strip()
    if not trimmed:
        return None
    m = re.search(r"\bAS\s+([A-Za-z_][A-Za-z0-9_$#]*)\s*$", trimmed, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"\s+([A-Za-z_][A-Za-z0-9_$#]*)\s*$", trimmed)
    if not m:
        return None
    candidate = m.group(1)
    if candidate.upper() in {"END", "WHEN", "THEN", "ELSE", "NULL"}:
        return None
    return candidate


def _normalize_text_key(text: str) -> str:
    return re.sub(r"[^0-9A-Za-z가-힣]+", "", str(text or "").lower())


def _tokenize_text(text: str) -> list[str]:
    raw = re.split(r"[^0-9A-Za-z가-힣]+", str(text or "").lower())
    tokens = [token for token in raw if len(token) >= 2]
    deduped: list[str] = []
    for token in tokens:
        if token not in deduped:
            deduped.append(token)
    return deduped


def _sql_quote_literal(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def _find_matching_paren_index(text: str, open_idx: int) -> int | None:
    if open_idx < 0 or open_idx >= len(text) or text[open_idx] != "(":
        return None
    depth = 0
    in_single = False
    i = open_idx
    while i < len(text):
        ch = text[i]
        if in_single:
            if ch == "'":
                if i + 1 < len(text) and text[i + 1] == "'":
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
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return None


def _table_alias_map(sql: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for match in _TABLE_ALIAS_REF_RE.finditer(sql):
        table = str(match.group(1) or "").strip().upper()
        alias = str(match.group(2) or "").strip().upper()
        if not table:
            continue
        mapping[table] = table
        if alias and alias not in {"WHERE", "JOIN", "ON", "GROUP", "ORDER", "HAVING"}:
            mapping[alias] = table
    return mapping


def _column_value_index() -> dict[str, dict[str, list[str]]]:
    index: dict[str, dict[str, list[str]]] = {}
    for row in load_column_value_rows():
        table = str(row.get("table") or "").strip().upper()
        column = str(row.get("column") or "").strip().upper()
        value = str(row.get("value") or "").strip()
        if not table or not column or not value:
            continue
        table_bucket = index.setdefault(table, {})
        values = table_bucket.setdefault(column, [])
        if value not in values:
            values.append(value)

    # Some sources include only SERVICES.PREV_SERVICE values, while most analysis
    # questions filter current service (CURR_SERVICE). Share value catalogs between
    # the paired columns to avoid zero-row filters from unseen literals.
    services_bucket = index.get("SERVICES")
    if isinstance(services_bucket, dict):
        prev_values = list(services_bucket.get("PREV_SERVICE") or [])
        curr_values = list(services_bucket.get("CURR_SERVICE") or [])
        if prev_values and not curr_values:
            services_bucket["CURR_SERVICE"] = list(prev_values)
        elif curr_values and not prev_values:
            services_bucket["PREV_SERVICE"] = list(curr_values)
        elif prev_values and curr_values:
            merged: list[str] = []
            for value in [*prev_values, *curr_values]:
                if value not in merged:
                    merged.append(value)
            services_bucket["PREV_SERVICE"] = list(merged)
            services_bucket["CURR_SERVICE"] = list(merged)
    return index


def _select_best_categorical_values(
    literal: str,
    *,
    values: list[str],
    question_tokens: list[str],
) -> list[str]:
    lit_norm = _normalize_text_key(literal)
    lit_tokens = _tokenize_text(literal)
    if not values or (not lit_norm and not lit_tokens):
        return []
    lit_token_set = set(lit_tokens)
    q_token_set = set(question_tokens)

    scored: list[tuple[int, str]] = []
    for value in values:
        val_norm = _normalize_text_key(value)
        val_tokens = _tokenize_text(value)
        val_token_set = set(val_tokens)
        score = 0

        if lit_norm and val_norm == lit_norm:
            score += 100
        if lit_norm and val_norm:
            if lit_norm in val_norm or val_norm in lit_norm:
                score += 30
        overlap = len(lit_token_set & val_token_set)
        if overlap > 0:
            score += overlap * 15
        q_overlap = len(q_token_set & val_token_set)
        if q_overlap > 0:
            score += q_overlap * 4
        if score <= 0:
            continue
        scored.append((score, value))

    if not scored:
        return []
    scored.sort(key=lambda item: (-item[0], len(item[1]), item[1].lower()))
    top = scored[0][0]
    if top < 15:
        return []
    threshold = max(15, top - 8)
    selected: list[str] = []
    for score, value in scored:
        if score < threshold:
            continue
        if value not in selected:
            selected.append(value)
        if len(selected) >= 3:
            break
    return selected


def _parse_columns(text: str) -> list[str]:
    cleaned = re.sub(r"\s+and\s+", ",", text.strip(), flags=re.IGNORECASE)
    cols = [c.strip() for c in cleaned.split(",") if c.strip()]
    if not cols:
        return []
    if any(not _IDENT_RE.fullmatch(c) for c in cols):
        return []
    return cols


def _extract_sample_table_from_question(question: str) -> str | None:
    q = str(question or "")
    if not q:
        return None

    paren_match = _SAMPLE_KO_TABLE_PAREN_RE.search(q)
    if paren_match:
        candidate = str(paren_match.group(1) or "").strip().upper()
        if _IDENT_RE.fullmatch(candidate):
            return candidate

    q_upper = q.upper()
    table_candidates = sorted(
        set(_tables_with_subject_id()) | set(_tables_with_hadm_id()) | {"PATIENTS"},
        key=lambda name: (-len(name), name),
    )
    for table in table_candidates:
        if not table:
            continue
        if re.search(rf"(?<![A-Z0-9_]){re.escape(table)}(?![A-Z0-9_])", q_upper):
            return table
    return None


def _extract_sample_limit_from_question(question: str, default: int = 100) -> int:
    q = str(question or "")
    if not q:
        return default
    match = _SAMPLE_KO_LIMIT_RE.search(q)
    if not match:
        return default
    raw = str(match.group(1) or match.group(2) or "").replace(",", "").strip()
    if not raw.isdigit():
        return default
    try:
        value = int(raw)
    except Exception:
        return default
    return value if value > 0 else default


def _build_ko_sample_template(question: str) -> tuple[str | None, list[str]]:
    rules: list[str] = []
    q = str(question or "").strip()
    if not q:
        return None, rules
    q_lower = q.lower()
    if "샘플" not in q_lower:
        return None, rules
    if any(token in q_lower for token in ("column", "columns", "with ", "컬럼", "열", "항목", "포함")):
        return None, rules

    table = _extract_sample_table_from_question(q)
    if not table:
        return None, rules

    columns: list[str] = []
    if table == "PATIENTS":
        columns = ["SUBJECT_ID", "GENDER"]
    elif table == "POE":
        columns = ["POE_ID", "POE_SEQ"]
    elif table in _tables_with_subject_id():
        columns = ["SUBJECT_ID"]
        if table in _tables_with_hadm_id():
            columns.append("HADM_ID")

    first = _first(columns)
    if not columns or not first:
        return None, rules

    limit = _extract_sample_limit_from_question(q, default=100)
    cols_sql = ", ".join(columns)
    rules.append("sample_rows_template_ko")
    return f"SELECT {cols_sql} FROM {table} WHERE {first} IS NOT NULL AND ROWNUM <= {limit}", rules


def _first(items: Iterable[str]) -> str | None:
    for item in items:
        return item
    return None


def _find_table_alias(text: str, table: str) -> str | None:
    pattern = re.compile(rf"\b(from|join)\s+{re.escape(table)}(?:\s+([A-Za-z0-9_]+))?", re.IGNORECASE)
    match = pattern.search(text)
    if not match:
        return None
    alias = match.group(2) or table
    if alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER"}:
        return table
    return alias


def _has_lab_intent(question: str) -> bool:
    return bool(_LAB_INTENT_RE.search(str(question or "")))


def _next_join_alias(sql: str, base: str) -> str:
    used_aliases: set[str] = set()
    for match in re.finditer(
        r"\b(?:FROM|JOIN)\s+[A-Za-z0-9_]+(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?",
        sql,
        re.IGNORECASE,
    ):
        alias = str(match.group(1) or "").strip()
        if alias and alias.upper() not in {"WHERE", "JOIN", "ON", "GROUP", "ORDER", "HAVING"}:
            used_aliases.add(alias.upper())

    candidate = base
    suffix = 1
    while candidate.upper() in used_aliases:
        candidate = f"{base}{suffix}"
        suffix += 1
    return candidate


def _insert_join(text: str, join_clause: str) -> str:
    if re.search(r"\bwhere\b", text, re.IGNORECASE):
        return re.sub(r"\bwhere\b", join_clause + " WHERE", text, count=1, flags=re.IGNORECASE)
    if re.search(r"\bgroup\s+by\b", text, re.IGNORECASE):
        return re.sub(r"\bgroup\s+by\b", join_clause + " GROUP BY", text, count=1, flags=re.IGNORECASE)
    if re.search(r"\border\s+by\b", text, re.IGNORECASE):
        return re.sub(r"\border\s+by\b", join_clause + " ORDER BY", text, count=1, flags=re.IGNORECASE)
    return text.rstrip(";") + join_clause


def _inject_join_in_outer(
    sql: str,
    base_table: str,
    join_template: str,
    replace_from: str,
    replace_to: str,
) -> tuple[str | None, list[str]]:
    rules: list[str] = []
    match = _OUTER_ROWNUM_RE.match(sql)
    if not match:
        return None, rules
    inner = match.group(1)
    limit = match.group(2)

    pattern = re.compile(rf"\bfrom\s+{re.escape(base_table)}(?:\s+([A-Za-z0-9_]+))?", re.IGNORECASE)
    m = pattern.search(inner)
    if not m:
        return None, rules
    alias = m.group(1) or base_table
    if alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER"}:
        alias = base_table

    base_clause = f"FROM {base_table}"
    if m.group(1):
        base_clause = f"FROM {base_table} {alias}"
    join_clause = join_template.format(alias=alias)

    inner = pattern.sub(base_clause + join_clause, inner, count=1)
    inner = re.sub(replace_from, replace_to, inner, flags=re.IGNORECASE)
    rules.append("inject_join_in_outer")
    return f"SELECT * FROM ({inner}) WHERE ROWNUM <= {limit}", rules


def _wrap_with_rownum(sql: str, n: int) -> str:
    core = sql.strip().rstrip(";")
    return f"SELECT * FROM ({core}) WHERE ROWNUM <= {n}"


def _apply_rownum_cap(sql: str, cap: int = 100000) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def _inject_cap(inner_sql: str) -> str:
        if re.search(r"\bwhere\b", inner_sql, re.IGNORECASE):
            return re.sub(r"\bwhere\b", f"WHERE ROWNUM <= {cap} AND", inner_sql, count=1, flags=re.IGNORECASE)
        if re.search(r"\bgroup\s+by\b", inner_sql, re.IGNORECASE):
            return re.sub(
                r"\bgroup\s+by\b",
                f"WHERE ROWNUM <= {cap} GROUP BY",
                inner_sql,
                count=1,
                flags=re.IGNORECASE,
            )
        if re.search(r"\border\s+by\b", inner_sql, re.IGNORECASE):
            return re.sub(
                r"\border\s+by\b",
                f"WHERE ROWNUM <= {cap} ORDER BY",
                inner_sql,
                count=1,
                flags=re.IGNORECASE,
            )
        return inner_sql.rstrip(";") + f" WHERE ROWNUM <= {cap}"

    if "ROWNUM" in text.upper():
        match = _OUTER_ROWNUM_RE.match(text)
        if match:
            inner = match.group(1)
            limit = match.group(2)
            heavy_tables = {
                "LABEVENTS",
                "CHARTEVENTS",
                "MICROBIOLOGYEVENTS",
                "INPUTEVENTS",
                "OUTPUTEVENTS",
                "EMAR",
                "PRESCRIPTIONS",
            }
            if "ROWNUM" not in inner.upper() and any(
                re.search(rf"\b{t}\b", inner, re.IGNORECASE) for t in heavy_tables
            ):
                inner = _inject_cap(inner)
                rules.append(f"rownum_cap_inner_{cap}")
                return f"SELECT * FROM ({inner}) WHERE ROWNUM <= {limit}", rules
        return text, rules
    heavy_tables = {
        "LABEVENTS",
        "CHARTEVENTS",
        "MICROBIOLOGYEVENTS",
        "INPUTEVENTS",
        "OUTPUTEVENTS",
        "EMAR",
        "PRESCRIPTIONS",
    }
    if not any(re.search(rf"\b{t}\b", text, re.IGNORECASE) for t in heavy_tables):
        return text, rules
    if re.search(r"\bwhere\b", text, re.IGNORECASE):
        text = re.sub(r"\bwhere\b", f"WHERE ROWNUM <= {cap} AND", text, count=1, flags=re.IGNORECASE)
    else:
        # Insert WHERE before GROUP BY / ORDER BY if present
        if re.search(r"\bgroup\s+by\b", text, re.IGNORECASE):
            text = re.sub(r"\bgroup\s+by\b", f"WHERE ROWNUM <= {cap} GROUP BY", text, count=1, flags=re.IGNORECASE)
        elif re.search(r"\border\s+by\b", text, re.IGNORECASE):
            text = re.sub(r"\border\s+by\b", f"WHERE ROWNUM <= {cap} ORDER BY", text, count=1, flags=re.IGNORECASE)
        else:
            text = text.rstrip(";") + f" WHERE ROWNUM <= {cap}"
    rules.append(f"rownum_cap_{cap}")
    return text, rules


def _should_apply_rownum_cap_conservative(question: str, sql: str) -> bool:
    q = (question or "").lower()
    if not q or not sql:
        return False
    text_upper = sql.upper()
    explicit_sample_intent = any(
        token in q
        for token in (
            "sample",
            "샘플",
            "미리보기",
            "preview",
            "상위",
            "top ",
            "top-",
        )
    )
    if explicit_sample_intent:
        if _OUTER_ROWNUM_RE.match(sql.strip().rstrip(";")) and "GROUP BY" in text_upper and "COUNT(" in text_upper:
            # Top-N grouped ranking already has explicit row limiting.
            # Adding an inner sample cap can distort ranking semantics.
            return False
        return True
    if "GROUP BY" in text_upper:
        return False
    if any(agg in text_upper for agg in ("COUNT(", "AVG(", "SUM(", "MIN(", "MAX(")):
        return False
    return bool(
        re.search(
            r"\b(LABEVENTS|CHARTEVENTS|MICROBIOLOGYEVENTS|INPUTEVENTS|OUTPUTEVENTS|EMAR|PRESCRIPTIONS)\b",
            text_upper,
        )
    )


def _rewrite_oracle_syntax(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    # Replace WHERE TRUE / AND TRUE with Oracle-friendly boolean
    if _WHERE_TRUE_RE.search(text):
        text = _WHERE_TRUE_RE.sub("WHERE 1=1", text)
        rules.append("where_true_to_1eq1")
    if _AND_TRUE_RE.search(text):
        text = _AND_TRUE_RE.sub("AND 1=1", text)
        rules.append("and_true_to_1eq1")
    if "WHERE" not in text.upper() and re.search(r"\b1=1\b", text):
        text = re.sub(r"\b1=1\b", "WHERE 1=1", text, count=1, flags=re.IGNORECASE)
        rules.append("insert_where_for_1eq1")

    # Preserve JOIN location if WHERE is injected after an outer join rewrite
    text = re.sub(r"\bJOIN\b\s+(.*)\s+WHERE\s+1=1", r"JOIN \\1 WHERE 1=1", text, flags=re.IGNORECASE)

    # Normalize INTERVAL literals (Oracle expects INTERVAL 'n' YEAR|MONTH|DAY)
    if _INTERVAL_YEAR_RE.search(text):
        text = _INTERVAL_YEAR_RE.sub(r"INTERVAL '\1' YEAR", text)
        rules.append("interval_year_normalized")
    if _INTERVAL_MONTH_RE.search(text):
        text = _INTERVAL_MONTH_RE.sub(r"INTERVAL '\1' MONTH", text)
        rules.append("interval_month_normalized")
    if _INTERVAL_DAY_RE.search(text):
        text = _INTERVAL_DAY_RE.sub(r"INTERVAL '\1' DAY", text)
        rules.append("interval_day_normalized")

    # LIMIT / FETCH FIRST / TOP -> ROWNUM wrapper
    if _LIMIT_RE.search(text):
        n = int(_LIMIT_RE.search(text).group(1))
        text = _LIMIT_RE.sub("", text).rstrip()
        if "ROWNUM" not in text.upper():
            text = _wrap_with_rownum(text, n)
            rules.append("limit_to_rownum")
    if _FETCH_RE.search(text):
        n = int(_FETCH_RE.search(text).group(1))
        text = _FETCH_RE.sub("", text).rstrip()
        if "ROWNUM" not in text.upper():
            text = _wrap_with_rownum(text, n)
            rules.append("fetch_first_to_rownum")
    if _TOP_RE.search(text):
        n = int(_TOP_RE.search(text).group(1))
        text = _TOP_RE.sub("SELECT ", text, count=1)
        if "ROWNUM" not in text.upper():
            text = _wrap_with_rownum(text, n)
            rules.append("top_to_rownum")

    return text, rules


def _apply_schema_mappings(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    rules_cfg = load_sql_postprocess_rules().get("schema_aliases", {})
    table_aliases_cfg = rules_cfg.get("table_aliases", {})
    column_aliases_cfg = rules_cfg.get("column_aliases", {})
    use_schema_hints = bool(rules_cfg.get("use_schema_hints", False))
    table_aliases: dict[str, str] = {}
    column_aliases: dict[str, str] = {}
    if use_schema_hints:
        table_aliases.update(dict(_table_aliases()))
        column_aliases.update(dict(_column_aliases()))
    if isinstance(table_aliases_cfg, dict):
        table_aliases.update({
            str(src): str(dest)
            for src, dest in table_aliases_cfg.items()
            if str(src).strip() and str(dest).strip()
        })
    if isinstance(column_aliases_cfg, dict):
        column_aliases.update({
            str(src): str(dest)
            for src, dest in column_aliases_cfg.items()
            if str(src).strip() and str(dest).strip()
        })

    # Table name replacements: restrict to FROM/JOIN positions to avoid
    # accidental rewrites of non-table identifiers.
    for src, dest in table_aliases.items():
        pattern = re.compile(
            rf"(?P<prefix>\b(?:FROM|JOIN)\s+){re.escape(src)}\b",
            re.IGNORECASE,
        )
        if pattern.search(text):
            text = pattern.sub(lambda m: f"{m.group('prefix')}{dest}", text)
            rules.append(f"table:{src}->{dest}")

    # Column name replacements (case-insensitive, word boundaries)
    for src, dest in column_aliases.items():
        pattern = re.compile(rf"\b{re.escape(src)}\b", re.IGNORECASE)
        if pattern.search(text):
            text = pattern.sub(dest, text)
            rules.append(f"column:{src}->{dest}")

    return text, rules


def _ensure_patients_join(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    # Skip if PATIENTS already referenced
    if re.search(r"\bPATIENTS\b", text, re.IGNORECASE):
        return text, rules

    # Trigger only if patients-only columns appear unqualified
    needed = [c for c in _patients_only_cols() if re.search(rf"(?<!\.)\b{c}\b", text, re.IGNORECASE)]
    if not needed:
        return text, rules

    # Find base FROM table and optional alias (simple SQL only)
    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    base_table = m.group(1)
    base_alias = m.group(2) or base_table
    # If alias accidentally captured a keyword, ignore
    if base_alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER"}:
        base_alias = base_table

    if base_table.upper() not in _tables_with_subject_id():
        return text, rules

    # Insert JOIN before WHERE (or end if WHERE missing)
    join_clause = f" JOIN PATIENTS p ON {base_alias}.SUBJECT_ID = p.SUBJECT_ID"
    text = _insert_join(text, join_clause)

    # Qualify unqualified patients-only columns
    for col in needed:
        text = re.sub(rf"(?<!\.)\b{col}\b", f"p.{col}", text, flags=re.IGNORECASE)

    rules.append("join_patients_for_demographics")
    return text, rules


def _rewrite_patients_id(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    alias = _find_table_alias(text, "PATIENTS")
    changed = False
    if alias:
        pattern = re.compile(rf"\b{re.escape(alias)}\s*\.\s*ID\b", re.IGNORECASE)
        rewritten = pattern.sub(f"{alias}.SUBJECT_ID", text)
        if rewritten != text:
            text = rewritten
            changed = True
    rewritten_patients = re.sub(r"\bPATIENTS\s*\.\s*ID\b", "PATIENTS.SUBJECT_ID", text, flags=re.IGNORECASE)
    if rewritten_patients != text:
        text = rewritten_patients
        changed = True
    if changed:
        rules.append("patients_id_to_subject_id")
    return text, rules


def _ensure_admissions_join(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    if re.search(r"\bADMISSIONS\b", text, re.IGNORECASE):
        return text, rules

    needed = [c for c in _admissions_only_cols() if re.search(rf"(?<!\.)\b{c}\b", text, re.IGNORECASE)]
    if not needed:
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    base_table = m.group(1)
    base_alias = m.group(2) or base_table
    if base_alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER"}:
        base_alias = base_table

    if base_table.upper() not in _tables_with_subject_id():
        return text, rules

    join_clause = f" JOIN ADMISSIONS a ON {base_alias}.SUBJECT_ID = a.SUBJECT_ID"
    if re.search(r"\bHADM_ID\b", text, re.IGNORECASE):
        join_clause = f" JOIN ADMISSIONS a ON {base_alias}.SUBJECT_ID = a.SUBJECT_ID AND {base_alias}.HADM_ID = a.HADM_ID"

    text = _insert_join(text, join_clause)
    for col in needed:
        text = re.sub(rf"(?<!\.)\b{col}\b", f"a.{col}", text, flags=re.IGNORECASE)

    rules.append("join_admissions_for_admission_fields")
    return text, rules


def _ensure_microbiology_table(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    if re.search(r"\bMICROBIOLOGYEVENTS\b", text, re.IGNORECASE):
        return text, rules

    needed = [c for c in _micro_only_cols() if re.search(rf"(?<!\.)\b{c}\b", text, re.IGNORECASE)]
    if not needed:
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    base_table = m.group(1)
    base_alias = m.group(2) or base_table
    if base_alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER"}:
        base_alias = base_table

    replacement = "FROM MICROBIOLOGYEVENTS"
    if m.group(2):
        replacement = f"FROM MICROBIOLOGYEVENTS {base_alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_microbiology_table")
    return text, rules


def _ensure_microbiology_by_question(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bMICROBIOLOGYEVENTS\b", text, re.IGNORECASE):
        return text, rules
    q = question.lower()
    if not any(k in q for k in ("micro", "microbiology", "organism", "antibiotic", "culture", "specimen")):
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM MICROBIOLOGYEVENTS"
    if alias:
        replacement = f"FROM MICROBIOLOGYEVENTS {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_microbiology_by_question")
    return text, rules


def _ensure_icustays_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bICUSTAYS\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    icu_only = "icu stay" in q or "icu stays" in q or ("icu" in q and "los" in q)
    if "admission" in q or "admissions" in q or "patient" in q or "patients" in q:
        icu_only = False

    # INTIME/OUTTIME are shared with TRANSFERS and should not alone force ICUSTAYS.
    icu_cols = {"FIRST_CAREUNIT", "LAST_CAREUNIT", "LOS", "STAY_ID"}
    has_icu_cols = any(re.search(rf"(?<!\.)\b{c}\b", text, re.IGNORECASE) for c in icu_cols)
    if not icu_only and not has_icu_cols:
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM ICUSTAYS"
    if alias:
        replacement = f"FROM ICUSTAYS {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_icustays_table")
    return text, rules


def _ensure_chartevents_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bCHARTEVENTS\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    if "chart event" not in q and "chart events" not in q and "chart" not in q:
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM CHARTEVENTS"
    if alias:
        replacement = f"FROM CHARTEVENTS {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_chartevents_table")
    return text, rules


def _ensure_chart_label(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "label" not in q or "chart" not in q:
        return text, rules
    if re.search(r"\bD_ITEMS\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"\bCHARTEVENTS\b", text, re.IGNORECASE):
        return text, rules

    alias = _find_table_alias(text, "CHARTEVENTS") or "CHARTEVENTS"
    label_alias = _next_join_alias(text, "d")
    join_clause = f" JOIN D_ITEMS {label_alias} ON {alias}.ITEMID = {label_alias}.ITEMID"
    text = _insert_join(text, join_clause)
    text = re.sub(r"(?<!\.)\bLABEL\b", f"{label_alias}.LABEL", text, flags=re.IGNORECASE)
    rules.append("force_chart_label")
    return text, rules


def _ensure_labevents_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bLABEVENTS\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    if not _has_lab_intent(q):
        return text, rules
    if "micro" in q or "microbiology" in q:
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM LABEVENTS"
    if alias:
        replacement = f"FROM LABEVENTS {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_labevents_table")
    return text, rules


def _ensure_lab_label(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "label" not in q or not _has_lab_intent(q):
        return text, rules
    if re.search(r"\bD_LABITEMS\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"\bLABEVENTS\b", text, re.IGNORECASE):
        return text, rules

    alias = _find_table_alias(text, "LABEVENTS") or "LABEVENTS"
    label_alias = _next_join_alias(text, "d")
    join_clause = f" JOIN D_LABITEMS {label_alias} ON {alias}.ITEMID = {label_alias}.ITEMID"
    text = _insert_join(text, join_clause)
    text = re.sub(r"(?<!\.)\bLABEL\b", f"{label_alias}.LABEL", text, flags=re.IGNORECASE)
    rules.append("force_lab_label")
    return text, rules


def _rewrite_label_field(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "label" not in q:
        return text, rules

    if "chart" in q and "lab" not in q:
        alias = _find_table_alias(text, "D_ITEMS")
        if alias:
            text = re.sub(r"(?<!\.)\bITEMID\b", f"{alias}.LABEL", text, flags=re.IGNORECASE)
            text = re.sub(r"(?<!\.)\bLABEL\b", f"{alias}.LABEL", text, flags=re.IGNORECASE)
            rules.append("chart_label_itemid_to_label")
        return text, rules

    if "lab" in q or "laboratory" in q:
        alias = _find_table_alias(text, "D_LABITEMS")
        if alias:
            text = re.sub(r"(?<!\.)\bITEMID\b", f"{alias}.LABEL", text, flags=re.IGNORECASE)
            text = re.sub(r"(?<!\.)\bLABEL\b", f"{alias}.LABEL", text, flags=re.IGNORECASE)
            rules.append("lab_label_itemid_to_label")
    return text, rules


def _ensure_prescriptions_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bPRESCRIPTIONS\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    if "emar" in q or "ingredient" in q:
        return text, rules
    triggers = ("prescription", "drug", "medication", "doses", "formulation")
    if not any(t in q for t in triggers):
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM PRESCRIPTIONS"
    if alias:
        replacement = f"FROM PRESCRIPTIONS {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_prescriptions_table")
    return text, rules


def _ensure_inputevents_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bINPUTEVENTS\b", text, re.IGNORECASE):
        return text, rules
    q = question.lower()
    triggers = ("input event", "input events", "input amount", "intake", "fluid intake", "infusion", "infusions")
    if not any(t in q for t in triggers):
        return text, rules
    if "ingredient" in q:
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM INPUTEVENTS"
    if alias:
        replacement = f"FROM INPUTEVENTS {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_inputevents_table")
    return text, rules


def _ensure_outputevents_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bOUTPUTEVENTS\b", text, re.IGNORECASE):
        return text, rules
    q = question.lower()
    triggers = ("output event", "output events", "output value", "output volume", "urine output", "drain output")
    if not any(t in q for t in triggers):
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM OUTPUTEVENTS"
    if alias:
        replacement = f"FROM OUTPUTEVENTS {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_outputevents_table")
    return text, rules


def _ensure_emar_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    triggers = ("emar", "med admin", "medication administration", "administration record", "dose given", "dose due")
    if not any(t in q for t in triggers):
        return text, rules

    detail_triggers = ("detail", "administration type", "dose given", "dose due", "barcode")
    target = "EMAR_DETAIL" if any(t in q for t in detail_triggers) else "EMAR"
    if re.search(rf"\b{target}\b", text, re.IGNORECASE):
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = f"FROM {target}"
    if alias:
        replacement = f"FROM {target} {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append(f"force_{target.lower()}_table")
    return text, rules


def _ensure_diagnoses_icd_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "diagnos" not in q:
        return text, rules
    if "title" in q:
        return text, rules
    if re.search(r"\bDIAGNOSES_ICD\b", text, re.IGNORECASE):
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM DIAGNOSES_ICD"
    if alias:
        replacement = f"FROM DIAGNOSES_ICD {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_diagnoses_icd_table")
    return text, rules


def _ensure_procedures_icd_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "procedur" not in q:
        return text, rules
    if "title" in q:
        return text, rules
    if "procedure event" in q or "procedureevents" in q:
        return text, rules
    if re.search(r"\bPROCEDURES_ICD\b", text, re.IGNORECASE):
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM PROCEDURES_ICD"
    if alias:
        replacement = f"FROM PROCEDURES_ICD {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_procedures_icd_table")
    return text, rules


def _rewrite_prescriptions_drug_field(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bPRESCRIPTIONS\b", text, re.IGNORECASE):
        return text, rules
    q = question.lower()
    if "drug" not in q and "medication" not in q:
        return text, rules

    if re.search(r"(?<!\.)\bITEMID\b", text, re.IGNORECASE):
        text = re.sub(r"(?<!\.)\bITEMID\b", "DRUG", text, flags=re.IGNORECASE)
        rules.append("prescriptions_itemid_to_drug")
    return text, rules


def _rewrite_prescriptions_columns(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    aliases: set[str] = {"PRESCRIPTIONS"}
    for m in re.finditer(
        r"\b(?:FROM|JOIN)\s+PRESCRIPTIONS(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?",
        text,
        re.IGNORECASE,
    ):
        alias = m.group(1)
        if alias and alias.upper() not in {"ON", "WHERE", "JOIN", "GROUP", "ORDER", "INNER", "LEFT", "RIGHT", "FULL"}:
            aliases.add(alias)

    aliases_upper = {a.upper() for a in aliases}

    def replace_qualified(col: str, repl: str) -> bool:
        nonlocal text
        changed = False

        def _repl(match: re.Match) -> str:
            nonlocal changed
            alias = match.group(1)
            if alias.upper() in aliases_upper:
                changed = True
                return f"{alias}.{repl}"
            return match.group(0)

        text = re.sub(
            rf"\b([A-Za-z0-9_]+)\.{col}\b",
            _repl,
            text,
            flags=re.IGNORECASE,
        )
        return changed

    if replace_qualified("MEDICATION", "DRUG"):
        rules.append("prescriptions_medication_to_drug")
    if replace_qualified("CHARTTIME", "STARTTIME"):
        rules.append("prescriptions_charttime_to_starttime")

    # If EMAR is absent, unqualified MEDICATION/CHARTTIME in PRESCRIPTIONS context should map to DRUG/STARTTIME.
    has_emar = re.search(r"\bEMAR(?:_DETAIL)?\b", text, re.IGNORECASE) is not None
    if not has_emar:
        if re.search(r"(?<!\.)\bMEDICATION\b", text, re.IGNORECASE):
            text = re.sub(r"(?<!\.)\bMEDICATION\b", "DRUG", text, flags=re.IGNORECASE)
            rules.append("prescriptions_unqualified_medication_to_drug")
        if re.search(r"(?<!\.)\bCHARTTIME\b", text, re.IGNORECASE):
            text = re.sub(r"(?<!\.)\bCHARTTIME\b", "STARTTIME", text, flags=re.IGNORECASE)
            rules.append("prescriptions_unqualified_charttime_to_starttime")

    return text, rules


def _rewrite_icd_code_field(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "code" not in q:
        return text, rules

    if "diagnos" in q and re.search(r"\bDIAGNOSES_ICD\b", text, re.IGNORECASE):
        if re.search(r"(?<!\.)\bITEMID\b", text, re.IGNORECASE):
            text = re.sub(r"(?<!\.)\bITEMID\b", "ICD_CODE", text, flags=re.IGNORECASE)
            rules.append("diagnoses_itemid_to_icd_code")
        return text, rules

    if "procedur" in q and re.search(r"\bPROCEDURES_ICD\b", text, re.IGNORECASE):
        if re.search(r"(?<!\.)\bITEMID\b", text, re.IGNORECASE):
            text = re.sub(r"(?<!\.)\bITEMID\b", "ICD_CODE", text, flags=re.IGNORECASE)
            rules.append("procedures_itemid_to_icd_code")
    return text, rules


def _rewrite_itemid_in_icd_tables(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bDIAGNOSES_ICD\b", text, re.IGNORECASE) or re.search(r"\bPROCEDURES_ICD\b", text, re.IGNORECASE):
        if re.search(r"\bITEMID\b", text, re.IGNORECASE):
            text = re.sub(r"(?<!\.)\bITEMID\b", "ICD_CODE", text, flags=re.IGNORECASE)
            text = re.sub(r"\b([A-Za-z0-9_]+)\.ITEMID\b", r"\1.ICD_CODE", text, flags=re.IGNORECASE)
            rules.append("icd_tables_itemid_to_icd_code")
    return text, rules


def _rewrite_emar_medication_field(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bEMAR\b", text, re.IGNORECASE):
        return text, rules
    q = question.lower()
    if "medication" not in q and "drug" not in q:
        return text, rules

    if re.search(r"(?<!\.)\bITEMID\b", text, re.IGNORECASE):
        text = re.sub(r"(?<!\.)\bITEMID\b", "MEDICATION", text, flags=re.IGNORECASE)
        rules.append("emar_itemid_to_medication")
    return text, rules


def _ensure_diagnosis_title_join(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "diagnos" not in q or "title" not in q:
        return text, rules
    if re.search(r"\bDIAGNOSES_ICD\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"\bD_ICD_DIAGNOSES\b", text, re.IGNORECASE):
        return text, rules

    replacement = (
        "FROM DIAGNOSES_ICD dx JOIN D_ICD_DIAGNOSES d "
        "ON dx.ICD_CODE = d.ICD_CODE AND dx.ICD_VERSION = d.ICD_VERSION"
    )
    text = re.sub(
        r"\bfrom\s+D_ICD_DIAGNOSES\b(?:\s+[A-Za-z0-9_]+)?",
        replacement,
        text,
        count=1,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"(?<!\.)\bLONG_TITLE\b", "d.LONG_TITLE", text, flags=re.IGNORECASE)
    rules.append("diagnosis_title_join")
    return text, rules


def _ensure_procedure_title_join(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "procedur" not in q or "title" not in q:
        return text, rules
    if re.search(r"\bPROCEDURES_ICD\b", text, re.IGNORECASE):
        return text, rules

    replacement = (
        "FROM PROCEDURES_ICD p JOIN D_ICD_PROCEDURES d "
        "ON p.ICD_CODE = d.ICD_CODE AND p.ICD_VERSION = d.ICD_VERSION"
    )
    text = re.sub(
        r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?",
        replacement,
        text,
        count=1,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"(?<!\.)\bLONG_TITLE\b", "d.LONG_TITLE", text, flags=re.IGNORECASE)
    rules.append("procedure_title_join")
    return text, rules


def _cleanup_procedure_title_joins(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bD_ICD_PROCEDURES\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"\b(ITEMID|TO_NUMBER)\b", text, re.IGNORECASE):
        return text, rules

    pattern = re.compile(r"\bJOIN\s+D_ICD_PROCEDURES\b", re.IGNORECASE)
    pos = 0
    while True:
        m = pattern.search(text, pos)
        if not m:
            break
        start = m.start()
        tail = text[m.end():]
        end_match = re.search(r"\bJOIN\b|\bWHERE\b|\bGROUP\b|\bORDER\b", tail, re.IGNORECASE)
        end = m.end() + (end_match.start() if end_match else len(tail))
        segment = text[start:end]
        if re.search(r"\bITEMID\b|\bTO_NUMBER\b", segment, re.IGNORECASE):
            text = text[:start] + " " + text[end:]
            rules.append("drop_bad_d_icd_procedures_join")
            pos = start
        else:
            pos = end
    return text, rules


def _ensure_services_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bSERVICES\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    if "service" not in q:
        return text, rules
    if "order" in q or "poe" in q:
        return text, rules

    if not re.search(r"\b(CURR_SERVICE|PREV_SERVICE)\b", text, re.IGNORECASE) and "current service" not in q:
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM SERVICES"
    if alias:
        replacement = f"FROM SERVICES {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_services_table")
    return text, rules


def _ensure_transfers_eventtype(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "transfer" not in q:
        return text, rules
    if "event type" not in q and "eventtype" not in q:
        return text, rules

    if re.search(r"\bSERVICES\b", text, re.IGNORECASE) or re.search(
        r"\b(CURR_SERVICE|PREV_SERVICE|ORDER_TYPE)\b", text, re.IGNORECASE
    ):
        m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
        if m:
            alias = m.group(2)
            replacement = "FROM TRANSFERS"
            if alias:
                replacement = f"FROM TRANSFERS {alias}"
            text = re.sub(
                r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?",
                replacement,
                text,
                count=1,
                flags=re.IGNORECASE,
            )
            rules.append("force_transfers_table")

    if re.search(r"(?<!\.)\b(CURR_SERVICE|PREV_SERVICE|ORDER_TYPE)\b", text, re.IGNORECASE):
        text = re.sub(
            r"(?<!\.)\b(CURR_SERVICE|PREV_SERVICE|ORDER_TYPE)\b",
            "EVENTTYPE",
            text,
            flags=re.IGNORECASE,
        )
        rules.append("eventtype_from_transfers")
    return text, rules


def _rewrite_services_order_type(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bSERVICES\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"(?<!\.)\bORDER_TYPE\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    target = "CURR_SERVICE"
    if "previous service" in q or "prev service" in q or "prior service" in q:
        target = "PREV_SERVICE"
    elif "current service" in q:
        target = "CURR_SERVICE"
    text = re.sub(r"(?<!\.)\bORDER_TYPE\b", target, text, flags=re.IGNORECASE)
    rules.append("services_order_type_to_curr_prev")
    return text, rules


def _rewrite_icustays_careunit(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bICUSTAYS\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"\bCAREUNIT\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    target = "FIRST_CAREUNIT"
    if "last careunit" in q or "last care unit" in q:
        target = "LAST_CAREUNIT"
    elif "first careunit" in q or "first care unit" in q:
        target = "FIRST_CAREUNIT"

    aliases: set[str] = {"ICUSTAYS"}
    for m in re.finditer(
        r"\b(?:FROM|JOIN)\s+ICUSTAYS(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?",
        text,
        re.IGNORECASE,
    ):
        alias = m.group(1)
        if alias and alias.upper() not in {"ON", "WHERE", "JOIN", "GROUP", "ORDER", "INNER", "LEFT", "RIGHT", "FULL"}:
            aliases.add(alias)

    aliases_upper = {a.upper() for a in aliases}
    updated = re.sub(
        r"\b([A-Za-z0-9_]+)\.CAREUNIT\b",
        lambda m: f"{m.group(1)}.{target}" if m.group(1).upper() in aliases_upper else m.group(0),
        text,
        flags=re.IGNORECASE,
    )

    # ICUSTAYS 단일 문맥일 때만 비한정 CAREUNIT을 FIRST/LAST로 보정
    if not re.search(r"\bTRANSFERS\b", updated, re.IGNORECASE):
        updated = re.sub(r"(?<!\.)\bCAREUNIT\b", target, updated, flags=re.IGNORECASE)

    if updated != text:
        text = updated
        rules.append("icustays_careunit_to_first_last")
    return text, rules


def _rewrite_transfers_careunit_fields(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bTRANSFERS\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"\b(FIRST_CAREUNIT|LAST_CAREUNIT)\b", text, re.IGNORECASE):
        return text, rules

    aliases: set[str] = {"TRANSFERS"}
    for m in re.finditer(
        r"\b(?:FROM|JOIN)\s+TRANSFERS(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?",
        text,
        re.IGNORECASE,
    ):
        alias = m.group(1)
        if alias and alias.upper() not in {"ON", "WHERE", "JOIN", "GROUP", "ORDER", "INNER", "LEFT", "RIGHT", "FULL"}:
            aliases.add(alias)

    aliases_upper = {a.upper() for a in aliases}
    updated = re.sub(
        r"\b([A-Za-z0-9_]+)\.(FIRST_CAREUNIT|LAST_CAREUNIT)\b",
        lambda m: f"{m.group(1)}.CAREUNIT" if m.group(1).upper() in aliases_upper else m.group(0),
        text,
        flags=re.IGNORECASE,
    )

    # TRANSFERS만 사용하는 문맥의 비한정 FIRST/LAST_CAREUNIT 보정
    if not re.search(r"\bICUSTAYS\b", updated, re.IGNORECASE):
        updated = re.sub(r"(?<!\.)\b(FIRST_CAREUNIT|LAST_CAREUNIT)\b", "CAREUNIT", updated, flags=re.IGNORECASE)

    if updated != text:
        text = updated
        rules.append("transfers_careunit_to_careunit")
    return text, rules


def _strip_rownum_cap_for_micro_topk(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    match = _OUTER_ROWNUM_RE.match(text)
    if not match:
        return text, rules
    inner = match.group(1)
    limit = match.group(2)
    if not re.search(r"\bMICROBIOLOGYEVENTS\b", inner, re.IGNORECASE):
        return text, rules

    new_inner = re.sub(
        r"\bWHERE\s+ROWNUM\s*<=\s*\d+\s+AND\s+",
        "WHERE ",
        inner,
        flags=re.IGNORECASE,
    )
    new_inner = re.sub(
        r"\bWHERE\s+ROWNUM\s*<=\s*\d+\s+GROUP\s+BY\b",
        "GROUP BY",
        new_inner,
        flags=re.IGNORECASE,
    )
    new_inner = re.sub(
        r"\bWHERE\s+ROWNUM\s*<=\s*\d+\s+ORDER\s+BY\b",
        "ORDER BY",
        new_inner,
        flags=re.IGNORECASE,
    )
    new_inner = re.sub(
        r"\bWHERE\s+ROWNUM\s*<=\s*\d+\b",
        "",
        new_inner,
        flags=re.IGNORECASE,
    )

    if new_inner != inner:
        text = f"SELECT * FROM ({new_inner.strip()}) WHERE ROWNUM <= {limit}"
        rules.append("strip_rownum_cap_for_micro_topk")
    return text, rules


def _strip_rownum_cap_for_grouped_tables(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    upper = text.upper()
    if "GROUP BY" not in upper:
        return text, rules

    if not re.search(r"\b(PRESCRIPTIONS|INPUTEVENTS|OUTPUTEVENTS)\b", upper):
        return text, rules

    def _maybe_strip(match: re.Match, replacement: str) -> str:
        nonlocal changed
        try:
            limit = int(match.group(1))
        except (TypeError, ValueError):
            return match.group(0)
        if limit < 1000:
            return match.group(0)
        changed = True
        return replacement

    changed = False
    text = re.sub(
        r"\bWHERE\s+ROWNUM\s*<=\s*(\d+)\s+AND\s+",
        lambda m: _maybe_strip(m, "WHERE "),
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\s+AND\s+ROWNUM\s*<=\s*(\d+)\s+AND\s+",
        lambda m: _maybe_strip(m, " AND "),
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\bWHERE\s+ROWNUM\s*<=\s*(\d+)\s+GROUP\s+BY\b",
        lambda m: _maybe_strip(m, "GROUP BY"),
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\bWHERE\s+ROWNUM\s*<=\s*(\d+)\s+ORDER\s+BY\b",
        lambda m: _maybe_strip(m, "ORDER BY"),
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\bWHERE\s+ROWNUM\s*<=\s*(\d+)\b",
        lambda m: _maybe_strip(m, ""),
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\s+AND\s+ROWNUM\s*<=\s*(\d+)\b",
        lambda m: _maybe_strip(m, ""),
        text,
        flags=re.IGNORECASE,
    )

    new_text, changed = text, changed
    if changed:
        rules.append("strip_rownum_cap_for_grouped_tables")
    return new_text, rules


def _pushdown_outer_predicates(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    pattern = re.compile(
        r"^\s*SELECT\s+\*\s+FROM\s*\((SELECT .*?)\)\s*WHERE\s+(.+?)\s*;?\s*$",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.match(text)
    if not match:
        return text, rules
    inner = match.group(1)
    where_clause = match.group(2)
    m_limit = re.search(r"\bROWNUM\s*<=\s*(\d+)\b", where_clause, re.IGNORECASE)
    if not m_limit:
        return text, rules
    limit = m_limit.group(1)
    pred = re.sub(r"\bROWNUM\s*<=\s*\d+\b", "", where_clause, flags=re.IGNORECASE)
    pred = re.sub(r"\bAND\b\s*(\bAND\b)?", "AND", pred, flags=re.IGNORECASE)
    pred = pred.strip()
    pred = re.sub(r"^(AND|OR)\s+", "", pred, flags=re.IGNORECASE)
    pred = re.sub(r"\s+(AND|OR)$", "", pred, flags=re.IGNORECASE)
    pred = pred.strip()
    if not pred:
        return text, rules

    if re.search(r"\bwhere\b", inner, re.IGNORECASE):
        inner = re.sub(r"\bwhere\b", f"WHERE {pred} AND", inner, count=1, flags=re.IGNORECASE)
    elif re.search(r"\bgroup\s+by\b", inner, re.IGNORECASE):
        inner = re.sub(r"\bgroup\s+by\b", f"WHERE {pred} GROUP BY", inner, count=1, flags=re.IGNORECASE)
    elif re.search(r"\border\s+by\b", inner, re.IGNORECASE):
        inner = re.sub(r"\border\s+by\b", f"WHERE {pred} ORDER BY", inner, count=1, flags=re.IGNORECASE)
    else:
        inner = inner.rstrip(";") + f" WHERE {pred}"

    text = f"SELECT * FROM ({inner}) WHERE ROWNUM <= {limit}"
    rules.append("pushdown_outer_predicate")
    return text, rules


def _fix_missing_where_predicate(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def fix_segment(segment: str) -> tuple[str, bool]:
        if re.search(r"\bwhere\b", segment, re.IGNORECASE):
            return segment, False
        match = re.search(
            r"\b([A-Za-z0-9_\.]+(?:\s+IS\s+NOT\s+NULL|\s+IS\s+NULL)"
            r"(?:\s+AND\s+[A-Za-z0-9_\.]+(?:\s+IS\s+NOT\s+NULL|\s+IS\s+NULL))*)\s+GROUP\s+BY\b",
            segment,
            re.IGNORECASE,
        )
        if not match:
            return segment, False
        predicate = match.group(1)
        fixed = segment.replace(f"{predicate} GROUP BY", f"WHERE {predicate} GROUP BY", 1)
        return fixed, True

    match = _OUTER_ROWNUM_RE.match(text)
    if match:
        inner = match.group(1)
        limit = match.group(2)
        fixed_inner, changed = fix_segment(inner)
        if changed:
            text = f"SELECT * FROM ({fixed_inner}) WHERE ROWNUM <= {limit}"
            rules.append("insert_missing_where_predicate")
        return text, rules

    fixed_text, changed = fix_segment(text)
    if changed:
        rules.append("insert_missing_where_predicate")
    return fixed_text, rules


def _rewrite_icustays_los(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bICUSTAYS\b", text, re.IGNORECASE):
        return text, rules

    pattern = re.compile(
        r"CAST\(([^)]+OUTTIME[^)]*)\s+AS\s+DATE\)\s*-\s*CAST\(([^)]+INTIME[^)]*)\s+AS\s+DATE\)",
        re.IGNORECASE,
    )
    new_text = pattern.sub("LOS", text)
    if new_text != text:
        rules.append("icustays_diff_to_los")
        return new_text, rules

    pattern_rev = re.compile(
        r"CAST\(([^)]+INTIME[^)]*)\s+AS\s+DATE\)\s*-\s*CAST\(([^)]+OUTTIME[^)]*)\s+AS\s+DATE\)",
        re.IGNORECASE,
    )
    new_text = pattern_rev.sub("LOS", text)
    if new_text != text:
        rules.append("icustays_diff_to_los")
        return new_text, rules
    return text, rules


def _rewrite_warning_flag(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "warning" not in q:
        return text, rules
    if not re.search(r"\bCHARTEVENTS\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"(?<!\.)\bSTATUSDESCRIPTION\b", text, re.IGNORECASE):
        return text, rules
    text = re.sub(r"(?<!\.)\bSTATUSDESCRIPTION\b", "WARNING", text, flags=re.IGNORECASE)
    rules.append("warning_flag_from_chartevents")
    return text, rules


def _rewrite_lab_priority(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "priority" not in q:
        return text, rules
    if not re.search(r"\bLABEVENTS\b", text, re.IGNORECASE):
        return text, rules
    if re.search(r"(?<!\.)\bPRIORITY\b", text, re.IGNORECASE):
        return text, rules
    text = re.sub(r"(?<!\.)\bSPEC_TYPE_DESC\b", "PRIORITY", text, flags=re.IGNORECASE)
    rules.append("lab_priority_from_labevents")
    return text, rules


def _rewrite_micro_count_field(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bMICROBIOLOGYEVENTS\b", text, re.IGNORECASE):
        return text, rules
    q = question.lower()
    target = None
    if "antibiotic" in q:
        target = "AB_NAME"
    elif "organism" in q:
        target = "ORG_NAME"
    elif "test" in q:
        target = "TEST_NAME"
    if not target:
        return text, rules

    # Replace the selected/grouped field if it is a generic ID.
    text = re.sub(
        r"(?<!\.)\b(MICROEVENT_ID|MICRO_SPECIMEN_ID|ITEMID|TEST_ITEMID|ORG_ITEMID|AB_ITEMID)\b",
        target,
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\bGROUP\s+BY\s+(.*)",
        lambda m: re.sub(
            r"(?<!\.)\b(MICROEVENT_ID|MICRO_SPECIMEN_ID|ITEMID|TEST_ITEMID|ORG_ITEMID|AB_ITEMID)\b",
            target,
            m.group(0),
            flags=re.IGNORECASE,
        ),
        text,
        flags=re.IGNORECASE,
    )
    rules.append("micro_count_field_to_name")
    return text, rules


def _ensure_icd_join(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"(?<!\.)\bICD_CODE\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    target = "DIAGNOSES_ICD"
    if "procedure" in q:
        target = "PROCEDURES_ICD"

    if re.search(rf"\b{target}\b", text, re.IGNORECASE):
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    base_table = m.group(1)
    base_alias = m.group(2) or base_table
    if base_alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER"}:
        base_alias = base_table

    if base_table.upper() not in _tables_with_subject_id():
        return text, rules

    join_clause = f" JOIN {target} d ON {base_alias}.SUBJECT_ID = d.SUBJECT_ID"
    if re.search(r"\bHADM_ID\b", text, re.IGNORECASE):
        join_clause = f" JOIN {target} d ON {base_alias}.SUBJECT_ID = d.SUBJECT_ID AND {base_alias}.HADM_ID = d.HADM_ID"

    text = _insert_join(text, join_clause)
    text = re.sub(r"(?<!\.)\bICD_CODE\b", "d.ICD_CODE", text, flags=re.IGNORECASE)
    rules.append(f"join_{target.lower()}_for_icd_code")
    return text, rules


def _rewrite_admission_length(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"(?<!\.)\b(ADMISSION_LENGTH|ADMISSION_DAYS)\b", text, re.IGNORECASE):
        return text, rules

    alias = _find_table_alias(text, "ADMISSIONS") or "ADMISSIONS"
    replacement = f"CAST({alias}.DISCHTIME AS DATE) - CAST({alias}.ADMITTIME AS DATE)"
    text = re.sub(r"(?<!\.)\b(ADMISSION_LENGTH|ADMISSION_DAYS)\b", replacement, text, flags=re.IGNORECASE)
    rules.append("admission_length_to_date_diff")
    return text, rules


def _rewrite_to_date_cast(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    changed = False

    def repl(match: re.Match) -> str:
        nonlocal changed
        col = match.group(1)
        col_name = col.split(".")[-1].upper()
        if col_name in _timestamp_cols():
            changed = True
            return f"CAST({col} AS DATE)"
        return match.group(0)

    new_text = _TO_DATE_RE.sub(repl, text)
    if changed:
        rules.append("to_date_on_timestamp_to_cast")
    return new_text, rules


def _rewrite_duration(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"(?<!\.)\b(DURATION_DAYS|DURATION)\b", text, re.IGNORECASE):
        return text, rules

    alias = _find_table_alias(text, "TRANSFERS") or "TRANSFERS"
    replacement = f"CAST({alias}.OUTTIME AS DATE) - CAST({alias}.INTIME AS DATE)"
    text = re.sub(r"(?<!\.)\b(DURATION_DAYS|DURATION)\b", replacement, text, flags=re.IGNORECASE)
    rules.append("duration_to_date_diff")
    return text, rules


def _fix_orphan_by(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if "GROUP BY" in text.upper():
        return text, rules
    if not re.search(r"\b(COUNT|AVG|SUM|MIN|MAX)\s*\(", text, re.IGNORECASE):
        return text, rules
    for match in re.finditer(r"\bBY\b", text, re.IGNORECASE):
        prefix = text[:match.start()].upper()
        if prefix.endswith("ORDER "):
            continue
        text = text[:match.start()] + "GROUP BY" + text[match.end():]
        rules.append("orphan_by_to_group_by")
        break
    return text, rules


def _fix_having_where(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if _HAVING_WHERE_RE.search(text):
        text = _HAVING_WHERE_RE.sub("HAVING", text)
        rules.append("fix_having_where")

    new_text = _HAVING_TRUE_RE.sub("", text)
    if new_text != text:
        text = new_text
        rules.append("drop_having_true")
    return text, rules


def _rewrite_hospital_expire_flag(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not _HOSPITAL_EXPIRE_RE.search(text):
        return text, rules
    text = _HOSPITAL_EXPIRE_RE.sub("HOSPITAL_EXPIRE_FLAG = 1", text)
    rules.append("hospital_expire_flag_to_one")
    return text, rules


def _rewrite_age_from_sysdate_diff(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    new_text = _SYSDATE_YEAR_DIFF_RE.sub("ANCHOR_AGE", text)
    if new_text != text:
        rules.append("sysdate_diff_years_to_anchor_age")
    return new_text, rules


def _rewrite_absolute_year_range(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    years = sorted({int(m.group(0)) for m in _ABS_YEAR_RE.finditer(question)})
    if len(years) < 2:
        return text, rules
    start_year = years[0]
    end_year = years[-1]
    if end_year < start_year or (end_year - start_year) > 30:
        return text, rules
    if "ADD_MONTHS" not in text.upper():
        return text, rules

    changed = False

    def _repl(match: re.Match) -> str:
        nonlocal changed
        col = match.group(1)
        op = match.group(2)
        changed = True
        if op in (">=", ">"):
            return f"{col} >= TO_DATE('{start_year}-01-01', 'YYYY-MM-DD')"
        # Upper bound is normalized to strict less-than of next year.
        return f"{col} < TO_DATE('{end_year + 1}-01-01', 'YYYY-MM-DD')"

    new_text = _ADD_MONTHS_PRED_RE.sub(_repl, text)
    if changed:
        rules.append("absolute_year_range_from_question")
    return new_text, rules


def _rewrite_extract_day_diff(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def repl(match: re.Match) -> str:
        expr = match.group(1).strip()
        # If the inner expression is already a date diff, EXTRACT(DAY FROM ...) is unnecessary.
        if "-" in expr:
            return expr
        return match.group(0)

    new_text = _EXTRACT_DAY_RE.sub(repl, text)
    if new_text != text:
        rules.append("extract_day_to_date_diff")
    return new_text, rules


def _rewrite_age_from_anchor(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def repl(match: re.Match) -> str:
        expr = match.group(1)
        if "." in expr:
            alias = expr.split(".")[0]
            return f"{alias}.ANCHOR_AGE"
        return "ANCHOR_AGE"

    new_text = _AGE_FROM_ANCHOR_RE.sub(repl, text)
    if new_text != text:
        rules.append("anchor_year_to_anchor_age")
    return new_text, rules


def _rewrite_age_from_birthdate(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def repl(match: re.Match) -> str:
        expr = match.group(1)
        if "." in expr:
            alias = expr.split(".")[0]
            return f"{alias}.ANCHOR_AGE"
        return "ANCHOR_AGE"

    new_text = _AGE_FROM_BIRTHDATE_RE.sub(repl, text)
    if new_text != text:
        rules.append("birthdate_to_anchor_age")
    return new_text, rules


def _rewrite_birthdate_to_anchor_age(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def repl(match: re.Match[str]) -> str:
        anchor = str(match.group("anchor") or "").strip()
        birth = str(match.group("birth") or "").strip()
        for expr in (anchor, birth):
            if "." in expr:
                alias = expr.split(".", 1)[0]
                if alias:
                    return f"{alias}.ANCHOR_AGE"
        return "ANCHOR_AGE"

    rewritten = _ANCHOR_MINUS_BIRTH_EXTRACT_RE.sub(repl, text)
    if rewritten != text:
        text = rewritten
        rules.append("anchor_minus_birth_extract_to_anchor_age")
    return text, rules


def _rewrite_birth_year_age(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def repl(match: re.Match) -> str:
        return "ANCHOR_AGE"

    new_text = _BIRTH_YEAR_DIFF_RE.sub(repl, text)
    if new_text != text:
        rules.append("birth_year_diff_to_anchor_age")
        text = new_text

    if _BIRTH_YEAR_RE.search(text):
        text = _BIRTH_YEAR_RE.sub("ANCHOR_YEAR", text)
        rules.append("birth_year_to_anchor_year")

    return text, rules


def _normalize_count_aliases(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.match(r"^\s*WITH\b", text, re.IGNORECASE):
        # Keep CTE-local aggregate aliases intact because outer SELECT scopes
        # often reference them by name.
        return text, rules
    if re.search(r"\bFROM\s*\(\s*SELECT\b", text, re.IGNORECASE):
        # Avoid alias rewrite inside derived tables; outer scopes may reference inner aliases.
        return text, rules
    keywords = {"FROM", "WHERE", "GROUP", "ORDER", "HAVING", "JOIN", "UNION", "LIMIT", "FETCH"}
    aliases: list[str] = []

    def repl(match: re.Match) -> str:
        # Skip COUNT aliases that are part of arithmetic expressions such as
        # "... / COUNT(...) AS ratio", otherwise ratio aliases can be rewritten to CNT.
        prev_idx = match.start() - 1
        while prev_idx >= 0 and text[prev_idx].isspace():
            prev_idx -= 1
        if prev_idx >= 0 and text[prev_idx] in {"/", "+", "-", "*", "("}:
            return match.group(0)

        alias = match.group(2)
        if alias.upper() == "CNT" or alias.upper() in keywords:
            return match.group(0)
        aliases.append(alias)
        return match.group(1) + "CNT"

    new_text = _COUNT_ALIAS_RE.sub(repl, text)
    if aliases:
        def fix_order(match: re.Match) -> str:
            clause = match.group(1)
            for old in aliases:
                clause = re.sub(rf"\b{re.escape(old)}\b", "CNT", clause, flags=re.IGNORECASE)
            return clause

        new_text = re.sub(r"(\border\s+by\b[^;]*)", fix_order, new_text, count=1, flags=re.IGNORECASE)
        rules.append("count_alias_to_cnt")
    return new_text, rules


def _is_simple_count_aggregate_sql(sql: str) -> bool:
    upper = sql.upper()
    if upper.count("COUNT(") != 1:
        return False
    blocked_tokens = ("AVG(", "SUM(", "MIN(", "MAX(", "CASE WHEN", "/")
    if any(token in upper for token in blocked_tokens):
        return False
    return True


def _is_simple_avg_aggregate_sql(sql: str) -> bool:
    upper = sql.upper()
    if upper.count("AVG(") != 1:
        return False
    blocked_tokens = ("COUNT(", "SUM(", "MIN(", "MAX(", "CASE WHEN", "/")
    if any(token in upper for token in blocked_tokens):
        return False
    return True


def _normalize_count_aliases_for_simple_counts(sql: str) -> tuple[str, list[str]]:
    if not _is_simple_count_aggregate_sql(sql):
        return sql, []
    return _normalize_count_aliases(sql)


def _ensure_group_by_not_null_for_simple_counts(question: str, sql: str) -> tuple[str, list[str]]:
    if not _is_simple_count_aggregate_sql(sql):
        return sql, []
    return _ensure_group_by_not_null(question, sql)


def _ensure_group_by_not_null_for_simple_avg(question: str, sql: str) -> tuple[str, list[str]]:
    if not _is_simple_avg_aggregate_sql(sql):
        return sql, []
    return _ensure_group_by_not_null(question, sql)


def _rewrite_avg_count_alias(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    aliases_in_order = re.findall(r"\bAS\s+([A-Za-z_][A-Za-z0-9_$#]*)\b", text, flags=re.IGNORECASE)
    projected_aliases = {alias.upper() for alias in aliases_in_order}
    count_like_aliases: list[str] = []
    seen_count_aliases: set[str] = set()
    for alias in aliases_in_order:
        alias_upper = alias.upper()
        if alias_upper in seen_count_aliases:
            continue
        if not _COUNTLIKE_ALIAS_RE.match(alias_upper):
            continue
        seen_count_aliases.add(alias_upper)
        count_like_aliases.append(alias)

    agg_alias_ref_re = re.compile(
        r"\b(?P<fn>AVG|STDDEV)\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_$#]*)\s*\)",
        re.IGNORECASE,
    )

    # If outer aggregate references an alias that is not projected, map it to the
    # single projected count-like alias to avoid ORA-00904.
    if len(count_like_aliases) == 1:
        target_alias = count_like_aliases[0]
        changed = False

        def _repl_agg_alias(match: re.Match[str]) -> str:
            nonlocal changed
            alias = str(match.group("alias") or "").strip()
            alias_upper = alias.upper()
            if alias_upper in projected_aliases:
                return match.group(0)
            if alias_upper == "CNT" or alias_upper.endswith("_COUNT"):
                changed = True
                fn = str(match.group("fn") or "AVG").upper()
                return f"{fn}({target_alias})"
            return match.group(0)

        rewritten = agg_alias_ref_re.sub(_repl_agg_alias, text)
        if changed and rewritten != text:
            text = rewritten
            rules.append("aggregate_alias_to_existing_count_alias")

    # Keep AVG(..._COUNT)->AVG(CNT) normalization only when CNT is explicitly projected.
    if "CNT" in projected_aliases and re.search(
        r"\bAVG\s*\(\s*(diagnosis_count|procedure_count|num_diagnoses|num_procedures|[A-Za-z0-9_]*_count)\s*\)",
        text,
        re.IGNORECASE,
    ):
        rewritten = re.sub(
            r"\bAVG\s*\(\s*(diagnosis_count|procedure_count|num_diagnoses|num_procedures|[A-Za-z0-9_]*_count)\s*\)",
            "AVG(CNT)",
            text,
            flags=re.IGNORECASE,
        )
        if rewritten != text:
            text = rewritten
            rules.append("avg_count_alias_to_cnt")
    return text, rules


def _normalize_avg_aliases(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    alias_map = {
        "DOSES_PER_24_HRS": "avg_doses",
        "AMOUNT": "avg_amount",
        "VALUE": "avg_value",
        "ANCHOR_AGE": "avg_age",
        "LOS": "avg_los",
        "DIAGNOSIS_COUNT": "avg_diag",
        "DIAG_CNT": "avg_diag",
        "PROCEDURE_COUNT": "avg_proc",
        "PROC_CNT": "avg_proc",
    }
    for col, alias in alias_map.items():
        pattern = re.compile(
            rf"AVG\(\s*([A-Za-z0-9_\.]*{col})\s*\)\s+AS\s+[A-Za-z0-9_]+",
            re.IGNORECASE,
        )
        if pattern.search(text):
            text = pattern.sub(lambda m: f"AVG({m.group(1)}) AS {alias}", text)
            rules.append(f"avg_alias_{col.lower()}")
    return text, rules


def _fix_order_by_bad_alias(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if _ORDER_BY_BAD_COUNT_RE.search(text):
        text = _ORDER_BY_BAD_COUNT_RE.sub("ORDER BY CNT", text)
        rules.append("order_by_bad_alias_to_cnt")
    if re.search(r"\bORDER\s+BY\s+CNT\s*\(\s*\*\s*\)\s+CNT\b", text, re.IGNORECASE):
        text = re.sub(r"\bORDER\s+BY\s+CNT\s*\(\s*\*\s*\)\s+CNT\b", "ORDER BY CNT", text, flags=re.IGNORECASE)
        rules.append("order_by_cnt_star")
    if re.search(r"\bORDER\s+BY\s+COUNT\(\*\)\s+CNT\b", text, re.IGNORECASE):
        text = re.sub(r"\bORDER\s+BY\s+COUNT\(\*\)\s+CNT\b", "ORDER BY CNT", text, flags=re.IGNORECASE)
        rules.append("order_by_count_cnt")
    return text, rules


def _fix_order_by_count_suffix(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bORDER\s+BY\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"\bCNT\b", text, re.IGNORECASE):
        return text, rules
    match = re.search(r"\bORDER\s+BY\s+([A-Za-z0-9_]+)(\s+DESC|\s+ASC)?\b", text, re.IGNORECASE)
    if not match:
        return text, rules
    alias = match.group(1)
    direction = match.group(2) or ""
    if alias.upper() != "CNT" and alias.upper().endswith("_COUNT"):
        text = re.sub(
            r"\bORDER\s+BY\s+[A-Za-z0-9_]+(\s+DESC|\s+ASC)?\b",
            f"ORDER BY CNT{direction}",
            text,
            count=1,
            flags=re.IGNORECASE,
        )
        rules.append("order_by_count_suffix_to_cnt")
    return text, rules


def _extract_top_n_from_question(question: str) -> int | None:
    def _parse_pos_int(value: str | None) -> int | None:
        if value is None:
            return None
        raw = str(value).strip().replace(",", "")
        if not raw or not raw.isdigit():
            return None
        try:
            return max(1, int(raw))
        except Exception:
            return None

    q = str(question or "").strip().lower()
    if not q:
        return None
    m = _TOP_N_EN_RE.search(q)
    if m:
        parsed = _parse_pos_int(m.group(1))
        return parsed if parsed is not None else 10
    m = _TOP_N_KO_RE.search(q)
    if m:
        parsed = _parse_pos_int(m.group(1))
        return parsed if parsed is not None else 10
    m = _TOP_N_KO_ONLY_RE.search(q)
    if m:
        parsed = _parse_pos_int(m.group(1))
        return parsed if parsed is not None else 10
    if "top" in q or "상위" in q or "탑" in q:
        return 10
    return None


def _strip_rownum_predicates(sql: str) -> tuple[str, bool]:
    text = sql
    changed = False
    patterns = [
        (r"\bWHERE\s+ROWNUM\s*<=\s*\d+\s+AND\s+", "WHERE "),
        (r"\s+AND\s+ROWNUM\s*<=\s*\d+", ""),
        (r"\bWHERE\s+ROWNUM\s*<=\s*\d+\s+(GROUP\s+BY|ORDER\s+BY|HAVING)\b", r" \1"),
        (r"\bWHERE\s+ROWNUM\s*<=\s*\d+\b", ""),
    ]
    for pattern, repl in patterns:
        updated = re.sub(pattern, repl, text, flags=re.IGNORECASE)
        if updated != text:
            changed = True
            text = updated
    text = re.sub(r"\bWHERE\s+AND\b", "WHERE", text, flags=re.IGNORECASE)
    text = re.sub(r"\s{2,}", " ", text).strip()
    return text, changed


def _strip_unrequested_top_n_cap(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    if _extract_top_n_from_question(question) is not None:
        return sql, rules

    q = str(question or "").lower()
    if any(token in q for token in ("sample", "preview", "샘플", "미리보기", "예시")):
        return sql, rules

    text = str(sql or "").strip().rstrip(";")
    if not text:
        return sql, rules

    def _is_small_topn(value: str) -> bool:
        try:
            limit = int(value)
        except Exception:
            return False
        return 0 < limit <= 200

    outer = _OUTER_ROWNUM_RE.match(text)
    if outer:
        inner = outer.group(1).strip()
        limit = outer.group(2)
        if _is_small_topn(limit) and ("GROUP BY" in inner.upper() or "ORDER BY" in inner.upper()):
            rules.append(f"strip_unrequested_top_n_rownum:{limit}")
            return inner, rules

    if "GROUP BY" in text.upper() or "ORDER BY" in text.upper():
        match = re.search(r"\bROWNUM\s*<=\s*(\d+)\b", text, re.IGNORECASE)
        if match and _is_small_topn(match.group(1)):
            stripped, changed = _strip_rownum_predicates(text)
            if changed:
                rules.append(f"strip_unrequested_top_n_rownum:{match.group(1)}")
                return stripped, rules
    return text, rules


def _enforce_top_n_wrapper(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    n = _extract_top_n_from_question(question)
    if n is None:
        return sql, rules

    text = str(sql or "").strip().rstrip(";")
    if not text:
        return sql, rules

    outer = _OUTER_ROWNUM_RE.match(text)
    if outer:
        inner = outer.group(1).strip()
        try:
            current = int(outer.group(2))
        except Exception:
            current = n
        if current != n:
            rules.append(f"enforce_top_n_rownum:{current}->{n}")
        else:
            rules.append(f"enforce_top_n_rownum:{n}")
        return f"SELECT * FROM ({inner}) WHERE ROWNUM <= {n}", rules

    stripped, stripped_changed = _strip_rownum_predicates(text)
    if stripped_changed:
        text = stripped
        rules.append("strip_rownum_before_top_n")

    if "ORDER BY" not in text.upper():
        return text, rules

    wrapped = _wrap_with_rownum(text, n)
    if wrapped != sql.strip().rstrip(";"):
        rules.append(f"wrap_top_n_rownum:{n}")
    return wrapped, rules


def _apply_monthly_trend_default_cap(question: str, sql: str, default_n: int = 120) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = str(sql or "").strip().rstrip(";")
    if not text or default_n <= 0:
        return sql, rules

    if _extract_top_n_from_question(question) is not None:
        return text, rules
    if not _MONTHLY_TREND_INTENT_RE.search(str(question or "")):
        return text, rules

    upper = text.upper()
    if "GROUP BY" not in upper or "ORDER BY" not in upper:
        return text, rules
    if "ROWNUM" in upper or _LIMIT_RE.search(text) or _FETCH_RE.search(text):
        return text, rules

    has_month_bucket = bool(
        re.search(r"TRUNC\s*\(\s*[^,]+,\s*'MM'\s*\)", text, re.IGNORECASE)
        or re.search(r"TO_CHAR\s*\(\s*[^,]+,\s*'YYYY[-_/]?MM'\s*\)", text, re.IGNORECASE)
        or re.search(r"EXTRACT\s*\(\s*MONTH\s+FROM\s+[^)]+\)", text, re.IGNORECASE)
    )
    if not has_month_bucket:
        return text, rules

    wrapped = _wrap_with_rownum(text, default_n)
    if wrapped != text:
        rules.append(f"default_monthly_trend_cap:{default_n}")
    return wrapped, rules


def _strip_first_icu_rownum_for_careunit_counts(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = str(sql or "").strip().rstrip(";")
    if not text:
        return sql, rules

    q = str(question or "").lower()
    first_careunit_intent = bool(
        re.search(r"(first\s+care\s*unit|first\s+careunit|icu\s+stays\s+by\s+first\s+careunit|첫\s*careunit|첫\s*병동)", q)
    )
    if not first_careunit_intent:
        return text, rules
    if _FIRST_ICU_INTENT_RE.search(q):
        return text, rules

    upper = text.upper()
    if "ICUSTAYS" not in upper or "ROW_NUMBER(" not in upper:
        return text, rules
    if "GROUP BY" not in upper or "FIRST_CAREUNIT" not in upper:
        return text, rules
    if not re.search(r"\b(?:[A-Za-z0-9_]+\.)?(?:RN_FIRST_ICU|RN)\s*=\s*1\b", text, re.IGNORECASE):
        return text, rules

    rewritten = text
    rewritten = re.sub(
        r"\bWHERE\s+\(*\s*(?:[A-Za-z0-9_]+\.)?(?:RN_FIRST_ICU|RN)\s*=\s*1\s*\)*\s+AND\s+",
        "WHERE ",
        rewritten,
        flags=re.IGNORECASE,
    )
    rewritten = re.sub(
        r"\bAND\s+\(*\s*(?:[A-Za-z0-9_]+\.)?(?:RN_FIRST_ICU|RN)\s*=\s*1\s*\)*",
        "",
        rewritten,
        flags=re.IGNORECASE,
    )
    rewritten = re.sub(
        r"\bWHERE\s+\(*\s*(?:[A-Za-z0-9_]+\.)?(?:RN_FIRST_ICU|RN)\s*=\s*1\s*\)*\s*(?=\bGROUP\b|\bORDER\b|\bHAVING\b|$)",
        "",
        rewritten,
        flags=re.IGNORECASE,
    )
    rewritten = re.sub(r"\bWHERE\s*(?=\bGROUP\b|\bORDER\b|\bHAVING\b|$)", "", rewritten, flags=re.IGNORECASE)
    rewritten = re.sub(r"\s{2,}", " ", rewritten).strip()
    if rewritten != text:
        rules.append("strip_first_icu_rownum_for_careunit_counts")
        return rewritten, rules
    return text, rules


def _apply_first_careunit_default_cap(question: str, sql: str, default_n: int = 10) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = str(sql or "").strip().rstrip(";")
    if not text or default_n <= 0:
        return sql, rules
    if _extract_top_n_from_question(question) is not None:
        return text, rules

    q = str(question or "").lower()
    first_careunit_intent = bool(
        re.search(r"(icu\s+stays\s+by\s+first\s+careunit|first\s+care\s*unit|first\s+careunit|첫\s*병동|첫\s*careunit)", q)
    )
    if not first_careunit_intent:
        return text, rules

    upper = text.upper()
    if "ROWNUM" in upper or _LIMIT_RE.search(text) or _FETCH_RE.search(text):
        return text, rules
    if "ICUSTAYS" not in upper or "GROUP BY" not in upper or "ORDER BY" not in upper:
        return text, rules
    if "FIRST_CAREUNIT" not in upper or "COUNT(" not in upper:
        return text, rules

    wrapped = _wrap_with_rownum(text, default_n)
    if wrapped != text:
        rules.append(f"default_first_careunit_cap:{default_n}")
    return wrapped, rules


def _append_where_predicate(sql: str, predicate: str) -> str:
    text = str(sql or "").strip().rstrip(";")
    if not text or not predicate:
        return text
    span = _find_final_select_from_span(text)
    if not span:
        return f"{text} WHERE {predicate}"
    core, _, from_idx = span
    where_idx = _find_first_top_level_keyword(core, from_idx, ("WHERE",))
    clause_idx = _find_first_top_level_keyword(core, from_idx, ("GROUP BY", "HAVING", "ORDER BY"))
    if where_idx >= 0 and (clause_idx < 0 or where_idx < clause_idx):
        if clause_idx >= 0:
            head = core[:clause_idx].rstrip()
            tail = core[clause_idx:]
            return f"{head} AND {predicate} {tail}".strip()
        return f"{core} AND {predicate}".strip()
    if clause_idx >= 0:
        head = core[:clause_idx].rstrip()
        tail = core[clause_idx:]
        return f"{head} WHERE {predicate} {tail}".strip()
    return f"{core} WHERE {predicate}".strip()


def _collect_table_aliases(sql: str) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for match in re.finditer(
        r"\b(?:FROM|JOIN)\s+([A-Za-z0-9_]+)(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?",
        str(sql or ""),
        re.IGNORECASE,
    ):
        table = str(match.group(1) or "").strip().upper()
        alias = str(match.group(2) or "").strip().upper()
        if not table:
            continue
        if not alias or alias in {"ON", "WHERE", "GROUP", "ORDER", "INNER", "LEFT", "RIGHT", "FULL", "JOIN"}:
            alias = table
        aliases[alias] = table
    return aliases


def _ensure_hadm_not_null_for_distinct_counts(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = str(sql or "").strip()
    if not text:
        return text, rules
    if re.match(r"^\s*WITH\b", text, re.IGNORECASE):
        # Appending a single predicate to the outer query can reference aliases
        # that exist only inside CTEs and cause ORA-00904.
        return text, rules

    alias_tables = _collect_table_aliases(text)
    aliases = {
        str(match.group(1) or "").strip().upper()
        for match in re.finditer(
            r"\bCOUNT\s*\(\s*DISTINCT\s+([A-Za-z0-9_]+)\.HADM_ID\s*\)",
            text,
            re.IGNORECASE,
        )
    }
    if not aliases:
        return text, rules

    target_tables = {
        "ADMISSIONS",
        "SERVICES",
        "PRESCRIPTIONS",
        "DIAGNOSES_ICD",
        "PROCEDURES_ICD",
        "ICUSTAYS",
        "TRANSFERS",
        "PROCEDUREEVENTS",
        "CHARTEVENTS",
        "LABEVENTS",
        "MICROBIOLOGYEVENTS",
    }
    for alias in sorted(aliases):
        table = alias_tables.get(alias, alias)
        if table not in target_tables:
            continue
        predicate = f"{alias}.HADM_ID IS NOT NULL"
        if re.search(rf"\b{re.escape(alias)}\s*\.\s*HADM_ID\s+IS\s+NOT\s+NULL\b", text, re.IGNORECASE):
            continue
        updated = _append_where_predicate(text, predicate)
        if updated != text:
            text = updated
            rules.append(f"hadm_not_null_distinct:{alias}")

    return text, rules


def _rewrite_prescriptions_hadm_count_to_admissions_exists(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    q = str(question or "").lower()
    if not any(token in q for token in ("입원", "admission", "hospitalization", "inpatient", "during admission")):
        return sql, rules

    text = str(sql or "").strip()
    if not text:
        return sql, rules
    if re.search(r"\bADMISSIONS\b", text, re.IGNORECASE):
        return sql, rules
    if not re.search(r"\bFROM\s+PRESCRIPTIONS\b", text, re.IGNORECASE):
        return sql, rules
    if not re.search(r"\bCOUNT\s*\(\s*DISTINCT\s+[A-Za-z0-9_]+\.HADM_ID\s*\)", text, re.IGNORECASE):
        return sql, rules
    if re.search(r"\bGROUP\s+BY\b|\bHAVING\b", text, re.IGNORECASE):
        return sql, rules
    if not _is_single_count_distinct_hadm_projection(text):
        return sql, rules

    from_match = re.search(
        r"\bFROM\s+PRESCRIPTIONS(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?",
        text,
        re.IGNORECASE,
    )
    if not from_match:
        return sql, rules
    alias = str(from_match.group(1) or "PRESCRIPTIONS").strip()
    alias_upper = alias.upper()
    if alias_upper in {"WHERE", "GROUP", "ORDER", "HAVING", "JOIN", "ON"}:
        alias = "PRESCRIPTIONS"

    where_match = re.search(
        r"\bWHERE\b(?P<body>.*?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bHAVING\b|$)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    where_body = str(where_match.group("body") or "").strip() if where_match else ""
    if not where_body:
        return sql, rules

    hadm_not_null_pattern = rf"(?:\bAND\s+)?\b{re.escape(alias)}\s*\.\s*HADM_ID\s+IS\s+NOT\s+NULL\b(?:\s+AND)?"
    cleaned_where = re.sub(hadm_not_null_pattern, " ", where_body, flags=re.IGNORECASE)
    cleaned_where = re.sub(r"\bAND\s+AND\b", "AND", cleaned_where, flags=re.IGNORECASE)
    cleaned_where = re.sub(r"^\s*AND\s+", "", cleaned_where, flags=re.IGNORECASE)
    cleaned_where = re.sub(r"\s+AND\s*$", "", cleaned_where, flags=re.IGNORECASE)
    cleaned_where = re.sub(r"\s{2,}", " ", cleaned_where).strip()

    exists_predicates = [f"{alias}.HADM_ID = a.HADM_ID"]
    if cleaned_where:
        exists_predicates.append(cleaned_where)
    exists_clause = " AND ".join(exists_predicates)
    rewritten = (
        "SELECT COUNT(DISTINCT a.HADM_ID) AS CNT "
        "FROM ADMISSIONS a "
        f"WHERE EXISTS (SELECT 1 FROM PRESCRIPTIONS {alias} WHERE {exists_clause})"
    )
    rules.append("prescriptions_hadm_count_to_admissions_exists")
    return rewritten, rules


def _ensure_prescriptions_hadm_not_null_for_grouping(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = str(sql or "").strip()
    if not text:
        return text, rules
    if not re.search(r"\bPRESCRIPTIONS\b", text, re.IGNORECASE):
        return text, rules

    alias_matches = list(
        re.finditer(
            r"\bFROM\s+PRESCRIPTIONS(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?",
            text,
            re.IGNORECASE,
        )
    )
    for match in alias_matches:
        alias = str(match.group(1) or "PRESCRIPTIONS").strip()
        alias_upper = alias.upper()
        if alias_upper in {"WHERE", "GROUP", "ORDER", "HAVING", "JOIN", "ON"}:
            alias = "PRESCRIPTIONS"
            alias_upper = "PRESCRIPTIONS"
        if not re.search(rf"\bGROUP\s+BY\b[\s\S]*\b{re.escape(alias)}\s*\.\s*HADM_ID\b", text, re.IGNORECASE):
            continue
        if re.search(rf"\b{re.escape(alias)}\s*\.\s*HADM_ID\s+IS\s+NOT\s+NULL\b", text, re.IGNORECASE):
            continue

        where_group_pattern = re.compile(
            rf"(\bFROM\s+PRESCRIPTIONS(?:\s+(?:AS\s+)?{re.escape(alias)})?\s+\bWHERE\b\s*)(?P<body>.*?)(\bGROUP\s+BY\b)",
            re.IGNORECASE | re.DOTALL,
        )

        def repl_where_group(found: re.Match[str]) -> str:
            body = str(found.group("body") or "").rstrip()
            if body:
                body = f"{body} AND {alias}.HADM_ID IS NOT NULL "
            else:
                body = f"{alias}.HADM_ID IS NOT NULL "
            return f"{found.group(1)}{body}{found.group(3)}"

        updated = where_group_pattern.sub(repl_where_group, text, count=1)
        if updated != text:
            text = updated
            rules.append(f"prescriptions_hadm_not_null_group:{alias_upper}")
            continue

        from_group_pattern = re.compile(
            rf"(\bFROM\s+PRESCRIPTIONS(?:\s+(?:AS\s+)?{re.escape(alias)})?\s*)(\bGROUP\s+BY\b)",
            re.IGNORECASE,
        )
        updated = from_group_pattern.sub(
            rf"\1WHERE {alias}.HADM_ID IS NOT NULL \2",
            text,
            count=1,
        )
        if updated != text:
            text = updated
            rules.append(f"prescriptions_hadm_not_null_group:{alias_upper}")

    return text, rules


def _rewrite_services_hadm_count_to_admissions_join(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    q = str(question or "").lower()
    if not any(token in q for token in ("입원", "admission", "hospitalization", "inpatient")):
        return sql, rules

    text = str(sql or "").strip()
    if not text:
        return sql, rules
    if re.search(r"\bADMISSIONS\b", text, re.IGNORECASE):
        return sql, rules
    if not re.search(r"\bFROM\s+SERVICES\b", text, re.IGNORECASE):
        return sql, rules

    from_match = re.search(
        r"\bFROM\s+SERVICES(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?",
        text,
        re.IGNORECASE,
    )
    if not from_match:
        return sql, rules
    alias = str(from_match.group(1) or "SERVICES").strip()
    alias_upper = alias.upper()
    if alias_upper in {"WHERE", "GROUP", "ORDER", "HAVING", "JOIN", "ON"}:
        alias = "SERVICES"
        alias_upper = "SERVICES"

    if not re.search(rf"\bCOUNT\s*\(\s*DISTINCT\s+{re.escape(alias)}\s*\.\s*HADM_ID\s*\)", text, re.IGNORECASE):
        if not re.search(r"\bCOUNT\s*\(\s*DISTINCT\s+HADM_ID\s*\)", text, re.IGNORECASE):
            return sql, rules
    if re.search(r"\bGROUP\s+BY\b|\bHAVING\b", text, re.IGNORECASE):
        return sql, rules
    if not _is_single_count_distinct_hadm_projection(text):
        return sql, rules

    where_match = re.search(
        r"\bWHERE\b(?P<body>.*?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bHAVING\b|$)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    where_body = str(where_match.group("body") or "").strip() if where_match else ""
    if where_body:
        if alias_upper != "S":
            where_body = re.sub(
                rf"\b{re.escape(alias)}\s*\.",
                "s.",
                where_body,
                flags=re.IGNORECASE,
            )
        where_clause = f" WHERE {where_body}"
    else:
        where_clause = ""

    rewritten = (
        "SELECT COUNT(DISTINCT a.HADM_ID) AS CNT "
        "FROM ADMISSIONS a "
        "JOIN SERVICES s ON s.HADM_ID = a.HADM_ID"
        f"{where_clause}"
    )
    rules.append("services_hadm_count_to_admissions_join")
    return rewritten, rules


def _rewrite_service_mortality_query(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    q = str(question or "").strip().lower()
    text = str(sql or "").strip()
    if not text:
        return sql, rules
    if not _SERVICE_STRATIFY_INTENT_RE.search(q):
        return sql, rules
    if _ADMISSION_TYPE_QUERY_INTENT_RE.search(q):
        return sql, rules
    if _DIAG_PROC_QUERY_INTENT_RE.search(q):
        return sql, rules
    if not (_MORTALITY_QUERY_INTENT_RE.search(q) or _RATIO_INTENT_RE.search(q)):
        return sql, rules

    # Skip when SQL already references service columns correctly.
    if re.search(r"\b(SERVICES|CURR_SERVICE|PREV_SERVICE)\b", text, re.IGNORECASE):
        return sql, rules

    # Rewrite only when explicit semantic drift is visible in SQL.
    # Avoid broad canonical rewrites for merely incomplete drafts.
    has_admission_type_ref = bool(re.search(r"\bADMISSION_TYPE\b", text, re.IGNORECASE))
    has_diag_proc_ref = bool(re.search(r"\b(DIAGNOSES_ICD|PROCEDURES_ICD)\b", text, re.IGNORECASE))
    if not (has_admission_type_ref or has_diag_proc_ref):
        return sql, rules

    prev_service_requested = bool(
        re.search(r"(prev(?:ious)?\s*service|prior\s*service|prev_service|이전\s*진료과|직전\s*진료과|과거\s*진료과)", q, re.IGNORECASE)
    )
    service_col = "PREV_SERVICE" if prev_service_requested else "CURR_SERVICE"
    icu_intent = bool(_ICU_QUERY_INTENT_RE.search(q))

    if icu_intent:
        rewritten = (
            f"SELECT s.{service_col} AS service_group, "
            "COUNT(DISTINCT a.HADM_ID) AS total_admissions, "
            "COUNT(DISTINCT CASE WHEN a.DEATHTIME IS NOT NULL "
            "AND i.INTIME IS NOT NULL AND i.OUTTIME IS NOT NULL "
            "AND a.DEATHTIME BETWEEN i.INTIME AND i.OUTTIME THEN a.HADM_ID END) AS icu_deaths, "
            f"ROUND(100 * COUNT(DISTINCT CASE WHEN a.DEATHTIME IS NOT NULL "
            "AND i.INTIME IS NOT NULL AND i.OUTTIME IS NOT NULL "
            "AND a.DEATHTIME BETWEEN i.INTIME AND i.OUTTIME THEN a.HADM_ID END) "
            "/ NULLIF(COUNT(DISTINCT a.HADM_ID), 0), 2) AS icu_mortality_rate_pct "
            "FROM SERVICES s "
            "JOIN ADMISSIONS a ON a.HADM_ID = s.HADM_ID "
            "JOIN ICUSTAYS i ON i.HADM_ID = a.HADM_ID AND i.SUBJECT_ID = a.SUBJECT_ID "
            f"WHERE s.{service_col} IS NOT NULL "
            f"GROUP BY s.{service_col} "
            "ORDER BY icu_mortality_rate_pct DESC"
        )
        rules.append(f"service_mortality_rewrite:{service_col.lower()}:icu")
        return rewritten, rules

    rewritten = (
        f"SELECT s.{service_col} AS service_group, "
        "COUNT(DISTINCT a.HADM_ID) AS total_admissions, "
        "COUNT(DISTINCT CASE WHEN a.DEATHTIME IS NOT NULL "
        "AND (a.DISCHTIME IS NULL OR a.DEATHTIME <= a.DISCHTIME) THEN a.HADM_ID END) AS deaths, "
        f"ROUND(100 * COUNT(DISTINCT CASE WHEN a.DEATHTIME IS NOT NULL "
        "AND (a.DISCHTIME IS NULL OR a.DEATHTIME <= a.DISCHTIME) THEN a.HADM_ID END) "
        "/ NULLIF(COUNT(DISTINCT a.HADM_ID), 0), 2) AS hospital_mortality_rate_pct "
        "FROM SERVICES s "
        "JOIN ADMISSIONS a ON a.HADM_ID = s.HADM_ID "
        f"WHERE s.{service_col} IS NOT NULL "
        f"GROUP BY s.{service_col} "
        "ORDER BY hospital_mortality_rate_pct DESC"
    )
    rules.append(f"service_mortality_rewrite:{service_col.lower()}:hospital")
    return rewritten, rules


def _rewrite_icu_mortality_outcome_alignment(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    q = str(question or "").strip().lower()
    text = str(sql or "").strip()
    if not text:
        return sql, rules
    if not (_ICU_QUERY_INTENT_RE.search(q) and _MORTALITY_QUERY_INTENT_RE.search(q)):
        return sql, rules
    upper = text.upper()
    if "ADMISSIONS" not in upper:
        return sql, rules
    span = _find_final_select_from_span(text)
    if not span:
        return sql, rules
    core, select_idx, _ = span
    final_query = core[select_idx:]
    final_upper = final_query.upper()
    if "HOSPITAL_EXPIRE_FLAG" not in final_upper:
        return sql, rules
    if "DEATHTIME" in final_upper and "INTIME" in final_upper and "OUTTIME" in final_upper:
        return sql, rules

    def _find_column_ref(fragment: str, column: str) -> str | None:
        aliased = re.search(rf"\b([A-Za-z0-9_]+)\.{column}\b", fragment, re.IGNORECASE)
        if aliased:
            return f"{aliased.group(1)}.{column.upper()}"
        if re.search(rf"\b{column}\b", fragment, re.IGNORECASE):
            return column.upper()
        return None

    intime_ref = _find_column_ref(final_query, "INTIME")
    outtime_ref = _find_column_ref(final_query, "OUTTIME")
    if not (intime_ref and outtime_ref):
        icu_alias = _find_table_alias(final_query, "ICUSTAYS")
        if icu_alias:
            intime_ref = f"{icu_alias}.INTIME"
            outtime_ref = f"{icu_alias}.OUTTIME"
        else:
            # Avoid introducing invalid aliases when ICU timing columns are not available
            # in the final query scope (e.g., hidden inside a CTE).
            return sql, rules

    adm_alias = _find_table_alias(final_query, "ADMISSIONS")
    death_ref = f"{adm_alias}.DEATHTIME" if adm_alias else "DEATHTIME"
    aligned_pred = (
        f"{death_ref} IS NOT NULL "
        f"AND {intime_ref} IS NOT NULL "
        f"AND {outtime_ref} IS NOT NULL "
        f"AND {death_ref} BETWEEN {intime_ref} AND {outtime_ref}"
    )

    case_pattern = re.compile(
        rf"CASE\s+WHEN\s+(?:[A-Za-z0-9_]+\.)?HOSPITAL_EXPIRE_FLAG\s*=\s*1\s+THEN\s+1\s+ELSE\s+0\s+END",
        re.IGNORECASE,
    )
    rewritten = case_pattern.sub(f"CASE WHEN {aligned_pred} THEN 1 ELSE 0 END", text)
    rewritten = re.sub(
        r"(?:[A-Za-z0-9_]+\.)?HOSPITAL_EXPIRE_FLAG\s*=\s*1",
        aligned_pred,
        rewritten,
        flags=re.IGNORECASE,
    )
    if rewritten != text:
        rules.append("icu_mortality_hospital_expire_to_deathtime_alignment")
        return rewritten, rules
    return text, rules


def _rewrite_unrequested_first_icu_window(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    q = str(question or "").strip().lower()
    text = str(sql or "").strip()
    if not text:
        return sql, rules
    if _FIRST_ICU_INTENT_RE.search(q):
        return sql, rules

    if not re.search(r"\bICUSTAYS\b", text, re.IGNORECASE):
        return sql, rules
    if not re.search(r"\bROW_NUMBER\s*\(", text, re.IGNORECASE):
        return sql, rules
    if not re.search(
        r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(\s*PARTITION\s+BY\s+[^)]*SUBJECT_ID[^)]*ORDER\s+BY\s+[A-Za-z0-9_\.]*INTIME",
        text,
        re.IGNORECASE,
    ):
        return sql, rules
    if not re.search(r"\b(?:[A-Za-z0-9_]+\.)?(?:RN_FIRST_ICU|RN)\s*=\s*1\b", text, re.IGNORECASE):
        return sql, rules

    rewritten = text
    rewritten = re.sub(
        r"\bWHERE\s+\(*\s*(?:[A-Za-z0-9_]+\.)?(?:RN_FIRST_ICU|RN)\s*=\s*1\s*\)*\s+AND\s+",
        "WHERE ",
        rewritten,
        flags=re.IGNORECASE,
    )
    rewritten = re.sub(
        r"\bAND\s+\(*\s*(?:[A-Za-z0-9_]+\.)?(?:RN_FIRST_ICU|RN)\s*=\s*1\s*\)*",
        "",
        rewritten,
        flags=re.IGNORECASE,
    )
    rewritten = re.sub(
        r"\bWHERE\s+\(*\s*(?:[A-Za-z0-9_]+\.)?(?:RN_FIRST_ICU|RN)\s*=\s*1\s*\)*\s*(?=\bGROUP\b|\bORDER\b|\bHAVING\b|$)",
        "",
        rewritten,
        flags=re.IGNORECASE,
    )
    rewritten = re.sub(r"\bWHERE\s*(?=\bGROUP\b|\bORDER\b|\bHAVING\b|$)", "", rewritten, flags=re.IGNORECASE)
    rewritten = re.sub(r"\s{2,}", " ", rewritten).strip()

    if rewritten != text:
        rules.append("remove_unrequested_first_icu_filter")
        return rewritten, rules
    return text, rules


def _rewrite_admissions_icd_count_grain(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    q = str(question or "").lower()
    if "count" not in q or "admission" not in q:
        return sql, rules
    if "code" not in q or ("diagnos" not in q and "진단" not in q and "procedur" not in q and "시술" not in q):
        return sql, rules

    text = str(sql or "").strip()
    if not text:
        return sql, rules
    upper = text.upper()
    if "GROUP BY" in upper:
        return sql, rules
    if "ADMISSIONS" not in upper:
        return sql, rules

    target_table = "DIAGNOSES_ICD" if ("diagnos" in q or "진단" in q) else "PROCEDURES_ICD"
    if target_table not in upper:
        return sql, rules

    adm_alias = _find_table_alias(text, "ADMISSIONS") or "ADMISSIONS"
    icd_alias = _find_table_alias(text, target_table) or target_table

    count_rewritten = re.sub(
        r"\bCOUNT\s*\(\s*(?:\*|[A-Za-z0-9_\.]+)\s*\)\s*(?:AS\s+[A-Za-z0-9_]+)?",
        f"COUNT(DISTINCT {adm_alias}.HADM_ID) AS CNT ",
        text,
        count=1,
        flags=re.IGNORECASE,
    )
    if count_rewritten != text:
        text = count_rewritten
        rules.append("admissions_icd_count_distinct_hadm")

    hadm_join_re = re.compile(
        rf"\b{re.escape(adm_alias)}\s*\.\s*HADM_ID\s*=\s*{re.escape(icd_alias)}\s*\.\s*HADM_ID\b"
        rf"|\b{re.escape(icd_alias)}\s*\.\s*HADM_ID\s*=\s*{re.escape(adm_alias)}\s*\.\s*HADM_ID\b",
        re.IGNORECASE,
    )
    if not hadm_join_re.search(text):
        text = _append_where_predicate(text, f"{adm_alias}.HADM_ID = {icd_alias}.HADM_ID")
        rules.append("admissions_icd_join_add_hadm_id")

    if not re.search(r"\bICD_CODE\s+IS\s+NOT\s+NULL\b", text, re.IGNORECASE):
        text = _append_where_predicate(text, f"{icd_alias}.ICD_CODE IS NOT NULL")
        rules.append("admissions_icd_require_code_not_null")

    text = re.sub(r"\s{2,}", " ", text).strip()
    return text, rules


def _infer_gender_count_target(question: str) -> tuple[str, str] | None:
    q = str(question or "").lower()
    mapping: list[tuple[tuple[str, ...], tuple[str, str]]] = [
        (("diagnos", "진단"), ("DIAGNOSES_ICD", "dx")),
        (("procedur", "시술", "수술"), ("PROCEDURES_ICD", "pr")),
        (("transfer", "이동"), ("TRANSFERS", "t")),
        (("service", "서비스"), ("SERVICES", "s")),
        (("prescription", "약물", "처방", "drug", "medication"), ("PRESCRIPTIONS", "r")),
        (("chart event", "chart", "차트"), ("CHARTEVENTS", "c")),
        (("lab event", "lab", "검사"), ("LABEVENTS", "l")),
        (("icu",), ("ICUSTAYS", "i")),
        (("admission", "입원"), ("ADMISSIONS", "a")),
    ]
    for tokens, target in mapping:
        if any(token in q for token in tokens):
            return target
    return None


def _rewrite_count_by_gender_template(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    q = str(question or "").strip()
    if not q:
        return sql, rules
    q_lower = q.lower()
    by_gender_intent = bool(_COUNT_BY_GENDER_EN_RE.search(q_lower) or _COUNT_BY_GENDER_KO_RE.search(q))
    if not by_gender_intent:
        return sql, rules
    if any(token in q_lower for token in ("rate", "ratio", "평균", "비율", "median", "중앙", "중위")):
        return sql, rules

    text = str(sql or "").strip()
    upper = text.upper()
    if "COUNT(" not in upper or "GENDER" not in upper:
        return sql, rules

    target = _infer_gender_count_target(q)
    if not target:
        return sql, rules
    target_table, target_alias = target
    suspicious = (
        "CUSTOMER" in upper
        or "ACCOUNT" in upper
        or " ICU_ADMISSION_DATE" in upper
        or "PATIENTS.ID" in upper
        or target_table not in upper
        or "PATIENTS" not in upper
    )
    if not suspicious:
        return sql, rules

    rewritten = (
        f"SELECT p.GENDER, COUNT(*) AS CNT "
        f"FROM {target_table} {target_alias} "
        f"JOIN PATIENTS p ON {target_alias}.SUBJECT_ID = p.SUBJECT_ID "
        f"WHERE p.GENDER IS NOT NULL "
        f"GROUP BY p.GENDER "
        f"ORDER BY CNT DESC"
    )
    rules.append(f"count_by_gender_template:{target_table}")
    return rewritten, rules


def _rewrite_age_group_diagnosis_extrema_by_gender(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    q = str(question or "").strip()
    if not q:
        return sql, rules
    if not (
        _AGE_GROUP_INTENT_RE.search(q)
        and _GENDER_INTENT_RE.search(q)
        and _EXTREMA_INTENT_RE.search(q)
        and _DIAGNOSIS_INTENT_RE.search(q)
    ):
        return sql, rules

    text = str(sql or "").strip().rstrip(";")
    upper = text.upper()
    if "PATIENTS" not in upper or "DIAGNOSES_ICD" not in upper or "COUNT(" not in upper:
        return sql, rules
    if re.search(r"\bPARTITION\s+BY\b[^\n;]*\bAGE_GROUP\b", upper, re.IGNORECASE):
        return sql, rules

    span = _find_final_select_from_span(text)
    if not span:
        return sql, rules
    core, select_idx, from_idx = span
    select_clause = core[select_idx + len("SELECT"):from_idx].strip()

    agg_match = re.search(
        r"\b(?P<agg>MAX|MIN)\s*\(\s*(?P<metric>[A-Za-z_][A-Za-z0-9_$#\.]*)\s*\)",
        select_clause,
        re.IGNORECASE,
    )
    if not agg_match:
        return sql, rules

    metric = str(agg_match.group("metric") or "").split(".")[-1].strip()
    if not _IDENT_RE.fullmatch(metric):
        return sql, rules

    source_start = from_idx + len("FROM")
    while source_start < len(core) and core[source_start].isspace():
        source_start += 1
    if source_start >= len(core) or core[source_start] != "(":
        return sql, rules
    source_end = _find_matching_paren_index(core, source_start)
    if source_end is None:
        return sql, rules

    inner_sql = core[source_start + 1:source_end].strip().rstrip(";")
    if not inner_sql.upper().startswith("SELECT"):
        return sql, rules
    if not re.search(r"\bAS\s+AGE_GROUP\b", inner_sql, re.IGNORECASE):
        return sql, rules
    if not re.search(r"\bGENDER\b", inner_sql, re.IGNORECASE):
        return sql, rules
    if not re.search(
        rf"\bAS\s+{re.escape(metric)}\b|\b{re.escape(metric)}\b",
        inner_sql,
        re.IGNORECASE,
    ):
        return sql, rules

    outer_tail = core[source_end + 1:]
    group_match = re.search(
        r"\bGROUP\s+BY\b\s+(.+?)(?:\bORDER\s+BY\b|$)",
        outer_tail,
        re.IGNORECASE | re.DOTALL,
    )
    if group_match:
        group_clause = group_match.group(1)
        if not re.search(r"\bGENDER\b", group_clause, re.IGNORECASE):
            return sql, rules
        if re.search(r"\bAGE_GROUP\b|\bANCHOR_AGE\b", group_clause, re.IGNORECASE):
            return sql, rules

    order_dir = "ASC" if agg_match.group("agg").upper() == "MIN" else "DESC"
    rewritten = (
        "WITH age_gender_counts AS ("
        f"{inner_sql}"
        "), ranked AS ("
        f"SELECT age_group, gender, {metric} AS {metric}, "
        f"DENSE_RANK() OVER (PARTITION BY age_group ORDER BY {metric} {order_dir}) AS age_group_rank "
        "FROM age_gender_counts"
        ") "
        f"SELECT age_group, gender, {metric} FROM ranked "
        "WHERE age_group_rank = 1 "
        f"ORDER BY age_group, {metric} {order_dir}"
    )
    rules.append(f"age_group_diagnosis_extrema_by_gender:{order_dir.lower()}")
    return rewritten, rules


def _strip_for_update(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if _FOR_UPDATE_RE.search(text):
        text = _FOR_UPDATE_RE.sub("", text)
        rules.append("strip_for_update")
    return text, rules


def _wrap_top_n(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bROWNUM\b", text, re.IGNORECASE) or _LIMIT_RE.search(text) or _FETCH_RE.search(text):
        return text, rules

    q = question.lower()
    match = re.search(r"\btop\s+(\d+)\b", q)
    if not match and not any(k in q for k in ("top", "most", "highest")):
        return text, rules
    n = int(match.group(1)) if match else 10
    if n <= 0:
        return text, rules

    text = _wrap_with_rownum(text, n)
    rules.append(f"wrap_top_{n}_rownum")
    return text, rules


def _reorder_count_select(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    pattern = re.compile(
        r"^\s*SELECT\s+COUNT\(\*\)\s+AS\s+CNT\s*,\s*([A-Za-z0-9_\.]+)\s+FROM",
        re.IGNORECASE,
    )
    match = pattern.search(text)
    if not match:
        return text, rules
    col = match.group(1)
    text = pattern.sub(f"SELECT {col}, COUNT(*) AS CNT FROM", text, count=1)
    rules.append("reorder_count_select")
    return text, rules


def _reorder_avg_select(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    pattern = re.compile(
        r"^\s*SELECT\s+AVG\(\s*([A-Za-z0-9_\.]+)\s*\)\s+AS\s+([A-Za-z0-9_]+)\s*,\s*([A-Za-z0-9_\.]+)\s+FROM",
        re.IGNORECASE,
    )
    match = pattern.search(text)
    if not match:
        return text, rules
    avg_expr = match.group(1)
    avg_alias = match.group(2)
    col = match.group(3)
    text = pattern.sub(f"SELECT {col}, AVG({avg_expr}) AS {avg_alias} FROM", text, count=1)
    rules.append("reorder_avg_select")
    return text, rules


def _ensure_avg_not_null(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bFROM\s*\(\s*SELECT\b", text, re.IGNORECASE):
        # Avoid injecting predicates into inner GROUP BY blocks of derived tables.
        return text, rules
    avg_exprs: list[str] = []

    for match in re.finditer(r"AVG\s*\(\s*([A-Za-z0-9_\.]+)\s*\)", text, re.IGNORECASE):
        expr = match.group(1)
        avg_exprs.append(expr)

    if not avg_exprs:
        return text, rules

    for expr in avg_exprs:
        col = expr.split(".")[-1]
        if re.search(rf"\b{re.escape(expr)}\b\s+IS\s+NOT\s+NULL", text, re.IGNORECASE):
            continue
        if re.search(rf"(?<!\.)\b{re.escape(col)}\b\s+IS\s+NOT\s+NULL", text, re.IGNORECASE):
            continue

        predicate = f"{expr} IS NOT NULL"
        if re.search(r"\bwhere\b", text, re.IGNORECASE):
            text = re.sub(r"\bwhere\b", f"WHERE {predicate} AND", text, count=1, flags=re.IGNORECASE)
        elif re.search(r"\bgroup\s+by\b", text, re.IGNORECASE):
            text = re.sub(r"\bgroup\s+by\b", f"WHERE {predicate} GROUP BY", text, count=1, flags=re.IGNORECASE)
        elif re.search(r"\border\s+by\b", text, re.IGNORECASE):
            text = re.sub(r"\border\s+by\b", f"WHERE {predicate} ORDER BY", text, count=1, flags=re.IGNORECASE)
        else:
            text = text.rstrip(";") + f" WHERE {predicate}"
        rules.append(f"avg_not_null_{col.lower()}")

    return text, rules


def _strip_transfers_eventtype_filter(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bTRANSFERS\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    explicit_eventtype_intent = any(
        token in q
        for token in (
            "event type",
            "eventtype",
            "이벤트 유형",
            "이벤트 타입",
            "이벤트 종류",
            "전입/전출 유형",
            "전입",
            "전출",
            "전원",
            "admit",
            "discharge",
        )
    )
    if explicit_eventtype_intent:
        return text, rules

    column_pattern = r"(?:UPPER\s*\(\s*)?(?:[A-Za-z0-9_]+\.)?EVENTTYPE(?:\s*\))?"
    value_pattern = r"'TRANSFERS'"
    predicate_pattern = rf"{column_pattern}\s*=\s*{value_pattern}"

    text = re.sub(
        rf"\bWHERE\s+{predicate_pattern}\s+AND\s+",
        "WHERE ",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        rf"\s+AND\s+{predicate_pattern}",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        rf"\bWHERE\s+{predicate_pattern}\s+(GROUP\s+BY|ORDER\s+BY|HAVING)\b",
        r" \1",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        rf"\bWHERE\s+{predicate_pattern}\s*(;)?\s*$",
        r"\1",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\bWHERE\s+AND\b", "WHERE", text, flags=re.IGNORECASE)
    text = re.sub(r"\s{2,}", " ", text).strip()
    if text != sql:
        rules.append("strip_transfers_eventtype_filter")
    return text, rules


def _strip_invalid_eventtype_filter_for_non_transfers(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = str(sql or "").strip()
    if not text:
        return sql, rules
    if "EVENTTYPE" not in text.upper():
        return text, rules
    if re.search(r"\bTRANSFERS\b", text, re.IGNORECASE):
        return text, rules

    column_pattern = r"(?:[A-Za-z0-9_]+\.)?EVENTTYPE"
    value_pattern = r"'[^']*'"

    text = re.sub(
        rf"\bWHERE\s+{column_pattern}\s*=\s*{value_pattern}\s+AND\s+",
        "WHERE ",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        rf"\s+AND\s+{column_pattern}\s*=\s*{value_pattern}",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        rf"\bWHERE\s+{column_pattern}\s*=\s*{value_pattern}\s+(GROUP\s+BY|ORDER\s+BY|HAVING)\b",
        r" \1",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        rf"\bWHERE\s+{column_pattern}\s*=\s*{value_pattern}\s*(;)?\s*$",
        r"\1",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\bWHERE\s+AND\b", "WHERE", text, flags=re.IGNORECASE)
    text = re.sub(r"\bAND\s+AND\b", "AND", text, flags=re.IGNORECASE)
    text = re.sub(r"\bWHERE\s*(?=\bGROUP\b|\bORDER\b|\bHAVING\b|$)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s{2,}", " ", text).strip()
    if text != sql:
        rules.append("strip_nontransfers_eventtype_filter")
    return text, rules


def _strip_inpatient_admission_type_filter(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bADMISSION_TYPE\b\s*=\s*'INPATIENT'", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    explicit_admission_type_intent = any(
        token in q
        for token in (
            "admission type",
            "admission_type",
            "encounter class",
            "admit type",
            "입원 유형",
            "입원 타입",
            "입원 형태",
            "입원 종류",
        )
    )
    if explicit_admission_type_intent:
        return text, rules

    column_pattern = r"(?:[A-Za-z0-9_]+\.)?ADMISSION_TYPE"
    value_pattern = r"'INPATIENT'"

    # WHERE admission_type='INPATIENT' AND ...
    text = re.sub(
        rf"\bWHERE\s+{column_pattern}\s*=\s*{value_pattern}\s+AND\s+",
        "WHERE ",
        text,
        flags=re.IGNORECASE,
    )
    # ... AND admission_type='INPATIENT'
    text = re.sub(
        rf"\s+AND\s+{column_pattern}\s*=\s*{value_pattern}",
        "",
        text,
        flags=re.IGNORECASE,
    )
    # WHERE admission_type='INPATIENT' GROUP/ORDER/HAVING ...
    text = re.sub(
        rf"\bWHERE\s+{column_pattern}\s*=\s*{value_pattern}\s+(GROUP\s+BY|ORDER\s+BY|HAVING)\b",
        r" \1",
        text,
        flags=re.IGNORECASE,
    )
    # WHERE admission_type='INPATIENT' (end)
    text = re.sub(
        rf"\bWHERE\s+{column_pattern}\s*=\s*{value_pattern}\s*(;)?\s*$",
        r"\1",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\bWHERE\s+AND\b", "WHERE", text, flags=re.IGNORECASE)
    text = re.sub(r"\s{2,}", " ", text).strip()
    if text != sql:
        rules.append("strip_inpatient_admission_type_filter")
    return text, rules


def _strip_time_window_if_absent(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if _QUESTION_TIME_INTENT_RE.search(q):
        return text, rules

    if not _TIME_WINDOW_RE.search(text):
        return text, rules

    text = _TIME_WINDOW_RE.sub("", text)
    text = re.sub(r"\bWHERE\s+AND\b", "WHERE", text, flags=re.IGNORECASE)
    text = re.sub(r"\bAND\s+AND\b", "AND", text, flags=re.IGNORECASE)
    text = re.sub(r"\bWHERE\s*(GROUP|ORDER)\b", r"\1", text, flags=re.IGNORECASE)
    text = re.sub(r"\bWHERE\s*$", "", text, flags=re.IGNORECASE)
    rules.append("strip_time_window")
    return text, rules


def _ensure_group_by_not_null(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    outer = _OUTER_ROWNUM_RE.match(text.strip().rstrip(";"))
    if outer:
        inner = outer.group(1).strip()
        limit = outer.group(2)
        inner_fixed, inner_rules = _ensure_group_by_not_null(question, inner)
        if inner_fixed != inner:
            rules.extend(inner_rules)
            rules.append("group_by_not_null_inner")
            return f"SELECT * FROM ({inner_fixed}) WHERE ROWNUM <= {limit}", rules
        return text, rules
    if "GROUP BY" not in text.upper():
        return text, rules
    q = question.lower()
    if "by" not in q and "count" not in q:
        return text, rules

    match = re.search(r"\bgroup\s+by\b\s+(.+?)(?:\border\s+by\b|$)", text, re.IGNORECASE | re.DOTALL)
    if not match:
        return text, rules
    group_clause = match.group(1)
    cols = [c.strip() for c in group_clause.split(",") if c.strip()]
    simple_cols = []
    for col in cols:
        if _IDENT_RE.fullmatch(col) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$#\\.]*", col):
            simple_cols.append(col)
    if not simple_cols:
        return text, rules

    filters = []
    for col in simple_cols:
        if re.search(rf"\b{re.escape(col)}\b\s+IS\s+NOT\s+NULL", text, re.IGNORECASE):
            continue
        filters.append(f"{col} IS NOT NULL")

    if not filters:
        return text, rules

    predicate = " AND ".join(filters)
    if re.search(r"\bwhere\b", text, re.IGNORECASE):
        text = re.sub(r"\bwhere\b", f"WHERE {predicate} AND", text, count=1, flags=re.IGNORECASE)
    else:
        if re.search(r"\bgroup\s+by\b", text, re.IGNORECASE):
            text = re.sub(r"\bgroup\s+by\b", f"WHERE {predicate} GROUP BY", text, count=1, flags=re.IGNORECASE)
        elif re.search(r"\border\s+by\b", text, re.IGNORECASE):
            text = re.sub(r"\border\s+by\b", f"WHERE {predicate} ORDER BY", text, count=1, flags=re.IGNORECASE)
        else:
            text = text.rstrip(";") + f" WHERE {predicate}"
    rules.append("group_by_not_null")
    return text, rules


def _ensure_order_by_count(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if "GROUP BY" not in text.upper() or "COUNT(" not in text.upper():
        return text, rules
    if re.search(r"\border\s+by\b", text, re.IGNORECASE):
        return text, rules
    q = question.lower()
    if "by" not in q and "top" not in q and "count" not in q and "most" not in q and "highest" not in q:
        return text, rules

    order_expr = "CNT"
    if not re.search(r"\bCNT\b", text, re.IGNORECASE):
        order_expr = "COUNT(*)"

    text = text.rstrip(";") + f" ORDER BY {order_expr} DESC"
    rules.append("order_by_count_desc")
    return text, rules


def _dedupe_table_alias(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    text2 = re.sub(r"\b(from|join)\s+([A-Za-z0-9_]+)\s+\2\b", r"\1 \2", text, flags=re.IGNORECASE)
    if text2 != text:
        rules.append("dedupe_table_alias")
    return text2, rules


def _rewrite_timestampdiff(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def repl(match: re.Match) -> str:
        start = match.group(1)
        end = match.group(2)
        return f"CAST({end} AS DATE) - CAST({start} AS DATE)"

    new_text = _TS_DIFF_RE.sub(repl, text)
    if new_text != text:
        rules.append("timestampdiff_day_to_date_diff")
    return new_text, rules


def _rewrite_extract_year(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def repl(match: re.Match) -> str:
        expr = match.group(1)
        col = expr.split(".")[-1].upper()
        if col in {"ANCHOR_YEAR", "ANCHOR_YEAR_GROUP"}:
            return expr
        return match.group(0)

    new_text = _EXTRACT_YEAR_RE.sub(repl, text)
    if new_text != text:
        rules.append("extract_year_on_anchor_year")
    return new_text, rules


def _rewrite_icu_stay(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not _ICU_STAY_RE.search(text):
        return text, rules

    alias = _find_table_alias(text, "ADMISSIONS")
    if alias is None:
        return text, rules

    replacement = (
        f"EXISTS (SELECT 1 FROM ICUSTAYS i "
        f"WHERE {alias}.HADM_ID = i.HADM_ID AND {alias}.SUBJECT_ID = i.SUBJECT_ID)"
    )
    text = _ICU_STAY_RE.sub(replacement, text)
    rules.append("icu_stay_to_icustays")
    return text, rules


def _rewrite_icustays_flag(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not _ICUSTAYS_FLAG_RE.search(text):
        return text, rules

    alias = _find_table_alias(text, "ADMISSIONS")
    if alias is None:
        m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
        if m:
            base_table = m.group(1)
            base_alias = m.group(2) or base_table
            if base_alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER"}:
                base_alias = base_table
            if base_table.upper() in _tables_with_hadm_id():
                alias = base_alias

    replacement = "HADM_ID IN (SELECT HADM_ID FROM ICUSTAYS)"
    if alias and _find_table_alias(text, "ADMISSIONS"):
        replacement = (
            f"EXISTS (SELECT 1 FROM ICUSTAYS i "
            f"WHERE {alias}.HADM_ID = i.HADM_ID AND {alias}.SUBJECT_ID = i.SUBJECT_ID)"
        )
    elif alias:
        replacement = f"{alias}.HADM_ID IN (SELECT HADM_ID FROM ICUSTAYS)"
    text = _ICUSTAYS_FLAG_RE.sub(replacement, text)
    rules.append("icustays_flag_to_icustays")
    return text, rules


def _rewrite_icustays_not_null(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not _ICUSTAYS_NOT_NULL_RE.search(text):
        return text, rules

    alias = _find_table_alias(text, "ADMISSIONS")
    if alias is None:
        m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
        if m:
            base_table = m.group(1)
            base_alias = m.group(2) or base_table
            if base_alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER"}:
                base_alias = base_table
            if base_table.upper() in _tables_with_hadm_id():
                alias = base_alias

    replacement = "HADM_ID IN (SELECT HADM_ID FROM ICUSTAYS)"
    if alias and _find_table_alias(text, "ADMISSIONS"):
        replacement = (
            f"EXISTS (SELECT 1 FROM ICUSTAYS i "
            f"WHERE {alias}.HADM_ID = i.HADM_ID AND {alias}.SUBJECT_ID = i.SUBJECT_ID)"
        )
    elif alias:
        replacement = f"{alias}.HADM_ID IN (SELECT HADM_ID FROM ICUSTAYS)"
    text = _ICUSTAYS_NOT_NULL_RE.sub(replacement, text)
    rules.append("icustays_not_null_to_icustays")
    return text, rules


def _ensure_label_join(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    if not re.search(r"(?<!\.)\bLABEL\b", text, re.IGNORECASE):
        return text, rules

    # If label is already available via D_ITEMS or D_LABITEMS, skip
    if re.search(r"\bD_ITEMS\b", text, re.IGNORECASE) or re.search(r"\bD_LABITEMS\b", text, re.IGNORECASE):
        return text, rules

    injected, inject_rules = _inject_join_in_outer(
        text,
        "CHARTEVENTS",
        " JOIN D_ITEMS d ON {alias}.ITEMID = d.ITEMID",
        r"(?<!\.)\bLABEL\b",
        "d.LABEL",
    )
    if injected:
        rules.extend(inject_rules)
        rules.append("join_d_items_for_label")
        return injected, rules

    if re.search(r"\bCHARTEVENTS\b", text, re.IGNORECASE):
        alias = _find_table_alias(text, "CHARTEVENTS") or "CHARTEVENTS"
        join_clause = f" JOIN D_ITEMS d ON {alias}.ITEMID = d.ITEMID"
        text = _insert_join(text, join_clause)
        text = re.sub(r"(?<!\.)\bLABEL\b", "d.LABEL", text, flags=re.IGNORECASE)
        rules.append("join_d_items_for_label")
        return text, rules

    injected, inject_rules = _inject_join_in_outer(
        text,
        "LABEVENTS",
        " JOIN D_LABITEMS d ON {alias}.ITEMID = d.ITEMID",
        r"(?<!\.)\bLABEL\b",
        "d.LABEL",
    )
    if injected:
        rules.extend(inject_rules)
        rules.append("join_d_labitems_for_label")
        return injected, rules

    if re.search(r"\bLABEVENTS\b", text, re.IGNORECASE):
        alias = _find_table_alias(text, "LABEVENTS") or "LABEVENTS"
        join_clause = f" JOIN D_LABITEMS d ON {alias}.ITEMID = d.ITEMID"
        text = _insert_join(text, join_clause)
        text = re.sub(r"(?<!\.)\bLABEL\b", "d.LABEL", text, flags=re.IGNORECASE)
        rules.append("join_d_labitems_for_label")
        return text, rules

    return text, rules


def _ensure_long_title_join(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    if not re.search(r"(?<!\.)\bLONG_TITLE\b", text, re.IGNORECASE):
        return text, rules

    if re.search(r"\bD_ICD_DIAGNOSES\b", text, re.IGNORECASE) or re.search(r"\bD_ICD_PROCEDURES\b", text, re.IGNORECASE):
        return text, rules

    injected, inject_rules = _inject_join_in_outer(
        text,
        "DIAGNOSES_ICD",
        " JOIN D_ICD_DIAGNOSES d ON {alias}.ICD_CODE = d.ICD_CODE AND {alias}.ICD_VERSION = d.ICD_VERSION",
        r"(?<!\.)\bLONG_TITLE\b",
        "d.LONG_TITLE",
    )
    if injected:
        rules.extend(inject_rules)
        rules.append("join_d_icd_diagnoses_for_long_title")
        return injected, rules

    if re.search(r"\bDIAGNOSES_ICD\b", text, re.IGNORECASE):
        alias = _find_table_alias(text, "DIAGNOSES_ICD") or "DIAGNOSES_ICD"
        join_clause = f" JOIN D_ICD_DIAGNOSES d ON {alias}.ICD_CODE = d.ICD_CODE AND {alias}.ICD_VERSION = d.ICD_VERSION"
        text = _insert_join(text, join_clause)
        text = re.sub(r"(?<!\.)\bLONG_TITLE\b", "d.LONG_TITLE", text, flags=re.IGNORECASE)
        rules.append("join_d_icd_diagnoses_for_long_title")
        return text, rules

    injected, inject_rules = _inject_join_in_outer(
        text,
        "PROCEDURES_ICD",
        " JOIN D_ICD_PROCEDURES d ON {alias}.ICD_CODE = d.ICD_CODE AND {alias}.ICD_VERSION = d.ICD_VERSION",
        r"(?<!\.)\bLONG_TITLE\b",
        "d.LONG_TITLE",
    )
    if injected:
        rules.extend(inject_rules)
        rules.append("join_d_icd_procedures_for_long_title")
        return injected, rules

    if re.search(r"\bPROCEDURES_ICD\b", text, re.IGNORECASE):
        alias = _find_table_alias(text, "PROCEDURES_ICD") or "PROCEDURES_ICD"
        join_clause = f" JOIN D_ICD_PROCEDURES d ON {alias}.ICD_CODE = d.ICD_CODE AND {alias}.ICD_VERSION = d.ICD_VERSION"
        text = _insert_join(text, join_clause)
        text = re.sub(r"(?<!\.)\bLONG_TITLE\b", "d.LONG_TITLE", text, flags=re.IGNORECASE)
        rules.append("join_d_icd_procedures_for_long_title")
        return text, rules

    return text, rules


def _rewrite_has_icu_stay(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not _HAS_ICU_RE.search(text):
        return text, rules

    alias = _find_table_alias(text, "ADMISSIONS")

    if alias is None:
        return text, rules

    replacement = (
        f"EXISTS (SELECT 1 FROM ICUSTAYS i "
        f"WHERE {alias}.HADM_ID = i.HADM_ID AND {alias}.SUBJECT_ID = i.SUBJECT_ID)"
    )
    text = _HAS_ICU_RE.sub(replacement, text)
    rules.append("has_icu_stay_to_icustays")
    return text, rules


def _align_admissions_icu_match_keys(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    cfg = load_sql_postprocess_rules().get("admissions_icu_alignment", {})
    if not bool(cfg.get("enabled", True)):
        return text, rules

    admissions_table = str(cfg.get("admissions_table") or "ADMISSIONS").strip().upper() or "ADMISSIONS"
    icu_table = str(cfg.get("icustays_table") or "ICUSTAYS").strip().upper() or "ICUSTAYS"
    if not re.search(rf"\b{re.escape(admissions_table)}\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(rf"\b{re.escape(icu_table)}\b", text, re.IGNORECASE):
        return text, rules

    adm_alias = _find_table_alias(text, admissions_table) or admissions_table
    icu_alias = _find_table_alias(text, icu_table) or icu_table

    hadm_patterns = [
        re.compile(rf"\b{re.escape(adm_alias)}\.HADM_ID\s*=\s*{re.escape(icu_alias)}\.HADM_ID\b", re.IGNORECASE),
        re.compile(rf"\b{re.escape(icu_alias)}\.HADM_ID\s*=\s*{re.escape(adm_alias)}\.HADM_ID\b", re.IGNORECASE),
    ]
    subj_patterns = [
        re.compile(rf"\b{re.escape(adm_alias)}\.SUBJECT_ID\s*=\s*{re.escape(icu_alias)}\.SUBJECT_ID\b", re.IGNORECASE),
        re.compile(rf"\b{re.escape(icu_alias)}\.SUBJECT_ID\s*=\s*{re.escape(adm_alias)}\.SUBJECT_ID\b", re.IGNORECASE),
    ]

    has_hadm = any(p.search(text) for p in hadm_patterns)
    has_subj = any(p.search(text) for p in subj_patterns)

    if has_hadm and not has_subj:
        replaced = False
        for p in hadm_patterns:
            if p.search(text):
                text = p.sub(
                    f"{adm_alias}.SUBJECT_ID = {icu_alias}.SUBJECT_ID "
                    f"AND {adm_alias}.HADM_ID = {icu_alias}.HADM_ID",
                    text,
                    count=1,
                )
                replaced = True
                break
        if replaced:
            rules.append("align_admissions_icu_match_keys")
            return text, rules

    if has_subj and not has_hadm:
        replaced = False
        for p in subj_patterns:
            if p.search(text):
                text = p.sub(
                    f"{adm_alias}.SUBJECT_ID = {icu_alias}.SUBJECT_ID "
                    f"AND {adm_alias}.HADM_ID = {icu_alias}.HADM_ID",
                    text,
                    count=1,
                )
                replaced = True
                break
        if replaced:
            rules.append("align_admissions_icu_match_keys")
            return text, rules

    def _in_to_exists(match: re.Match) -> str:
        alias = match.group("adm")
        return (
            f"EXISTS (SELECT 1 FROM {icu_table} i "
            f"WHERE {alias}.HADM_ID = i.HADM_ID AND {alias}.SUBJECT_ID = i.SUBJECT_ID)"
        )

    rewritten = _ADM_ICU_IN_RE.sub(_in_to_exists, text)
    if rewritten != text:
        rules.append("align_admissions_icu_match_keys")
        return rewritten, rules

    return text, rules


def _normalize_timestamp_diffs(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def repl(match: re.Match) -> str:
        a = match.group(1)
        b = match.group(2)
        a_col = a.split(".")[-1].upper()
        b_col = b.split(".")[-1].upper()
        if a_col in _timestamp_cols() and b_col in _timestamp_cols():
            return f"CAST({a} AS DATE) - CAST({b} AS DATE)"
        return match.group(0)

    new_text = _DIFF_RE.sub(repl, text)
    if new_text != text:
        rules.append("timestamp_diff_cast_to_date")
    return new_text, rules


def _rewrite_title_filter_with_icd_map(
    *,
    question: str,
    sql: str,
    cfg_key: str,
    default_table_name: str,
    matcher: Any,
    rule_name: str,
) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    cfg = load_sql_postprocess_rules().get(cfg_key, {})
    if not bool(cfg.get("enabled", True)):
        return text, rules

    table_name = str(cfg.get("table_name") or default_table_name).strip().upper() or default_table_name
    if not re.search(rf"\b{re.escape(table_name)}\b", text, re.IGNORECASE):
        return text, rules
    if not _DIAGNOSIS_TITLE_FILTER_RE.search(text):
        return text, rules

    matched = matcher(question)
    if not matched:
        return text, rules

    prefixes: list[str] = []
    for item in matched:
        for prefix in item.get("icd_prefixes", []):
            value = str(prefix).strip().upper()
            if not value or value in prefixes:
                continue
            prefixes.append(value)
    if not prefixes:
        return text, rules

    alias = _find_table_alias(text, table_name) or table_name
    like_template = str(cfg.get("icd_like_template") or "{alias}.ICD_CODE LIKE '{prefix}%'")
    join_operator = str(cfg.get("join_operator") or " OR ")
    predicates: list[str] = []
    for prefix in prefixes:
        try:
            predicates.append(like_template.format(alias=alias, prefix=prefix))
        except Exception:
            predicates.append(f"{alias}.ICD_CODE LIKE '{prefix}%'")
    icd_filter = "(" + join_operator.join(predicates) + ")"
    rewritten = _DIAGNOSIS_TITLE_FILTER_RE.sub(icd_filter, text)
    if rewritten != text:
        rules.append(rule_name)
    return rewritten, rules


def _rewrite_diagnosis_title_filter_with_icd_map(question: str, sql: str) -> tuple[str, list[str]]:
    return _rewrite_title_filter_with_icd_map(
        question=question,
        sql=sql,
        cfg_key="diagnosis_rewrite",
        default_table_name="DIAGNOSES_ICD",
        matcher=match_diagnosis_mappings,
        rule_name="diagnosis_title_filter_to_icd_prefix",
    )


def _rewrite_procedure_title_filter_with_icd_map(question: str, sql: str) -> tuple[str, list[str]]:
    return _rewrite_title_filter_with_icd_map(
        question=question,
        sql=sql,
        cfg_key="procedure_rewrite",
        default_table_name="PROCEDURES_ICD",
        matcher=match_procedure_mappings,
        rule_name="procedure_title_filter_to_icd_prefix",
    )


def _rewrite_mortality_avg_under_icd_join(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    mortality_cfg = load_sql_postprocess_rules().get("mortality_rewrite", {})
    if not bool(mortality_cfg.get("enabled", True)):
        return text, rules

    join_tables_cfg = mortality_cfg.get("join_tables")
    if isinstance(join_tables_cfg, list):
        join_tables = [str(item).strip().upper() for item in join_tables_cfg if str(item).strip()]
    else:
        single = str(mortality_cfg.get("join_table") or "DIAGNOSES_ICD").strip().upper()
        join_tables = [single] if single else ["DIAGNOSES_ICD"]
    admissions_table = str(mortality_cfg.get("admissions_table") or "ADMISSIONS").strip().upper() or "ADMISSIONS"
    outcome_column = str(mortality_cfg.get("outcome_column") or "HOSPITAL_EXPIRE_FLAG").strip().upper() or "HOSPITAL_EXPIRE_FLAG"
    key_column = str(mortality_cfg.get("key_column") or "HADM_ID").strip().upper() or "HADM_ID"
    numerator_template = str(
        mortality_cfg.get("numerator_template")
        or "COUNT(DISTINCT CASE WHEN {expire_ref} = 1 THEN {key_ref} END)"
    )
    denominator_template = str(
        mortality_cfg.get("denominator_template")
        or "NULLIF(COUNT(DISTINCT {key_ref}), 0)"
    )

    has_target_join = any(re.search(rf"\bJOIN\s+{re.escape(table)}\b", text, re.IGNORECASE) for table in join_tables)
    if not has_target_join:
        return text, rules
    if not re.search(r"\bAVG\s*\(", text, re.IGNORECASE):
        return text, rules
    if not re.search(rf"\b{re.escape(outcome_column)}\b", text, re.IGNORECASE):
        return text, rules

    adm_alias = _find_table_alias(text, admissions_table)
    key_ref = f"{adm_alias}.{key_column}" if adm_alias else key_column
    expire_ref = f"{adm_alias}.{outcome_column}" if adm_alias else outcome_column
    try:
        numerator_expr = numerator_template.format(expire_ref=expire_ref, key_ref=key_ref)
    except Exception:
        numerator_expr = f"COUNT(DISTINCT CASE WHEN {expire_ref} = 1 THEN {key_ref} END)"
    try:
        denominator_expr = denominator_template.format(key_ref=key_ref, expire_ref=expire_ref)
    except Exception:
        denominator_expr = f"NULLIF(COUNT(DISTINCT {key_ref}), 0)"
    ratio_expr = f"{numerator_expr} / {denominator_expr}"

    expire_ref_pattern = re.escape(expire_ref)
    changed = False

    direct_avg_re = re.compile(rf"AVG\s*\(\s*{expire_ref_pattern}\s*\)", re.IGNORECASE)
    rewritten = direct_avg_re.sub(ratio_expr, text)
    if rewritten != text:
        changed = True
        text = rewritten

    case_avg_re = re.compile(
        rf"AVG\s*\(\s*CASE\s+WHEN\s+{expire_ref_pattern}\s*=\s*1\s+THEN\s+1\s+ELSE\s+0\s+END\s*\)",
        re.IGNORECASE,
    )
    rewritten = case_avg_re.sub(ratio_expr, text)
    if rewritten != text:
        changed = True
        text = rewritten

    if changed:
        rules.append("mortality_avg_to_distinct_hadm_ratio")
    return text, rules


def _rewrite_ratio_denominator_distinct_under_icd_join(sql: str) -> tuple[str, list[str]]:
    """Avoid diagnosis/procedure join fan-out in ratio denominators."""
    rules: list[str] = []
    text = sql
    if not _JOIN_ICD_TABLE_RE.search(text):
        return text, rules
    if "/" not in text or "COUNT(" not in text.upper():
        return text, rules

    admissions_alias = _find_table_alias(text, "ADMISSIONS")
    key_ref = f"{admissions_alias}.HADM_ID" if admissions_alias else "HADM_ID"
    changed = False

    def _repl_nullif(match: re.Match) -> str:
        nonlocal changed
        den = str(match.group("den") or "").strip()
        if den and den.upper().endswith("HADM_ID"):
            return match.group(0)
        changed = True
        return f"/ NULLIF(COUNT(DISTINCT {key_ref}), 0)"

    rewritten = _COUNT_DENOM_NULLIF_RE.sub(_repl_nullif, text)

    def _repl_count(match: re.Match) -> str:
        nonlocal changed
        den = str(match.group("den") or "").strip()
        if den and den.upper().endswith("HADM_ID"):
            return match.group(0)
        changed = True
        return f"/ NULLIF(COUNT(DISTINCT {key_ref}), 0)"

    rewritten2 = _COUNT_DENOM_RE.sub(_repl_count, rewritten)
    if changed and rewritten2 != text:
        rules.append("ratio_denominator_to_distinct_hadm_under_icd_join")
        return rewritten2, rules
    return text, rules


def recommend_postprocess_profile(
    question: str,
    sql: str,
    default_profile: str = "relaxed",
) -> tuple[str, list[str]]:
    """Choose relaxed/aggressive profile by semantic risk hints."""
    profile = str(default_profile or "relaxed").strip().lower()
    if profile not in {"relaxed", "aggressive", "auto"}:
        profile = "relaxed"

    q = str(question or "")
    text = str(sql or "")
    upper = text.upper()
    reasons: list[str] = []

    if _RATIO_INTENT_RE.search(q) and _JOIN_ICD_TABLE_RE.search(upper):
        if _COUNT_DENOM_NULLIF_RE.search(upper) or _COUNT_DENOM_RE.search(upper):
            reasons.append("ratio_denominator_not_distinct_under_icd_join")
        if re.search(r"\bAVG\s*\(\s*(?:[A-Za-z0-9_]+\.)?HOSPITAL_EXPIRE_FLAG\s*\)", upper, re.IGNORECASE):
            reasons.append("mortality_avg_under_icd_join")

    if _ICU_QUERY_INTENT_RE.search(q) and _MORTALITY_QUERY_INTENT_RE.search(q):
        has_hospital_expire = bool(re.search(r"\bHOSPITAL_EXPIRE_FLAG\b", upper, re.IGNORECASE))
        has_death_alignment = bool(
            re.search(r"\bDEATHTIME\b", upper, re.IGNORECASE)
            and re.search(r"\bINTIME\b", upper, re.IGNORECASE)
            and re.search(r"\bOUTTIME\b", upper, re.IGNORECASE)
        )
        if has_hospital_expire and not has_death_alignment:
            reasons.append("icu_mortality_outcome_misaligned")

    if re.search(r"\bJOIN\s+ICUSTAYS\b", upper):
        on_clause = re.search(r"\bJOIN\s+ICUSTAYS\b[\s\S]*?\bON\b([\s\S]*?)(?:\bJOIN\b|\bWHERE\b|\bGROUP\b|\bORDER\b|$)", text, re.IGNORECASE)
        if on_clause:
            join_cond = on_clause.group(1).upper()
            if "HADM_ID" in join_cond and "SUBJECT_ID" not in join_cond:
                reasons.append("admissions_icu_partial_join_key")

    first_last_careunit_intent = bool(
        re.search(r"(first|last)\s*care\s*unit|(first|last)\s*careunit|첫\s*careunit|마지막\s*careunit", q, re.IGNORECASE)
    )
    if first_last_careunit_intent:
        has_transfers = bool(re.search(r"\bTRANSFERS\b", upper))
        has_icustays = bool(re.search(r"\bICUSTAYS\b", upper))
        has_bare_careunit = bool(re.search(r"\bCAREUNIT\b", upper))
        has_first_last_col = bool(re.search(r"\b(FIRST_CAREUNIT|LAST_CAREUNIT)\b", upper))
        if has_transfers and has_bare_careunit and not has_icustays:
            reasons.append("first_last_careunit_intent_on_transfers")
        elif has_icustays and has_bare_careunit and not has_first_last_col:
            reasons.append("first_last_careunit_missing_specific_column")

    if reasons:
        return "aggressive", reasons
    return ("relaxed" if profile == "auto" else profile), reasons


def _rewrite_count_columns_to_ratio_by_intent(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql.strip()
    if not text:
        return sql, rules
    if not _RATIO_INTENT_RE.search(question):
        return sql, rules

    select_span = _find_final_select_from_span(text)
    if not select_span:
        return sql, rules
    core, select_idx, from_idx = select_span
    if re.match(r"^\s*WITH\b", core, re.IGNORECASE):
        return sql, rules
    select_clause = core[select_idx + len("SELECT"):from_idx]
    if not select_clause.strip():
        return sql, rules

    items = _split_top_level_csv(select_clause)
    if not items:
        return sql, rules

    if re.search(r"/\s*NULLIF\s*\(", select_clause, re.IGNORECASE):
        return sql, rules
    for item in items:
        alias = _extract_select_alias(item) or ""
        if alias and _RATIO_ALIAS_RE.search(alias):
            return sql, rules
        upper_item = item.upper()
        if "AVG(" in upper_item and "COUNT(" in upper_item:
            return sql, rules

    count_aliases: list[str] = []
    for item in items:
        if "COUNT(" not in item.upper():
            continue
        alias = _extract_select_alias(item)
        if not alias or not _COUNT_ALIAS_NAME_RE.match(alias):
            continue
        if alias not in count_aliases:
            count_aliases.append(alias)

    if len(count_aliases) < 2:
        return sql, rules

    denominator = next((name for name in count_aliases if _DENOM_ALIAS_HINT_RE.search(name)), None)
    if not denominator:
        has_explicit_denominator_intent = bool(_RATIO_DENOM_INTENT_RE.search(question))
        if has_explicit_denominator_intent and len(count_aliases) == 2:
            denominator = count_aliases[1]
    if not denominator:
        return sql, rules

    numerator = next(
        (name for name in count_aliases if name != denominator and not _DENOM_ALIAS_HINT_RE.search(name)),
        None,
    )
    if not numerator:
        numerator = next((name for name in count_aliases if name != denominator), None)
    if not numerator or numerator == denominator:
        return sql, rules

    wrapped = (
        f"SELECT t.*, ROUND(100 * t.{numerator} / NULLIF(t.{denominator}, 0), 2) AS RATIO_PCT "
        f"FROM ({core}) t"
    )
    rules.append(f"count_columns_to_ratio_pct:{numerator}/{denominator}")
    return wrapped, rules


def _rewrite_unknown_categorical_equals(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not _CATEGORICAL_REWRITE_INTENT_RE.search(str(question or "")):
        return text, rules
    value_index = _column_value_index()
    if not value_index:
        return text, rules

    alias_map = _table_alias_map(text)
    question_tokens = _tokenize_text(question)
    changed = False

    def repl(match: re.Match) -> str:
        nonlocal changed
        ref = str(match.group("ref") or "").strip()
        literal = str(match.group("lit") or "").strip()
        if not ref or not literal:
            return match.group(0)
        if "%" in literal or "_" in literal:
            return match.group(0)

        if "." in ref:
            lhs, col = ref.split(".", 1)
            table = alias_map.get(lhs.strip().upper())
            col_ref = col.strip().upper()
        else:
            table = None
            col_ref = ref.strip().upper()

        if not table:
            table_candidates = [table_name for table_name, cols in value_index.items() if col_ref in cols]
            if len(table_candidates) != 1:
                return match.group(0)
            table = table_candidates[0]

        col_values = value_index.get(table, {}).get(col_ref, [])
        if not col_values:
            return match.group(0)
        if any(value.lower() == literal.lower() for value in col_values):
            # Keep canonical value casing if possible.
            canonical = next((value for value in col_values if value.lower() == literal.lower()), literal)
            if canonical != literal:
                changed = True
                return f"{ref} = {_sql_quote_literal(canonical)}"
            return match.group(0)

        selected = _select_best_categorical_values(
            literal,
            values=col_values,
            question_tokens=question_tokens,
        )

        target_ref = ref
        target_col = col_ref
        if not selected:
            # Generic fallback: DESCRIPTION column often stores coarse labels.
            # If literal looks like a concrete term, try paired NAME column.
            if col_ref.endswith("DESCRIPTION"):
                alt_col = col_ref[: -len("DESCRIPTION")] + "NAME"
                alt_values = value_index.get(table, {}).get(alt_col, [])
                selected = _select_best_categorical_values(
                    literal,
                    values=alt_values,
                    question_tokens=question_tokens,
                )
                if selected:
                    target_col = alt_col
                    if "." in ref:
                        lhs, _ = ref.split(".", 1)
                        target_ref = f"{lhs}.{alt_col}"
                    else:
                        target_ref = alt_col
            elif col_ref.endswith("NAME"):
                alt_col = col_ref[: -len("NAME")] + "DESCRIPTION"
                alt_values = value_index.get(table, {}).get(alt_col, [])
                selected = _select_best_categorical_values(
                    literal,
                    values=alt_values,
                    question_tokens=question_tokens,
                )
                if selected:
                    target_col = alt_col
                    if "." in ref:
                        lhs, _ = ref.split(".", 1)
                        target_ref = f"{lhs}.{alt_col}"
                    else:
                        target_ref = alt_col

        if not selected:
            return match.group(0)

        changed = True
        if len(selected) == 1:
            return f"{target_ref} = {_sql_quote_literal(selected[0])}"

        joined = ", ".join(_sql_quote_literal(value) for value in selected[:3])
        return f"{target_ref} IN ({joined})"

    rewritten = _CATEGORICAL_EQ_LITERAL_RE.sub(repl, text)
    if changed and rewritten != text:
        rules.append("unknown_categorical_equals_to_known_values")
        return rewritten, rules
    return text, rules


def _rewrite_d_items_long_title_to_label(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bD_ITEMS\b", text, re.IGNORECASE):
        return text, rules

    changed = False
    alias_map = _table_alias_map(text)
    d_items_aliases = [alias for alias, table in alias_map.items() if table == "D_ITEMS"]
    for alias in d_items_aliases:
        pattern = re.compile(rf"\b{re.escape(alias)}\.LONG_TITLE\b", re.IGNORECASE)
        rewritten = pattern.sub(f"{alias}.LABEL", text)
        if rewritten != text:
            changed = True
            text = rewritten

    if not re.search(r"\bD_ICD_DIAGNOSES\b|\bD_ICD_PROCEDURES\b", text, re.IGNORECASE):
        rewritten = re.sub(r"(?<!\.)\bLONG_TITLE\b", "LABEL", text, flags=re.IGNORECASE)
        if rewritten != text:
            changed = True
            text = rewritten

    if changed:
        rules.append("d_items_long_title_to_label")
    return text, rules


def _rewrite_itemid_scalar_subquery_to_safe_in(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    changed = False
    pos = 0

    while True:
        match = _ITEMID_SCALAR_SUBQUERY_EQ_RE.search(text, pos)
        if not match:
            break
        open_idx = match.end() - 1
        close_idx = _find_matching_paren_index(text, open_idx)
        if close_idx is None:
            break

        subquery = text[open_idx + 1 : close_idx].strip()
        if not re.match(
            r"^\s*SELECT\s+(?:[A-Za-z_][A-Za-z0-9_$#]*\.)?ITEMID\s+FROM\s+(D_ITEMS|D_LABITEMS)\b",
            subquery,
            re.IGNORECASE | re.DOTALL,
        ):
            pos = close_idx + 1
            continue

        repaired_subquery = subquery
        repaired_subquery = re.sub(
            r"^(\s*SELECT\s+)(?P<sel>(?:[A-Za-z_][A-Za-z0-9_$#]*\.)?ITEMID)\b",
            lambda m: f"{m.group(1)}TO_CHAR({m.group('sel')})",
            repaired_subquery,
            count=1,
            flags=re.IGNORECASE | re.DOTALL,
        )
        repaired_subquery = re.sub(
            r"\b([A-Za-z_][A-Za-z0-9_$#]*)\.LONG_TITLE\b",
            r"\1.LABEL",
            repaired_subquery,
            flags=re.IGNORECASE,
        )
        repaired_subquery = re.sub(
            r"(?<!\.)\bLONG_TITLE\b",
            "LABEL",
            repaired_subquery,
            flags=re.IGNORECASE,
        )
        replacement = f"TO_CHAR({match.group('lhs')}) IN ({repaired_subquery})"
        text = text[: match.start()] + replacement + text[close_idx + 1 :]
        changed = True
        pos = match.start() + len(replacement)

    if changed:
        rules.append("itemid_scalar_subquery_to_safe_in")
    return text, rules


def _rewrite_itemid_icd_join_mismatch(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    alias_map = _table_alias_map(text)
    if not alias_map:
        return text, rules

    changed = False
    planned_dim_tables: dict[str, str] = {}

    itemid_fact_tables = {
        "CHARTEVENTS",
        "DATETIMEEVENTS",
        "INPUTEVENTS",
        "OUTPUTEVENTS",
        "PROCEDUREEVENTS",
        "INGREDIENTEVENTS",
        "LABEVENTS",
    }
    icd_dim_tables = {"D_ICD_DIAGNOSES", "D_ICD_PROCEDURES"}

    def repl(match: re.Match) -> str:
        nonlocal changed
        lhs_alias = str(match.group("lhs") or "").upper()
        rhs_alias = str(match.group("rhs") or "").upper()
        lcol = str(match.group("lcol") or "").upper()
        rcol = str(match.group("rcol") or "").upper()
        if lcol == rcol:
            return match.group(0)
        if {lcol, rcol} != {"ITEMID", "ICD_CODE"}:
            return match.group(0)

        left_table = alias_map.get(lhs_alias, lhs_alias)
        right_table = alias_map.get(rhs_alias, rhs_alias)

        if left_table in icd_dim_tables and right_table in itemid_fact_tables:
            icd_alias, item_alias = lhs_alias, rhs_alias
            item_table = right_table
        elif right_table in icd_dim_tables and left_table in itemid_fact_tables:
            icd_alias, item_alias = rhs_alias, lhs_alias
            item_table = left_table
        else:
            return match.group(0)

        dim_table = "D_LABITEMS" if item_table == "LABEVENTS" else "D_ITEMS"
        planned_dim_tables[icd_alias] = dim_table
        changed = True
        return f"TO_CHAR({item_alias}.ITEMID) = TO_CHAR({icd_alias}.ITEMID)"

    rewritten = _ITEMID_ICD_EQ_RE.sub(repl, text)
    if rewritten != text:
        text = rewritten
        changed = True

    for icd_alias, dim_table in planned_dim_tables.items():
        text = re.sub(
            rf"\bJOIN\s+D_ICD_DIAGNOSES\s+{re.escape(icd_alias)}\b",
            f"JOIN {dim_table} {icd_alias}",
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(
            rf"\bJOIN\s+D_ICD_PROCEDURES\s+{re.escape(icd_alias)}\b",
            f"JOIN {dim_table} {icd_alias}",
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(
            rf"\b{re.escape(icd_alias)}\.ICD_CODE\b",
            f"{icd_alias}.ITEMID",
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(
            rf"\b{re.escape(icd_alias)}\.LONG_TITLE\b",
            f"{icd_alias}.LABEL",
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(
            rf"\b{re.escape(icd_alias)}\.ICD_VERSION\s*=\s*(?:9|10)\s+AND\s*",
            "",
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(
            rf"\s+(?:AND|OR)\s+{re.escape(icd_alias)}\.ICD_VERSION\s*=\s*(?:9|10)\b",
            "",
            text,
            flags=re.IGNORECASE,
        )

    if changed:
        rules.append("rewrite_itemid_icd_join_mismatch")
    return text, rules


def _extract_cte_projection_aliases(sql: str, cte_name: str) -> set[str]:
    if not cte_name:
        return set()
    pattern = re.compile(rf"\b{re.escape(cte_name)}\s+AS\s*\(", re.IGNORECASE)
    match = pattern.search(sql)
    if not match:
        return set()

    open_idx = match.end() - 1
    close_idx = _find_matching_paren_index(sql, open_idx)
    if close_idx is None:
        return set()

    body = sql[open_idx + 1 : close_idx].strip()
    if not body:
        return set()

    select_span = _find_final_select_from_span(body)
    if not select_span:
        return set()
    core, select_idx, from_idx = select_span
    select_clause = core[select_idx + len("SELECT") : from_idx]
    items = _split_top_level_csv(select_clause)
    aliases: set[str] = set()
    for item in items:
        alias = _extract_select_alias(item)
        if alias:
            aliases.add(alias.upper())
            continue
        simple = _SIMPLE_COLUMN_REF_RE.match(item.strip())
        if simple:
            aliases.add(str(simple.group("col") or "").upper())
    return aliases


def _fix_cte_projection_alias_mismatch(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.match(r"^\s*WITH\b", text, re.IGNORECASE):
        return text, rules

    select_span = _find_final_select_from_span(text)
    if not select_span:
        return text, rules
    core, select_idx, from_idx = select_span

    from_clause = core[from_idx:]
    from_match = re.match(
        r"^\s*FROM\s+([A-Za-z_][A-Za-z0-9_$#]*)"
        r"(?:\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_$#]*))?",
        from_clause,
        re.IGNORECASE,
    )
    if not from_match:
        return text, rules
    source_name = str(from_match.group(1) or "").strip()
    source_alias = str(from_match.group(2) or "").strip()
    if source_alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER", "HAVING"}:
        source_alias = ""
    if not source_name:
        return text, rules

    cte_aliases = _extract_cte_projection_aliases(core, source_name)
    if not cte_aliases:
        return text, rules

    measure_alias = "CNT" if "CNT" in cte_aliases else ""
    if not measure_alias:
        countlike = [name for name in cte_aliases if _COUNTLIKE_ALIAS_RE.match(name)]
        if len(countlike) == 1:
            measure_alias = countlike[0]
    if not measure_alias:
        return text, rules

    select_clause = core[select_idx + len("SELECT") : from_idx]
    items = _split_top_level_csv(select_clause)
    if not items:
        return text, rules

    new_items: list[str] = []
    unknown_cols: list[str] = []
    changed = False
    source_alias_upper = source_alias.upper()
    for item in items:
        stripped = item.strip()
        simple = _SIMPLE_COLUMN_REF_RE.match(stripped)
        if not simple:
            new_items.append(item)
            continue

        prefix = str(simple.group("prefix") or "").strip()
        col = str(simple.group("col") or "").strip()
        col_upper = col.upper()
        if prefix and source_alias_upper and prefix.upper() != source_alias_upper:
            new_items.append(item)
            continue

        if col_upper in cte_aliases:
            new_items.append(item)
            continue

        unknown_cols.append(col_upper)
        if prefix:
            new_items.append(f"{prefix}.{measure_alias}")
        else:
            new_items.append(measure_alias)
        changed = True

    if not changed:
        return text, rules
    if len(set(unknown_cols)) != 1:
        return text, rules

    rebuilt = core[: select_idx + len("SELECT")] + " " + ", ".join(new_items) + " " + core[from_idx:]
    if rebuilt != text:
        rules.append(f"cte_projection_alias_mismatch_to_{measure_alias.lower()}")
        return rebuilt, rules
    return text, rules


def _rewrite_label_like_case_insensitive(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    alias_map = _table_alias_map(text)
    tables = set(alias_map.values())
    if "D_ITEMS" not in tables and "D_LABITEMS" not in tables:
        return text, rules

    changed = False

    def repl(match: re.Match) -> str:
        nonlocal changed
        ref = str(match.group("ref") or "").strip()
        op = str(match.group("op") or "LIKE").upper()
        literal = str(match.group("lit") or "")
        if not ref:
            return match.group(0)
        # Skip literals without alphabetic characters.
        if not re.search(r"[A-Za-z]", literal):
            return match.group(0)

        table_ok = False
        if "." in ref:
            alias = ref.split(".", 1)[0].strip().upper()
            table = alias_map.get(alias)
            table_ok = table in {"D_ITEMS", "D_LABITEMS"}
        else:
            # Unqualified LABEL: rewrite only when exactly one label dictionary is present.
            dict_tables = [name for name in ("D_ITEMS", "D_LABITEMS") if name in tables]
            table_ok = len(dict_tables) == 1

        if not table_ok:
            return match.group(0)

        upper_lit = literal.upper()
        changed = True
        return f"UPPER({ref}) {op} '{upper_lit}'"

    rewritten = _RAW_LABEL_LIKE_RE.sub(repl, text)
    if changed and rewritten != text:
        rules.append("label_like_case_insensitive")
        return rewritten, rules
    return text, rules


def _upper_tokens(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    tokens: list[str] = []
    for raw in values:
        token = str(raw or "").strip().upper()
        if token and token not in tokens:
            tokens.append(token)
    return tokens


def _question_has_any_token(question_lower: str, tokens: list[str]) -> bool:
    if not tokens:
        return True
    compact = re.sub(r"\s+", "", question_lower)
    for token in tokens:
        t = token.lower()
        if t in question_lower or t in compact:
            return True
    return False


def _is_placeholder_question(question: str) -> bool:
    text = re.sub(r"\s+", " ", str(question or "").strip().lower())
    if not text:
        return True
    placeholders = {
        "fix failed sql while preserving original intent.",
        "fix failed sql while preserving original intent",
    }
    return text in placeholders


def _load_active_label_intent_profiles(question: str, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    question_placeholder = _is_placeholder_question(question)
    static_profiles = cfg.get("profiles")
    if isinstance(static_profiles, list):
        for item in static_profiles:
            if isinstance(item, dict):
                profiles.append(dict(item))

    if bool(cfg.get("use_metadata_profiles", True)):
        try:
            max_metadata_profiles = int(cfg.get("max_metadata_profiles", 4))
        except Exception:
            max_metadata_profiles = 4
        max_metadata_profiles = max(1, min(12, max_metadata_profiles))
        try:
            min_metadata_score = int(cfg.get("min_metadata_score", 1))
        except Exception:
            min_metadata_score = 1
        loaded_profiles = load_label_intent_profiles()
        if question_placeholder:
            matched_profiles = [
                {**dict(item), "_score": max(min_metadata_score, 1)}
                for item in loaded_profiles
                if isinstance(item, dict) and bool(item.get("allow_sql_pattern_only", False))
            ][:max_metadata_profiles]
        else:
            matched_profiles = match_label_intent_profiles(
                question,
                profiles=loaded_profiles,
                k=max_metadata_profiles,
            )
        for item in matched_profiles:
            if not isinstance(item, dict):
                continue
            try:
                score = int(item.get("_score") or 0)
            except Exception:
                score = 0
            if not question_placeholder and score < min_metadata_score:
                continue
            profiles.append(dict(item))

    deduped: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for item in profiles:
        key = str(item.get("id") or item.get("name") or "").strip().lower()
        if not key:
            key = json.dumps(item, ensure_ascii=True, sort_keys=True)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(item)
    return deduped


def _build_label_anchor_verb_block_re(anchor_terms: list[str], verb_terms: list[str]) -> re.Pattern | None:
    if not anchor_terms or not verb_terms:
        return None
    label_expr = (
        r"(?:UPPER\(\s*(?:[A-Za-z_][A-Za-z0-9_$#]*\.)?LABEL\s*\)"
        r"|(?:[A-Za-z_][A-Za-z0-9_$#]*\.)?LABEL)"
    )
    anchor_alt = "|".join(re.escape(term) + r"[^']*" for term in anchor_terms)
    verb_alt = "|".join(re.escape(term) + r"[^']*" for term in verb_terms)
    return re.compile(
        rf"\(\s*(?P<anchor>(?P<ref>{label_expr})\s+LIKE\s+'%(?:{anchor_alt})%')\s+AND\s*"
        rf"\((?:\s*(?P=ref)\s+LIKE\s+'%(?:{verb_alt})%'\s*(?:OR\s*)?)+\)\s*\)",
        re.IGNORECASE,
    )


def _build_label_anchor_verb_inline_re(anchor_terms: list[str], verb_terms: list[str]) -> re.Pattern | None:
    if not anchor_terms or not verb_terms:
        return None
    label_expr = (
        r"(?:UPPER\(\s*(?:[A-Za-z_][A-Za-z0-9_$#]*\.)?LABEL\s*\)"
        r"|(?:[A-Za-z_][A-Za-z0-9_$#]*\.)?LABEL)"
    )
    anchor_alt = "|".join(re.escape(term) + r"[^']*" for term in anchor_terms)
    verb_alt = "|".join(re.escape(term) + r"[^']*" for term in verb_terms)
    return re.compile(
        rf"(?P<anchor>(?P<ref>{label_expr})\s+LIKE\s+'%(?:{anchor_alt})%')\s+AND\s*"
        rf"\((?:\s*(?P=ref)\s+LIKE\s+'%(?:{verb_alt})%'\s*(?:OR\s*)?)+\)",
        re.IGNORECASE,
    )


def _build_label_anchor_expr_re(anchor_terms: list[str]) -> re.Pattern | None:
    if not anchor_terms:
        return None
    label_expr = (
        r"(?:UPPER\(\s*(?:[A-Za-z_][A-Za-z0-9_$#]*\.)?LABEL\s*\)"
        r"|(?:[A-Za-z_][A-Za-z0-9_$#]*\.)?LABEL)"
    )
    anchor_alt = "|".join(re.escape(term) + r"[^']*" for term in anchor_terms)
    return re.compile(
        rf"(?P<expr>(?P<ref>{label_expr})\s+LIKE\s+'%(?:{anchor_alt})%')",
        re.IGNORECASE,
    )


def _rewrite_label_filter_by_intent_profile(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    question_placeholder = _is_placeholder_question(question)
    cfg = load_sql_postprocess_rules().get("label_intent_rewrite", {})
    if not bool(cfg.get("enabled", True)):
        return text, rules
    profiles = _load_active_label_intent_profiles(question, cfg if isinstance(cfg, dict) else {})
    if not profiles:
        return text, rules

    changed = False
    for profile in profiles:
        if not isinstance(profile, dict):
            continue
        table_name = str(profile.get("table") or "D_ITEMS").strip().upper() or "D_ITEMS"
        event_table = str(profile.get("event_table") or "PROCEDUREEVENTS").strip().upper() or "PROCEDUREEVENTS"
        if not re.search(rf"\b{re.escape(table_name)}\b", text, re.IGNORECASE):
            continue
        if event_table and not re.search(rf"\b{re.escape(event_table)}\b", text, re.IGNORECASE):
            continue

        allow_sql_pattern_only = bool(profile.get("allow_sql_pattern_only", False))
        anchor_terms = _upper_tokens(profile.get("anchor_terms"))
        verb_terms = _upper_tokens(profile.get("co_terms")) or _upper_tokens(profile.get("insert_verb_terms"))
        required_terms = _upper_tokens(profile.get("required_terms_with_anchor"))
        exclude_terms = _upper_tokens(profile.get("exclude_terms_with_anchor"))
        block_re = _build_label_anchor_verb_block_re(anchor_terms, verb_terms)
        has_anchor_verb_block = bool(block_re.search(text)) if block_re else False

        question_any = [str(item).strip() for item in (profile.get("question_any") or []) if str(item).strip()]
        question_intent_any = [
            str(item).strip()
            for item in (profile.get("question_intent_any") or [])
            if str(item).strip()
        ]
        question_matched = True
        if not question_placeholder:
            if question_any and not _question_has_any_token(q, [item.upper() for item in question_any]):
                question_matched = False
            if question_intent_any and not _question_has_any_token(q, [item.upper() for item in question_intent_any]):
                question_matched = False
        if not question_matched and not (allow_sql_pattern_only and has_anchor_verb_block):
            continue

        if block_re and has_anchor_verb_block:
            rewritten = block_re.sub(lambda m: f"({str(m.group('anchor') or '').strip()})", text)
            if rewritten != text:
                changed = True
                text = rewritten
        inline_re = _build_label_anchor_verb_inline_re(anchor_terms, verb_terms)
        if inline_re:
            rewritten = inline_re.sub(lambda m: f"{str(m.group('anchor') or '').strip()}", text)
            if rewritten != text:
                changed = True
                text = rewritten

        normalize_groups = profile.get("normalize_or_groups")
        if isinstance(normalize_groups, list):
            for group in normalize_groups:
                if not isinstance(group, dict):
                    continue
                any_of = set(_upper_tokens(group.get("any_of")))
                target = str(group.get("to") or "").strip().upper()
                if not any_of or not target:
                    continue

                def _norm_or(match: re.Match) -> str:
                    ref = str(match.group("ref") or "").strip()
                    t1 = str(match.group("t1") or "").strip().upper()
                    t2 = str(match.group("t2") or "").strip().upper()
                    if not ref or not t1 or not t2:
                        return match.group(0)
                    if t1 in any_of and t2 in any_of:
                        return f"({ref} LIKE '%{target}%')"
                    return match.group(0)

                or_re = re.compile(
                    r"\(\s*(?P<ref>(?:UPPER\(\s*(?:[A-Za-z_][A-Za-z0-9_$#]*\.)?LABEL\s*\)|(?:[A-Za-z_][A-Za-z0-9_$#]*\.)?LABEL))\s+LIKE\s+'%(?P<t1>[^']+)%'\s+OR\s+"
                    r"(?P=ref)\s+LIKE\s+'%(?P<t2>[^']+)%'\s*\)",
                    re.IGNORECASE,
                )
                rewritten = or_re.sub(_norm_or, text)
                if rewritten != text:
                    changed = True
                    text = rewritten

        require_if_question_any = [
            str(item).strip()
            for item in (profile.get("require_if_question_any") or [])
            if str(item).strip()
        ]
        should_require = _question_has_any_token(q, [item.upper() for item in require_if_question_any])
        if not require_if_question_any:
            should_require = True
        if question_placeholder:
            should_require = True
        anchor_expr_re = _build_label_anchor_expr_re(anchor_terms)
        if should_require and required_terms:
            if anchor_expr_re:
                for required in required_terms:
                    if re.search(rf"LIKE\s+'%{re.escape(required)}%'", text, re.IGNORECASE):
                        continue

                    def _add_required(match: re.Match) -> str:
                        ref = str(match.group("ref") or "").strip()
                        expr = str(match.group("expr") or "").strip()
                        if not ref or not expr:
                            return match.group(0)
                        return f"({ref} LIKE '%{required}%' AND {expr})"

                    rewritten = anchor_expr_re.sub(_add_required, text, count=1)
                    if rewritten != text:
                        changed = True
                        text = rewritten
                        break

        if exclude_terms and anchor_expr_re:
            for excluded in exclude_terms:
                if re.search(rf"NOT\s+LIKE\s+'%{re.escape(excluded)}%'", text, re.IGNORECASE):
                    continue

                def _add_excluded(match: re.Match) -> str:
                    ref = str(match.group("ref") or "").strip()
                    expr = str(match.group("expr") or "").strip()
                    if not ref or not expr:
                        return match.group(0)
                    return f"({expr} AND {ref} NOT LIKE '%{excluded}%')"

                rewritten = anchor_expr_re.sub(_add_excluded, text, count=1)
                if rewritten != text:
                    changed = True
                    text = rewritten
                    break

    if changed:
        rules.append("rewrite_label_filter_by_intent_profile")
        return text, rules
    return text, rules


def _add_icd_version_for_prefix_filters(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    version_cfg = load_sql_postprocess_rules().get("icd_version_inference", {})
    if not bool(version_cfg.get("enabled", True)):
        return text, rules

    table_names_cfg = version_cfg.get("table_names")
    if isinstance(table_names_cfg, list):
        table_names = [str(item).strip().upper() for item in table_names_cfg if str(item).strip()]
    else:
        single = str(version_cfg.get("table_name") or "DIAGNOSES_ICD").strip().upper()
        table_names = [single] if single else ["DIAGNOSES_ICD"]
    version_column = str(version_cfg.get("version_column") or "ICD_VERSION").strip().upper() or "ICD_VERSION"
    predicate_template = str(
        version_cfg.get("predicate_template")
        or "({version_col} = {version} AND {code_expr} LIKE '{prefix}%')"
    )
    try:
        letter_version = int(version_cfg.get("letter_prefix_version", 10))
    except Exception:
        letter_version = 10
    try:
        digit_version = int(version_cfg.get("digit_prefix_version", 9))
    except Exception:
        digit_version = 9
    overrides_cfg = version_cfg.get("prefix_version_overrides")
    prefix_version_overrides: dict[str, int] = {}
    if isinstance(overrides_cfg, dict):
        for key, value in overrides_cfg.items():
            prefix = str(key).strip().upper()
            if not prefix:
                continue
            try:
                parsed = int(value)
            except Exception:
                continue
            prefix_version_overrides[prefix] = parsed

    def resolve_expected_version(prefix: str) -> int | None:
        normalized = prefix.strip().upper()
        if not normalized:
            return None
        for key in sorted(prefix_version_overrides.keys(), key=len, reverse=True):
            if normalized.startswith(key):
                return prefix_version_overrides[key]
        first = normalized[0]
        if first.isalpha():
            return letter_version
        if first.isdigit():
            return digit_version
        return None

    has_target_table = any(re.search(rf"\b{re.escape(table)}\b", text, re.IGNORECASE) for table in table_names)
    if not has_target_table:
        return text, rules
    if not _ICD_CODE_LIKE_RE.search(text):
        return text, rules

    changed = False

    def repl(match: re.Match) -> str:
        nonlocal changed
        lhs = match.group("lhs")
        prefix = match.group("prefix").strip()
        if not prefix:
            return match.group(0)
        alias = lhs.rsplit(".", 1)[0] if "." in lhs else ""
        nearby = text[max(0, match.start() - 80): min(len(text), match.end() + 80)]
        version_pred_re = re.compile(
            rf"(?:[A-Za-z0-9_]+\.)?{re.escape(version_column)}\s*=\s*(?:9|10)",
            re.IGNORECASE,
        )
        alias_version_pred_re = re.compile(
            rf"{re.escape(alias)}\.{re.escape(version_column)}\s*=\s*(?:9|10)",
            re.IGNORECASE,
        ) if alias else None
        if version_pred_re.search(nearby):
            if not alias or (alias_version_pred_re is not None and alias_version_pred_re.search(nearby)):
                return match.group(0)
        version = resolve_expected_version(prefix)
        if version is None:
            return match.group(0)
        version_col = f"{lhs.rsplit('.', 1)[0]}.{version_column}" if "." in lhs else version_column
        changed = True
        try:
            return predicate_template.format(
                version_col=version_col,
                version=version,
                code_expr=lhs,
                prefix=prefix,
            )
        except Exception:
            return f"({version_col} = {version} AND {lhs} LIKE '{prefix}%')"

    rewritten = _ICD_CODE_LIKE_RE.sub(repl, text)
    if changed and rewritten != text:
        rules.append("add_icd_version_to_prefix_filters")
        return rewritten, rules
    return text, rules


def _fix_icd_version_prefix_mismatch(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if "ICD_VERSION" not in text.upper() or "ICD_CODE" not in text.upper():
        return text, rules

    changed = False
    version_cfg = load_sql_postprocess_rules().get("icd_version_inference", {})
    try:
        letter_version = int(version_cfg.get("letter_prefix_version", 10))
    except Exception:
        letter_version = 10
    try:
        digit_version = int(version_cfg.get("digit_prefix_version", 9))
    except Exception:
        digit_version = 9
    overrides_cfg = version_cfg.get("prefix_version_overrides")
    prefix_version_overrides: dict[str, int] = {}
    if isinstance(overrides_cfg, dict):
        for key, value in overrides_cfg.items():
            prefix = str(key).strip().upper()
            if not prefix:
                continue
            try:
                parsed = int(value)
            except Exception:
                continue
            prefix_version_overrides[prefix] = parsed

    def expected_version(prefix: str) -> int | None:
        normalized = prefix.strip().upper()
        if not normalized:
            return None
        for key in sorted(prefix_version_overrides.keys(), key=len, reverse=True):
            if normalized.startswith(key):
                return prefix_version_overrides[key]
        head = normalized[0]
        if head.isalpha():
            return letter_version
        if head.isdigit():
            return digit_version
        return None

    def repl_vc(match: re.Match) -> str:
        nonlocal changed
        ver_col = match.group("ver_col")
        code_col = match.group("code_col")
        prefix = match.group("prefix")
        current_version = int(match.group("version"))
        target = expected_version(prefix)
        if target is None or target == current_version:
            return match.group(0)
        changed = True
        return f"{ver_col} = {target} AND {code_col} LIKE '{prefix}%'"

    def repl_cv(match: re.Match) -> str:
        nonlocal changed
        ver_col = match.group("ver_col")
        code_col = match.group("code_col")
        prefix = match.group("prefix")
        current_version = int(match.group("version"))
        target = expected_version(prefix)
        if target is None or target == current_version:
            return match.group(0)
        changed = True
        return f"{code_col} LIKE '{prefix}%' AND {ver_col} = {target}"

    rewritten = _ICD_VERSION_CODE_AND_RE.sub(repl_vc, text)
    rewritten2 = _ICD_CODE_VERSION_AND_RE.sub(repl_cv, rewritten)
    if changed and rewritten2 != text:
        rules.append("fix_icd_version_prefix_mismatch")
        return rewritten2, rules
    return text, rules


def _expand_diagnosis_prefixes_from_question(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    q = str(question or "").strip()
    text = str(sql or "")
    if not q or not text:
        return text, rules
    if not re.search(r"\bDIAGNOSES_ICD\b", text, re.IGNORECASE):
        return text, rules

    # Respect explicit user-specified code queries (e.g., "I50 코드").
    if _ICD_CODE_HINT_RE.search(q) and _EXPLICIT_ICD_PREFIX_RE.search(q):
        return text, rules

    mapped = match_diagnosis_mappings(q)
    if not mapped:
        return text, rules
    if len(mapped) < 2 and not _COMORBIDITY_HINT_RE.search(q):
        return text, rules

    version_cfg = load_sql_postprocess_rules().get("icd_version_inference", {})
    try:
        letter_version = int(version_cfg.get("letter_prefix_version", 10))
    except Exception:
        letter_version = 10
    try:
        digit_version = int(version_cfg.get("digit_prefix_version", 9))
    except Exception:
        digit_version = 9
    overrides_cfg = version_cfg.get("prefix_version_overrides")
    prefix_version_overrides: dict[str, int] = {}
    if isinstance(overrides_cfg, dict):
        for key, value in overrides_cfg.items():
            normalized_key = str(key).strip().upper()
            if not normalized_key:
                continue
            try:
                parsed = int(value)
            except Exception:
                continue
            prefix_version_overrides[normalized_key] = parsed

    def expected_version(prefix: str) -> int | None:
        normalized = str(prefix or "").strip().upper().replace(".", "")
        if not normalized:
            return None
        for key in sorted(prefix_version_overrides.keys(), key=len, reverse=True):
            if normalized.startswith(key):
                return prefix_version_overrides[key]
        head = normalized[0]
        if head.isalpha():
            return letter_version
        if head.isdigit():
            return digit_version
        return None

    prefix_to_entry: dict[str, dict[str, Any]] = {}
    for entry in mapped:
        prefixes = [
            str(prefix).strip().upper().replace(".", "")
            for prefix in entry.get("icd_prefixes", [])
            if str(prefix).strip()
        ]
        deduped: list[str] = []
        for prefix in prefixes:
            if prefix not in deduped:
                deduped.append(prefix)
        if len(deduped) <= 1:
            continue
        for prefix in deduped:
            prefix_to_entry.setdefault(prefix, {"term": entry.get("term"), "prefixes": deduped})
    if not prefix_to_entry:
        return text, rules

    changed = False
    expanded_terms: list[str] = []

    def _expand_clause(version_col: str, code_col: str, prefix: str) -> str | None:
        nonlocal changed
        normalized_prefix = str(prefix or "").strip().upper().replace(".", "")
        entry = prefix_to_entry.get(normalized_prefix)
        if not entry:
            return None
        prefixes = [str(item).strip().upper().replace(".", "") for item in entry.get("prefixes", []) if str(item).strip()]
        if normalized_prefix not in prefixes or len(prefixes) <= 1:
            return None

        predicates: list[str] = []
        for mapped_prefix in prefixes:
            version = expected_version(mapped_prefix)
            if version is None:
                predicate = f"{code_col} LIKE '{mapped_prefix}%'"
            else:
                predicate = f"({version_col} = {version} AND {code_col} LIKE '{mapped_prefix}%')"
            if predicate not in predicates:
                predicates.append(predicate)
        if len(predicates) <= 1:
            return None

        term = str(entry.get("term") or "").strip()
        if term and term not in expanded_terms:
            expanded_terms.append(term)
        changed = True
        return "(" + " OR ".join(predicates) + ")"

    def repl_vc(match: re.Match) -> str:
        rewritten = _expand_clause(match.group("ver_col"), match.group("code_col"), match.group("prefix"))
        return rewritten or match.group(0)

    def repl_cv(match: re.Match) -> str:
        rewritten = _expand_clause(match.group("ver_col"), match.group("code_col"), match.group("prefix"))
        return rewritten or match.group(0)

    rewritten = _ICD_VERSION_CODE_AND_RE.sub(repl_vc, text)
    rewritten2 = _ICD_CODE_VERSION_AND_RE.sub(repl_cv, rewritten)
    if changed and rewritten2 != text:
        suffix = ""
        if expanded_terms:
            suffix = ":" + ",".join(expanded_terms[:3])
        rules.append("expand_diagnosis_prefixes_from_question" + suffix)
        return rewritten2, rules
    return text, rules


def _extract_post_window_days(question: str) -> int | None:
    match_ko = _POST_WINDOW_KO_RE.search(question)
    if match_ko:
        for group in match_ko.groups():
            if group and group.isdigit():
                return int(group)
    match_en = _POST_WINDOW_EN_RE.search(question)
    if match_en and match_en.group(1).isdigit():
        return int(match_en.group(1))
    return None


def _rewrite_post_window_deathtime_anchor(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    cfg = load_sql_postprocess_rules().get("time_window_rewrite", {})
    if not bool(cfg.get("enabled", True)):
        return text, rules

    requested_days = _extract_post_window_days(question)
    if requested_days is None:
        return text, rules

    exclude_keywords_cfg = cfg.get("exclude_question_keywords")
    if isinstance(exclude_keywords_cfg, list):
        exclude_keywords = [str(item).lower() for item in exclude_keywords_cfg if str(item).strip()]
    else:
        exclude_keywords = ["퇴원 후", "퇴원후", "after discharge", "post-discharge"]
    lower_question = question.lower()
    if any(keyword in lower_question for keyword in exclude_keywords):
        return text, rules

    death_anchor_column = str(cfg.get("death_anchor_column") or "DEATHTIME").strip().upper() or "DEATHTIME"
    from_column = str(cfg.get("from_column") or "DISCHTIME").strip().upper() or "DISCHTIME"
    to_column = str(cfg.get("to_column") or "ADMITTIME").strip().upper() or "ADMITTIME"
    if death_anchor_column != "DEATHTIME" or from_column != "DISCHTIME":
        # Current pattern targets deathtime-from-dischtime anchor rewrites.
        return text, rules

    changed = False

    def repl(match: re.Match) -> str:
        nonlocal changed
        dis_expr = match.group("dis")
        days = int(match.group("days"))
        if days != requested_days:
            return match.group(0)
        changed = True
        target_expr = re.sub(rf"{from_column}\b", to_column, dis_expr, flags=re.IGNORECASE)
        return f"{match.group('death')} <= ({target_expr} + INTERVAL '{days}' DAY)"

    rewritten = _DEATHTIME_FROM_DISCHTIME_RE.sub(repl, text)
    if changed and rewritten != text:
        rules.append("rewrite_post_window_anchor_to_admittime")
        return rewritten, rules
    return text, rules


def _quote_to_char_format_literals(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    rewritten = _TO_CHAR_BARE_FMT_RE.sub(
        lambda m: f"TO_CHAR({m.group('expr').strip()}, '{m.group('fmt').upper()}')",
        text,
    )
    if rewritten != text:
        rules.append("quote_to_char_format_literal")
        return rewritten, rules
    return text, rules


def _postprocess_sql_relaxed(question: str, sql: str) -> tuple[str, list[str]]:
    """Apply low-risk SQL fixes only.

    This path is intended for first-pass execution to reduce over-correction.
    """
    rules: list[str] = []
    q = question.strip()

    mapped, map_rules = _apply_schema_mappings(sql)
    rules.extend(map_rules)

    service_mortality_fixed, service_mortality_rules = _rewrite_service_mortality_query(q, mapped)
    rules.extend(service_mortality_rules)

    icu_mortality_fixed, icu_mortality_rules = _rewrite_icu_mortality_outcome_alignment(q, service_mortality_fixed)
    rules.extend(icu_mortality_rules)

    first_icu_fixed, first_icu_rules = _rewrite_unrequested_first_icu_window(q, icu_mortality_fixed)
    rules.extend(first_icu_rules)

    rewritten_date_cast, date_cast_rules = _rewrite_to_date_cast(first_icu_fixed)
    rules.extend(date_cast_rules)

    rewritten_extract_day, extract_day_rules = _rewrite_extract_day_diff(rewritten_date_cast)
    rules.extend(extract_day_rules)

    rewritten_ts, ts_rules = _rewrite_timestampdiff(rewritten_extract_day)
    rules.extend(ts_rules)

    rewritten_ext, ext_rules = _rewrite_extract_year(rewritten_ts)
    rules.extend(ext_rules)

    timed, time_rules = _normalize_timestamp_diffs(rewritten_ext)
    rules.extend(time_rules)

    deduped, dedupe_rules = _dedupe_table_alias(timed)
    rules.extend(dedupe_rules)

    grouped, group_rules = _fix_orphan_by(deduped)
    rules.extend(group_rules)

    having_fixed, having_rules = _fix_having_where(grouped)
    rules.extend(having_rules)

    ratio_denom_fixed, ratio_denom_rules = _rewrite_ratio_denominator_distinct_under_icd_join(having_fixed)
    rules.extend(ratio_denom_rules)

    count_fixed, count_rules = _normalize_count_aliases_for_simple_counts(ratio_denom_fixed)
    rules.extend(count_rules)

    grouped_count_fixed, grouped_count_rules = _ensure_group_by_not_null_for_simple_counts(q, count_fixed)
    rules.extend(grouped_count_rules)

    grouped_avg_fixed, grouped_avg_rules = _ensure_group_by_not_null_for_simple_avg(q, grouped_count_fixed)
    rules.extend(grouped_avg_rules)

    avg_not_null_fixed, avg_not_null_rules = _ensure_avg_not_null(grouped_avg_fixed)
    rules.extend(avg_not_null_rules)

    avg_count_fixed, avg_count_rules = _rewrite_avg_count_alias(avg_not_null_fixed)
    rules.extend(avg_count_rules)

    avg_alias_fixed, avg_alias_rules = _normalize_avg_aliases(avg_count_fixed)
    rules.extend(avg_alias_rules)

    hadm_not_null_fixed, hadm_not_null_rules = _ensure_hadm_not_null_for_distinct_counts(avg_alias_fixed)
    rules.extend(hadm_not_null_rules)

    hadm_group_fixed, hadm_group_rules = _ensure_prescriptions_hadm_not_null_for_grouping(hadm_not_null_fixed)
    rules.extend(hadm_group_rules)

    # Keep relaxed mode intent-preserving: do not rewrite user-provided categorical
    # literals to nearest known values. This rewrite is reserved for aggressive mode.
    transfers_eventtype_fixed, transfers_eventtype_rules = _strip_transfers_eventtype_filter(q, hadm_group_fixed)
    rules.extend(transfers_eventtype_rules)

    nontransfers_eventtype_fixed, nontransfers_eventtype_rules = _strip_invalid_eventtype_filter_for_non_transfers(
        transfers_eventtype_fixed
    )
    rules.extend(nontransfers_eventtype_rules)

    admission_type_fixed, admission_type_rules = _strip_inpatient_admission_type_filter(q, nontransfers_eventtype_fixed)
    rules.extend(admission_type_rules)

    ordered_count_fixed, ordered_count_rules = _ensure_order_by_count(q, admission_type_fixed)
    rules.extend(ordered_count_rules)

    ordered, order_rules = _fix_order_by_bad_alias(ordered_count_fixed)
    rules.extend(order_rules)

    ordered2, order_suffix_rules = _fix_order_by_count_suffix(ordered)
    rules.extend(order_suffix_rules)

    update_stripped, update_rules = _strip_for_update(ordered2)
    rules.extend(update_rules)

    d_items_title_fixed, d_items_title_rules = _rewrite_d_items_long_title_to_label(update_stripped)
    rules.extend(d_items_title_rules)

    scalar_itemid_fixed, scalar_itemid_rules = _rewrite_itemid_scalar_subquery_to_safe_in(d_items_title_fixed)
    rules.extend(scalar_itemid_rules)

    label_like_fixed, label_like_rules = _rewrite_label_like_case_insensitive(scalar_itemid_fixed)
    rules.extend(label_like_rules)

    to_char_fixed, to_char_rules = _quote_to_char_format_literals(label_like_fixed)
    rules.extend(to_char_rules)

    rewritten, rewrite_rules = _rewrite_oracle_syntax(to_char_fixed)
    rules.extend(rewrite_rules)

    uncapped, uncap_rules = _strip_unrequested_top_n_cap(q, rewritten)
    rules.extend(uncap_rules)

    first_icu_window_fixed, first_icu_window_rules = _strip_first_icu_rownum_for_careunit_counts(q, uncapped)
    rules.extend(first_icu_window_rules)

    top_fixed, top_rules = _enforce_top_n_wrapper(q, first_icu_window_fixed)
    rules.extend(top_rules)

    monthly_capped, monthly_cap_rules = _apply_monthly_trend_default_cap(q, top_fixed)
    rules.extend(monthly_cap_rules)
    careunit_capped, careunit_cap_rules = _apply_first_careunit_default_cap(q, monthly_capped)
    rules.extend(careunit_cap_rules)
    return careunit_capped, rules


def _postprocess_sql_conservative(question: str, sql: str) -> tuple[str, list[str]]:
    # Conservative mode should remain close to model output: start with relaxed
    # fixes, then apply only a small set of high-signal semantic corrections.
    q = question.strip()
    text, rules = _postprocess_sql_relaxed(question, sql)

    diagnosis_map_fixed, diagnosis_map_rules = _rewrite_diagnosis_title_filter_with_icd_map(q, text)
    rules.extend(diagnosis_map_rules)

    procedure_map_fixed, procedure_map_rules = _rewrite_procedure_title_filter_with_icd_map(q, diagnosis_map_fixed)
    rules.extend(procedure_map_rules)

    diagnosis_prefix_expanded, diagnosis_prefix_expand_rules = _expand_diagnosis_prefixes_from_question(
        q,
        procedure_map_fixed,
    )
    rules.extend(diagnosis_prefix_expand_rules)

    icd_version_consistent, icd_version_consistent_rules = _fix_icd_version_prefix_mismatch(diagnosis_prefix_expanded)
    rules.extend(icd_version_consistent_rules)

    icd_version_fixed, icd_version_rules = _add_icd_version_for_prefix_filters(icd_version_consistent)
    rules.extend(icd_version_rules)

    mortality_rate_fixed, mortality_rate_rules = _rewrite_mortality_avg_under_icd_join(icd_version_fixed)
    rules.extend(mortality_rate_rules)

    ratio_denom_fixed, ratio_denom_rules = _rewrite_ratio_denominator_distinct_under_icd_join(mortality_rate_fixed)
    rules.extend(ratio_denom_rules)

    window_fixed, window_rules = _rewrite_post_window_deathtime_anchor(q, ratio_denom_fixed)
    rules.extend(window_rules)

    admissions_icd_grain_fixed, admissions_icd_grain_rules = _rewrite_admissions_icd_count_grain(q, window_fixed)
    rules.extend(admissions_icd_grain_rules)

    age_gender_extrema_fixed, age_gender_extrema_rules = _rewrite_age_group_diagnosis_extrema_by_gender(
        q,
        admissions_icd_grain_fixed,
    )
    rules.extend(age_gender_extrema_rules)

    first_icu_window_fixed, first_icu_window_rules = _strip_first_icu_rownum_for_careunit_counts(q, age_gender_extrema_fixed)
    rules.extend(first_icu_window_rules)

    monthly_capped, monthly_cap_rules = _apply_monthly_trend_default_cap(q, first_icu_window_fixed)
    rules.extend(monthly_cap_rules)
    careunit_capped, careunit_cap_rules = _apply_first_careunit_default_cap(q, monthly_capped)
    rules.extend(careunit_cap_rules)
    return careunit_capped, rules


def postprocess_sql(question: str, sql: str, profile: str | None = None) -> tuple[str, list[str]]:
    rules: list[str] = []
    q = question.strip()
    profile_mode = str(profile or "auto").strip().lower()
    if profile_mode not in {"auto", "relaxed", "aggressive"}:
        profile_mode = "auto"

    learned_fix = find_learned_sql_fix(sql)
    if isinstance(learned_fix, dict):
        fixed_sql = str(learned_fix.get("fixed_sql") or "").strip()
        if fixed_sql and fixed_sql.strip().rstrip(";") != str(sql).strip().rstrip(";"):
            sql = fixed_sql
            rule_id = str(learned_fix.get("id") or "").strip()
            if rule_id:
                mark_learned_sql_fix_used(rule_id)
                rules.append(f"learned_error_fix:{rule_id}")
            else:
                rules.append("learned_error_fix")

    match = _COUNT_RE.match(q)
    if match:
        table = match.group(1)
        rules.append("count_rows_sampled_template")
        return f"SELECT COUNT(*) AS cnt FROM {table} WHERE ROWNUM <= 1000", rules

    match = _DISTINCT_RE.match(q)
    if match:
        col = match.group(1)
        table = match.group(2)
        rules.append("distinct_sample_template")
        return f"SELECT DISTINCT {col} FROM {table} WHERE {col} IS NOT NULL AND ROWNUM <= 50", rules

    match = _SAMPLE_RE.match(q)
    if match:
        table = match.group(1)
        cols = _parse_columns(match.group(2))
        first = _first(cols)
        if cols and first:
            cols_sql = ", ".join(cols)
            rules.append("sample_rows_template")
            return (
                f"SELECT {cols_sql} FROM {table} WHERE {first} IS NOT NULL AND ROWNUM <= 100",
                rules,
            )

    ko_sample_sql, ko_sample_rules = _build_ko_sample_template(q)
    if ko_sample_sql:
        rules.extend(ko_sample_rules)
        return ko_sample_sql, rules

    execution_cfg = load_sql_postprocess_rules().get("execution", {})
    mode = str(execution_cfg.get("mode") or "conservative").strip().lower()
    if mode not in {"full", "conservative"}:
        mode = "conservative"

    if profile_mode == "relaxed":
        relaxed_sql, relaxed_rules = _postprocess_sql_relaxed(q, sql)
        rules.extend(relaxed_rules)
        return relaxed_sql, rules

    if profile_mode != "aggressive" and mode == "conservative":
        conservative_sql, conservative_rules = _postprocess_sql_conservative(q, sql)
        rules.extend(conservative_rules)
        return conservative_sql, rules

    mapped, map_rules = _apply_schema_mappings(sql)
    rules.extend(map_rules)

    service_mortality_fixed, service_mortality_rules = _rewrite_service_mortality_query(q, mapped)
    rules.extend(service_mortality_rules)

    icu_mortality_fixed, icu_mortality_rules = _rewrite_icu_mortality_outcome_alignment(q, service_mortality_fixed)
    rules.extend(icu_mortality_rules)

    first_icu_fixed, first_icu_rules = _rewrite_unrequested_first_icu_window(q, icu_mortality_fixed)
    rules.extend(first_icu_rules)

    micro_fixed, micro_rules = _ensure_microbiology_table(first_icu_fixed)
    rules.extend(micro_rules)

    micro_by_q, micro_q_rules = _ensure_microbiology_by_question(q, micro_fixed)
    rules.extend(micro_q_rules)

    icu_forced, icu_force_rules = _ensure_icustays_table(q, micro_by_q)
    rules.extend(icu_force_rules)

    chart_forced, chart_rules = _ensure_chartevents_table(q, icu_forced)
    rules.extend(chart_rules)

    lab_forced, lab_rules = _ensure_labevents_table(q, chart_forced)
    rules.extend(lab_rules)

    services_forced, services_rules = _ensure_services_table(q, lab_forced)
    rules.extend(services_rules)

    prescriptions_forced, prescriptions_rules = _ensure_prescriptions_table(q, services_forced)
    rules.extend(prescriptions_rules)

    input_forced, input_rules = _ensure_inputevents_table(q, prescriptions_forced)
    rules.extend(input_rules)

    output_forced, output_rules = _ensure_outputevents_table(q, input_forced)
    rules.extend(output_rules)

    emar_forced, emar_rules = _ensure_emar_table(q, output_forced)
    rules.extend(emar_rules)

    diagnoses_forced, diagnoses_rules = _ensure_diagnoses_icd_table(q, emar_forced)
    rules.extend(diagnoses_rules)

    procedures_forced, procedures_rules = _ensure_procedures_icd_table(q, diagnoses_forced)
    rules.extend(procedures_rules)

    prescriptions_field_fixed, prescriptions_field_rules = _rewrite_prescriptions_drug_field(q, procedures_forced)
    rules.extend(prescriptions_field_rules)

    prescriptions_col_fixed, prescriptions_col_rules = _rewrite_prescriptions_columns(prescriptions_field_fixed)
    rules.extend(prescriptions_col_rules)

    icd_code_fixed, icd_code_rules = _rewrite_icd_code_field(q, prescriptions_col_fixed)
    rules.extend(icd_code_rules)

    icd_itemid_fixed, icd_itemid_rules = _rewrite_itemid_in_icd_tables(icd_code_fixed)
    rules.extend(icd_itemid_rules)

    emar_field_fixed, emar_field_rules = _rewrite_emar_medication_field(q, icd_itemid_fixed)
    rules.extend(emar_field_rules)

    transfers_fixed, transfers_rules = _ensure_transfers_eventtype(q, emar_field_fixed)
    rules.extend(transfers_rules)

    transfers_careunit_fixed, transfers_careunit_rules = _rewrite_transfers_careunit_fields(transfers_fixed)
    rules.extend(transfers_careunit_rules)

    services_order_fixed, services_order_rules = _rewrite_services_order_type(q, transfers_careunit_fixed)
    rules.extend(services_order_rules)

    transfers_eventtype_fixed, transfers_eventtype_rules = _strip_transfers_eventtype_filter(q, services_order_fixed)
    rules.extend(transfers_eventtype_rules)

    rewritten_icu, icu_rules = _rewrite_has_icu_stay(transfers_eventtype_fixed)
    rules.extend(icu_rules)

    rewritten_icu2, icu2_rules = _rewrite_icu_stay(rewritten_icu)
    rules.extend(icu2_rules)

    rewritten_icu3, icu3_rules = _rewrite_icustays_flag(rewritten_icu2)
    rules.extend(icu3_rules)

    rewritten_icu4, icu4_rules = _rewrite_icustays_not_null(rewritten_icu3)
    rules.extend(icu4_rules)

    rewritten_adm_len, adm_len_rules = _rewrite_admission_length(rewritten_icu4)
    rules.extend(adm_len_rules)

    rewritten_dur, dur_rules = _rewrite_duration(rewritten_adm_len)
    rules.extend(dur_rules)

    rewritten_date_cast, date_cast_rules = _rewrite_to_date_cast(rewritten_dur)
    rules.extend(date_cast_rules)

    rewritten_extract_day, extract_day_rules = _rewrite_extract_day_diff(rewritten_date_cast)
    rules.extend(extract_day_rules)

    rewritten_ts, ts_rules = _rewrite_timestampdiff(rewritten_extract_day)
    rules.extend(ts_rules)

    rewritten_ext, ext_rules = _rewrite_extract_year(rewritten_ts)
    rules.extend(ext_rules)

    joined_adm, adm_rules = _ensure_admissions_join(rewritten_ext)
    rules.extend(adm_rules)

    year_range_fixed, year_range_rules = _rewrite_absolute_year_range(q, joined_adm)
    rules.extend(year_range_rules)

    age_from_diff_fixed, age_from_diff_rules = _rewrite_age_from_sysdate_diff(year_range_fixed)
    rules.extend(age_from_diff_rules)

    joined_patients, patient_rules = _ensure_patients_join(age_from_diff_fixed)
    rules.extend(patient_rules)

    rewritten_patients_id, patient_id_rules = _rewrite_patients_id(joined_patients)
    rules.extend(patient_id_rules)

    joined_icd, icd_rules = _ensure_icd_join(question, rewritten_patients_id)
    rules.extend(icd_rules)

    labeled, label_rules = _ensure_label_join(joined_icd)
    rules.extend(label_rules)

    diag_title_fixed, diag_title_rules = _ensure_diagnosis_title_join(q, labeled)
    rules.extend(diag_title_rules)

    proc_title_fixed, proc_title_rules = _ensure_procedure_title_join(q, diag_title_fixed)
    rules.extend(proc_title_rules)

    proc_cleanup_fixed, proc_cleanup_rules = _cleanup_procedure_title_joins(proc_title_fixed)
    rules.extend(proc_cleanup_rules)

    titled, title_rules = _ensure_long_title_join(proc_cleanup_fixed)
    rules.extend(title_rules)

    diagnosis_map_fixed, diagnosis_map_rules = _rewrite_diagnosis_title_filter_with_icd_map(q, titled)
    rules.extend(diagnosis_map_rules)

    procedure_map_fixed, procedure_map_rules = _rewrite_procedure_title_filter_with_icd_map(q, diagnosis_map_fixed)
    rules.extend(procedure_map_rules)

    diagnosis_prefix_expanded, diagnosis_prefix_expand_rules = _expand_diagnosis_prefixes_from_question(
        q,
        procedure_map_fixed,
    )
    rules.extend(diagnosis_prefix_expand_rules)

    icd_version_consistent, icd_version_consistent_rules = _fix_icd_version_prefix_mismatch(diagnosis_prefix_expanded)
    rules.extend(icd_version_consistent_rules)

    icd_version_fixed, icd_version_rules = _add_icd_version_for_prefix_filters(icd_version_consistent)
    rules.extend(icd_version_rules)

    mortality_rate_fixed, mortality_rate_rules = _rewrite_mortality_avg_under_icd_join(icd_version_fixed)
    rules.extend(mortality_rate_rules)

    ratio_denom_fixed, ratio_denom_rules = _rewrite_ratio_denominator_distinct_under_icd_join(mortality_rate_fixed)
    rules.extend(ratio_denom_rules)

    window_fixed, window_rules = _rewrite_post_window_deathtime_anchor(q, ratio_denom_fixed)
    rules.extend(window_rules)

    timed, time_rules = _normalize_timestamp_diffs(window_fixed)
    rules.extend(time_rules)

    deduped, dedupe_rules = _dedupe_table_alias(timed)
    rules.extend(dedupe_rules)

    grouped, group_rules = _fix_orphan_by(deduped)
    rules.extend(group_rules)

    having_fixed, having_rules = _fix_having_where(grouped)
    rules.extend(having_rules)

    aligned_icu_keys, align_icu_rules = _align_admissions_icu_match_keys(having_fixed)
    rules.extend(align_icu_rules)

    admissions_icd_grain_fixed, admissions_icd_grain_rules = _rewrite_admissions_icd_count_grain(q, aligned_icu_keys)
    rules.extend(admissions_icd_grain_rules)

    gender_template_fixed, gender_template_rules = _rewrite_count_by_gender_template(q, admissions_icd_grain_fixed)
    rules.extend(gender_template_rules)

    expire_fixed, expire_rules = _rewrite_hospital_expire_flag(gender_template_fixed)
    rules.extend(expire_rules)

    age_fixed, age_rules = _rewrite_age_from_anchor(expire_fixed)
    rules.extend(age_rules)

    birth_fixed, birth_rules = _rewrite_age_from_birthdate(age_fixed)
    rules.extend(birth_rules)

    birth_col_fixed, birth_col_rules = _rewrite_birthdate_to_anchor_age(birth_fixed)
    rules.extend(birth_col_rules)

    birth_year_fixed, birth_year_rules = _rewrite_birth_year_age(birth_col_fixed)
    rules.extend(birth_year_rules)

    age_gender_extrema_fixed, age_gender_extrema_rules = _rewrite_age_group_diagnosis_extrema_by_gender(
        q,
        birth_year_fixed,
    )
    rules.extend(age_gender_extrema_rules)

    icu_careunit_fixed, icu_careunit_rules = _rewrite_icustays_careunit(q, age_gender_extrema_fixed)
    rules.extend(icu_careunit_rules)

    icu_los_fixed, icu_los_rules = _rewrite_icustays_los(icu_careunit_fixed)
    rules.extend(icu_los_rules)

    warning_fixed, warning_rules = _rewrite_warning_flag(q, icu_los_fixed)
    rules.extend(warning_rules)

    lab_priority_fixed, lab_priority_rules = _rewrite_lab_priority(q, warning_fixed)
    rules.extend(lab_priority_rules)

    micro_field_fixed, micro_field_rules = _rewrite_micro_count_field(q, lab_priority_fixed)
    rules.extend(micro_field_rules)

    chart_label_fixed, chart_label_rules = _ensure_chart_label(q, micro_field_fixed)
    rules.extend(chart_label_rules)

    lab_label_fixed, lab_label_rules = _ensure_lab_label(q, chart_label_fixed)
    rules.extend(lab_label_rules)

    label_field_fixed, label_field_rules = _rewrite_label_field(q, lab_label_fixed)
    rules.extend(label_field_rules)

    count_fixed, count_rules = _normalize_count_aliases(label_field_fixed)
    rules.extend(count_rules)

    avg_fixed, avg_rules = _rewrite_avg_count_alias(count_fixed)
    rules.extend(avg_rules)

    avg_alias_fixed, avg_alias_rules = _normalize_avg_aliases(avg_fixed)
    rules.extend(avg_alias_rules)

    hadm_not_null_fixed, hadm_not_null_rules = _ensure_hadm_not_null_for_distinct_counts(avg_alias_fixed)
    rules.extend(hadm_not_null_rules)

    hadm_group_fixed, hadm_group_rules = _ensure_prescriptions_hadm_not_null_for_grouping(hadm_not_null_fixed)
    rules.extend(hadm_group_rules)

    admissions_count_fixed, admissions_count_rules = _rewrite_prescriptions_hadm_count_to_admissions_exists(
        q,
        hadm_group_fixed,
    )
    rules.extend(admissions_count_rules)

    services_adm_fixed, services_adm_rules = _rewrite_services_hadm_count_to_admissions_join(
        q,
        admissions_count_fixed,
    )
    rules.extend(services_adm_rules)

    time_stripped, time_rules = _strip_time_window_if_absent(q, services_adm_fixed)
    rules.extend(time_rules)

    grouped_filtered, group_filter_rules = _ensure_group_by_not_null(q, time_stripped)
    rules.extend(group_filter_rules)

    avg_not_null_fixed, avg_not_null_rules = _ensure_avg_not_null(grouped_filtered)
    rules.extend(avg_not_null_rules)

    admission_type_fixed, admission_type_rules = _strip_inpatient_admission_type_filter(q, avg_not_null_fixed)
    rules.extend(admission_type_rules)

    ordered, order_rules = _ensure_order_by_count(q, admission_type_fixed)
    rules.extend(order_rules)

    ordered2, order_alias_rules = _fix_order_by_bad_alias(ordered)
    rules.extend(order_alias_rules)

    ordered3, order_suffix_rules = _fix_order_by_count_suffix(ordered2)
    rules.extend(order_suffix_rules)

    update_stripped, update_rules = _strip_for_update(ordered3)
    rules.extend(update_rules)

    # Do not replace whole SQL by keyword-triggered canonical templates here.
    # Postprocess should only normalize/fix generated SQL.
    reordered, reorder_rules = _reorder_count_select(update_stripped)
    rules.extend(reorder_rules)

    avg_reordered, avg_reorder_rules = _reorder_avg_select(reordered)
    rules.extend(avg_reorder_rules)

    wrapped, wrap_rules = _wrap_top_n(q, avg_reordered)
    rules.extend(wrap_rules)

    # Disable automatic ROWNUM capping; preserve explicit Top-N only.
    capped = wrapped

    micro_cap_fixed, micro_cap_rules = _strip_rownum_cap_for_micro_topk(capped)
    rules.extend(micro_cap_rules)

    group_cap_fixed, group_cap_rules = _strip_rownum_cap_for_grouped_tables(micro_cap_fixed)
    rules.extend(group_cap_rules)

    pushed_fixed, pushed_rules = _pushdown_outer_predicates(group_cap_fixed)
    rules.extend(pushed_rules)

    missing_where_fixed, missing_where_rules = _fix_missing_where_predicate(pushed_fixed)
    rules.extend(missing_where_rules)

    categorical_fixed, categorical_rules = _rewrite_unknown_categorical_equals(q, missing_where_fixed)
    rules.extend(categorical_rules)

    transfers_eventtype_final, transfers_eventtype_final_rules = _strip_transfers_eventtype_filter(q, categorical_fixed)
    rules.extend(transfers_eventtype_final_rules)

    nontransfers_eventtype_final, nontransfers_eventtype_final_rules = _strip_invalid_eventtype_filter_for_non_transfers(
        transfers_eventtype_final
    )
    rules.extend(nontransfers_eventtype_final_rules)

    d_items_title_fixed, d_items_title_rules = _rewrite_d_items_long_title_to_label(nontransfers_eventtype_final)
    rules.extend(d_items_title_rules)

    scalar_itemid_fixed, scalar_itemid_rules = _rewrite_itemid_scalar_subquery_to_safe_in(d_items_title_fixed)
    rules.extend(scalar_itemid_rules)

    itemid_icd_fixed, itemid_icd_rules = _rewrite_itemid_icd_join_mismatch(scalar_itemid_fixed)
    rules.extend(itemid_icd_rules)

    label_like_fixed, label_like_rules = _rewrite_label_like_case_insensitive(itemid_icd_fixed)
    rules.extend(label_like_rules)

    label_profile_fixed, label_profile_rules = _rewrite_label_filter_by_intent_profile(q, label_like_fixed)
    rules.extend(label_profile_rules)

    to_char_fixed, to_char_rules = _quote_to_char_format_literals(label_profile_fixed)
    rules.extend(to_char_rules)

    rewritten, rewrite_rules = _rewrite_oracle_syntax(to_char_fixed)
    rules.extend(rewrite_rules)
    ratio_fixed, ratio_rules = _rewrite_count_columns_to_ratio_by_intent(q, rewritten)
    rules.extend(ratio_rules)
    uncapped, uncap_rules = _strip_unrequested_top_n_cap(q, ratio_fixed)
    rules.extend(uncap_rules)
    first_icu_window_fixed, first_icu_window_rules = _strip_first_icu_rownum_for_careunit_counts(q, uncapped)
    rules.extend(first_icu_window_rules)

    top_fixed, top_rules = _enforce_top_n_wrapper(q, first_icu_window_fixed)
    rules.extend(top_rules)

    monthly_capped, monthly_cap_rules = _apply_monthly_trend_default_cap(q, top_fixed)
    rules.extend(monthly_cap_rules)
    careunit_capped, careunit_cap_rules = _apply_first_careunit_default_cap(q, monthly_capped)
    rules.extend(careunit_cap_rules)
    return careunit_capped, rules
