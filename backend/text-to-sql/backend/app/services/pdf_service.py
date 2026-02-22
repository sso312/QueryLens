# app/services/pdf_service.py
import os
import fitz  # PyMuPDF
import json
import logging
import traceback
import hashlib
from pathlib import Path
from datetime import datetime
from openai import AsyncOpenAI
import re
import base64
from difflib import get_close_matches
from typing import Any

from app.services.runtime.state_store import get_state_store
from app.services.oracle.executor import execute_sql
from app.services.agents.orchestrator import run_oneshot
from app.services.cohort_adaptive import run_adaptive_extraction
from app.services.cohort_adaptive.upgrade_rules import (
    should_upgrade_to_accurate,
    should_upgrade_to_strict,
)
from app.services.cohort_ambiguity import resolve_ambiguities
from app.services.cohort_spec import (
    enforce_condition_evidence,
    validate_cohort_spec,
    validate_supported_types,
)
from app.services.cohort_spec.type_catalog import (
    has_icd_shorthand_risk,
    has_measurement_required,
)
from app.services.cohort_sql_compiler import apply_oracle_compiler_guards
from app.services.cohort_validate.report import build_validation_markdown
from app.services.cohort_validate.validator import summarize_validation

logger = logging.getLogger(__name__)
# 로깅 설정을 보장하기 위해 출력 핸들러 강제 추가
if not logger.handlers:
    import sys
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(sh)
logger.setLevel(logging.INFO)

_SAVED_COHORTS_KEY = "cohort::saved"
_PDF_CACHE_KEY = "pdf_extraction::cache"

# 사용할 테이블 목록 (schema_catalog.json 기반)
_COHORT_TABLES = [
    "PATIENTS", "ADMISSIONS", "DIAGNOSES_ICD", "ICUSTAYS",
    "D_ICD_DIAGNOSES", "D_ICD_PROCEDURES", "PROCEDURES_ICD",
    "LABEVENTS", "D_LABITEMS", "PRESCRIPTIONS", "TRANSFERS",
]

_SCHEMA_CATALOG_PATH = Path(os.getenv("SCHEMA_CATALOG_PATH", "/app/var/metadata/schema_catalog.json"))
_DERIVED_VAR_PATH = Path(os.getenv("DERIVED_VAR_PATH", "/app/var/metadata/derived_variables.json"))
_JOIN_GRAPH_PATH = Path(os.getenv("JOIN_GRAPH_PATH", "/app/var/metadata/join_graph.json"))

# 대체 경로 (로컬 개발용)
_METADATA_LOCAL_BASE = Path(__file__).resolve().parents[3] / "var" / "metadata"
_SCHEMA_CATALOG_LOCAL = _METADATA_LOCAL_BASE / "schema_catalog.json"
_DERIVED_VAR_LOCAL = _METADATA_LOCAL_BASE / "derived_variables.json"
_JOIN_GRAPH_LOCAL = _METADATA_LOCAL_BASE / "join_graph.json"

_SIGNAL_NAME_ALIASES: dict[str, str] = {
    "temp": "body_temperature",
    "temperature": "body_temperature",
    "body_temp": "body_temperature",
    "bodytemperature": "body_temperature",
    "bun_level": "bun",
    "blood_urea_nitrogen": "bun",
    "blood_urea_nitrogen_level": "bun",
    "urea_nitrogen": "bun",
    "serum_bun": "bun",
    "cr": "creatinine",
    "creat": "creatinine",
    "serum_creatinine": "creatinine",
    "po2": "pao2",
    "pa_o2": "pao2",
    "partial_pressure_o2": "pao2",
    "arterial_o2_tension": "pao2",
    "blood_ph": "ph",
    "arterial_ph": "ph",
    "ph_value": "ph",
    "anion_gap_level": "anion_gap",
    "uop": "urine_output",
    "uo": "urine_output",
    "urine": "urine_output",
    "urine_out": "urine_output",
    "urine_volume": "urine_output",
    "sex": "gender",
    "hospital_length_of_stay": "hospital_los",
    "length_of_hospital_stay": "hospital_los",
    "hospital_los_days": "hospital_los",
    "hosp_los": "hospital_los",
    "icu_length_of_stay": "icu_los",
    "length_of_icu_stay": "icu_los",
    "icu_stay_length": "icu_los",
    "icu_los_days": "icu_los",
    "in_hospital_death": "in_hospital_mortality",
    "inhospital_mortality": "in_hospital_mortality",
    "hospital_expire_flag": "in_hospital_mortality",
}

_RESULT_IDENTIFIER_COLUMNS = {"SUBJECT_ID", "HADM_ID", "STAY_ID"}


def _env_int(
    name: str,
    default: int,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    raw = str(os.getenv(name, "")).strip()
    try:
        value = int(raw) if raw else int(default)
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _env_mode(name: str, default: str, allowed: set[str]) -> str:
    value = str(os.getenv(name, default)).strip().lower()
    return value if value in allowed else default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


_PDF_MAX_PAGES = _env_int("PDF_MAX_PAGES", 7, minimum=1, maximum=50)
_PDF_MAX_TEXT_CHARS = _env_int("PDF_MAX_TEXT_CHARS", 22000, minimum=4000, maximum=120000)
_PDF_ASSET_TABLE_ROWS = _env_int("PDF_ASSET_TABLE_ROWS", 20, minimum=1, maximum=100)
_PDF_ASSET_TABLE_COLS = _env_int("PDF_ASSET_TABLE_COLS", 8, minimum=1, maximum=30)
_PDF_ASSET_TABLE_CHARS = _env_int("PDF_ASSET_TABLE_CHARS", 2500, minimum=300, maximum=20000)
_PDF_RAG_REFINEMENT_MODE = _env_mode(
    "PDF_RAG_REFINEMENT_MODE",
    "auto",
    {"auto", "always", "off"},
)
_PDF_SNIPPET_MAX_COUNT = _env_int("PDF_SNIPPET_MAX_COUNT", 30, minimum=5, maximum=120)
_PDF_VALIDATION_ENABLED = _env_bool("PDF_VALIDATION_ENABLED", True)
_PDF_VALIDATION_SAMPLE_ROWS = _env_int("PDF_VALIDATION_SAMPLE_ROWS", 5, minimum=1, maximum=20)
_PDF_STRICT_AMBIGUITY_MODE = _env_bool("PDF_STRICT_AMBIGUITY_MODE", False)
_PDF_ACCURACY_MODE_DEFAULT = _env_bool("PDF_ACCURACY_MODE", False)
_PDF_ACCURACY_REPAIR_MAX_ROUNDS = _env_int("PDF_ACCURACY_REPAIR_MAX_ROUNDS", 2, minimum=1, maximum=3)
_PDF_ACCURACY_NEGATIVE_SAMPLE_N = _env_int("PDF_ACCURACY_NEGATIVE_SAMPLE_N", 50, minimum=5, maximum=200)
_PDF_ACCURACY_INCLUDE_TABLES = _env_bool("PDF_ACCURACY_INCLUDE_TABLES", True)
_PDF_ACCURACY_TABLE_CAPTURE_CHARS = _env_int("PDF_ACCURACY_TABLE_CAPTURE_CHARS", 6000, minimum=800, maximum=20000)
_PDF_ACCURACY_SECTION_KEYWORDS_EXPANDED = _env_bool("PDF_ACCURACY_SECTION_KEYWORDS_EXPANDED", True)
_PDF_ACCURACY_FAIL_FAST_ON_INVARIANTS = _env_bool("PDF_ACCURACY_FAIL_FAST_ON_INVARIANTS", True)
_PDF_SCHEMA_MAP_PATH = Path(os.getenv("PDF_SCHEMA_MAP_PATH", "/app/var/metadata/pdf_schema_map.json"))
_PDF_SCHEMA_MAP_LOCAL = _METADATA_LOCAL_BASE / "pdf_schema_map.json"


class _SkipRagRefinement(Exception):
    pass


def _normalize_signal_name(value: Any) -> str:
    if isinstance(value, (list, tuple, set)):
        for item in value:
            normalized = _normalize_signal_name(item)
            if normalized:
                return normalized
        return ""
    if isinstance(value, dict):
        for key in ("signal", "name", "label", "value", "text"):
            if key in value:
                normalized = _normalize_signal_name(value.get(key))
                if normalized:
                    return normalized
        return ""
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    key = re.sub(r"[^a-z0-9]+", "_", raw)
    key = re.sub(r"_+", "_", key).strip("_")
    return _SIGNAL_NAME_ALIASES.get(key, key)


def _load_metadata_json(path_env: Path, path_local: Path) -> dict:
    path = path_env if path_env.exists() else path_local
    if not path.exists():
        logger.warning(f"Metadata file not found: {path}")
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Failed to load metadata {path}: {e}")
        return {}


def _load_schema_for_prompt() -> str:
    """schema_catalog.json에서 테이블/컬럼 정보를 읽어 프롬프트용 텍스트 생성"""
    catalog_path = _SCHEMA_CATALOG_PATH if _SCHEMA_CATALOG_PATH.exists() else _SCHEMA_CATALOG_LOCAL

    if not catalog_path.exists():
        logger.warning("schema_catalog.json을 찾을 수 없습니다: %s", catalog_path)
        return _fallback_schema()

    try:
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("schema_catalog.json 파싱 실패: %s", e)
        return _fallback_schema()

    tables = catalog.get("tables", {})
    lines = []
    for tname in _COHORT_TABLES:
        tinfo = tables.get(tname)
        if not tinfo:
            continue
        cols = tinfo.get("columns", [])
        col_parts = []
        for c in cols:
            cname = c.get("name", "")
            ctype = c.get("type", "")
            col_parts.append(f"{cname} {ctype}")
        pks = tinfo.get("primary_keys", [])
        pk_text = f" [PK: {', '.join(pks)}]" if pks else ""
        lines.append(f"- {tname} ({', '.join(col_parts)}){pk_text}")

    return "\n".join(lines)


def _fallback_schema() -> str:
    """schema_catalog.json 없을 때 사용하는 하드코딩 스키마"""
    return """- PATIENTS (SUBJECT_ID NUMBER, GENDER VARCHAR2, ANCHOR_AGE NUMBER, DOD TIMESTAMP)
- ADMISSIONS (HADM_ID NUMBER, SUBJECT_ID NUMBER, ADMITTIME TIMESTAMP(6), DISCHTIME TIMESTAMP(6), DEATHTIME TIMESTAMP(6), ADMISSION_TYPE VARCHAR2, ADMIT_PROVIDER_ID VARCHAR2, ADMISSION_LOCATION VARCHAR2, DISCHARGE_LOCATION VARCHAR2, INSURANCE VARCHAR2, LANGUAGE VARCHAR2, MARITAL_STATUS VARCHAR2, RACE VARCHAR2, EDREGTIME TIMESTAMP(6), EDOUTTIME TIMESTAMP(6), HOSPITAL_EXPIRE_FLAG NUMBER) [PK: HADM_ID]
- DIAGNOSES_ICD (SUBJECT_ID NUMBER, HADM_ID NUMBER, SEQ_NUM NUMBER, ICD_CODE CHAR, ICD_VERSION NUMBER)
- ICUSTAYS (SUBJECT_ID NUMBER, HADM_ID NUMBER, STAY_ID NUMBER, FIRST_CAREUNIT VARCHAR2, LAST_CAREUNIT VARCHAR2, INTIME TIMESTAMP(0), OUTTIME TIMESTAMP(0), LOS NUMBER) [PK: STAY_ID]
- D_ICD_DIAGNOSES (ICD_CODE CHAR, ICD_VERSION NUMBER, LONG_TITLE VARCHAR2) [PK: ICD_CODE, ICD_VERSION]"""


def _load_valid_columns() -> dict[str, set[str]]:
    """schema_catalog.json에서 테이블별 유효 컬럼명 집합을 로드"""
    catalog_path = _SCHEMA_CATALOG_PATH if _SCHEMA_CATALOG_PATH.exists() else _SCHEMA_CATALOG_LOCAL
    if not catalog_path.exists():
        return {}
    try:
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    result = {}
    for tname, tinfo in catalog.get("tables", {}).items():
        cols = {c["name"].upper() for c in tinfo.get("columns", []) if c.get("name")}
        result[tname.upper()] = cols
    return result


def _fix_column_names_in_sql(sql: str) -> tuple[str, list[str]]:
    """
    SQL에서 alias.COLUMN 패턴을 찾아 실제 스키마와 대조하고, 
    유사한 컬럼명으로 자동 수정.
    """
    valid_cols = _load_valid_columns()
    if not valid_cols:
        return sql, []

    # SQL에서 테이블 alias → 실제 테이블명 매핑 구축
    alias_map: dict[str, str] = {}
    # FROM/JOIN TABLENAME alias 패턴
    for match in re.finditer(
        r'(?:FROM|JOIN)\s+([A-Z_]+)\s+([A-Z]{1,4})\b',
        sql, re.IGNORECASE
    ):
        table_name = match.group(1).upper()
        alias = match.group(2).upper()
        if table_name in valid_cols:
            alias_map[alias] = table_name

    if not alias_map:
        return sql, []

    fixes = []
    # 모든 유효 컬럼을 하나의 풀로 합침 (fuzzy match 대상)
    all_columns = set()
    for cols in valid_cols.values():
        all_columns.update(cols)

    def replace_column(m: re.Match) -> str:
        alias = m.group(1).upper()
        col = m.group(2).upper()
        original = m.group(0)

        table = alias_map.get(alias)
        if not table:
            return original

        table_cols = valid_cols.get(table, set())
        if col in table_cols:
            return original  # 이미 유효

        # fuzzy match: 해당 테이블 컬럼에서 유사한 것 찾기
        candidates = get_close_matches(col, list(table_cols), n=1, cutoff=0.6)
        if candidates:
            fixed_col = candidates[0]
            fixes.append(f"{alias}.{col} → {alias}.{fixed_col} ({table})")
            return f"{m.group(1)}.{fixed_col}"

        return original

    # alias.COLUMN_NAME 패턴 매칭
    fixed_sql = re.sub(
        r'\b([A-Za-z]{1,4})\.([A-Z_]+)\b',
        replace_column,
        sql,
        flags=re.IGNORECASE
    )

    if fixes:
        logger.info("SQL 컬럼명 자동수정: %s", "; ".join(fixes))

    return fixed_sql, fixes


def _normalize_result_columns(columns: Any) -> list[str]:
    if not isinstance(columns, list):
        return []
    normalized: list[str] = []
    for col in columns:
        name = str(col or "").strip().upper()
        if name:
            normalized.append(name)
    return normalized


def _has_identifier_columns(columns: list[str]) -> bool:
    return any(col in _RESULT_IDENTIFIER_COLUMNS for col in columns)


def _append_warning_once(result: dict[str, Any], message: str) -> None:
    warnings = result.get("warning")
    if not isinstance(warnings, list):
        warnings = []
    if message not in warnings:
        warnings.append(message)
    result["warning"] = warnings


_SQL_LIKE_RE = re.compile(
    r"\bselect\b[\s\S]{0,400}\bfrom\b|\bwith\b[\s\S]{0,200}\bselect\b",
    re.IGNORECASE,
)


def _iter_strings(payload: Any):
    if isinstance(payload, str):
        yield payload
    elif isinstance(payload, dict):
        for value in payload.values():
            yield from _iter_strings(value)
    elif isinstance(payload, (list, tuple, set)):
        for item in payload:
            yield from _iter_strings(item)


def _contains_sql_like_text(payload: Any) -> bool:
    for text in _iter_strings(payload):
        value = str(text or "").strip()
        if not value:
            continue
        if len(value) < 8:
            continue
        if _SQL_LIKE_RE.search(value):
            return True
    return False


def _normalize_yn(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"yes", "y", "true", "1", "예", "네"}:
        return "yes"
    if text in {"no", "n", "false", "0", "아니오"}:
        return "no"
    return ""


def _normalize_population_unit(value: Any) -> str:
    text = str(value or "").strip().lower()
    aliases = {
        "patient": "per_subject",
        "subject": "per_subject",
        "per_subject": "per_subject",
        "admission": "per_hadm",
        "hadm": "per_hadm",
        "per_hadm": "per_hadm",
        "icu_stay": "per_hadm",
        "stay": "per_hadm",
    }
    return aliases.get(text, "")


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return default


def _extract_codes(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        text = str(value or "")
        raw_items = text.split(",") if "," in text else [text]
    cleaned: list[str] = []
    for item in raw_items:
        code = re.sub(r"[^A-Za-z0-9]+", "", str(item or "")).upper().strip()
        if code and code not in cleaned:
            cleaned.append(code)
    return cleaned


def _extract_min_days(text: str) -> float | None:
    matched = re.search(r"(\d+(?:\.\d+)?)\s*(day|days|일|d)\b", text, flags=re.IGNORECASE)
    if not matched:
        if "24h" in text.lower() or "24 hour" in text.lower():
            return 1.0
        return None
    try:
        return float(matched.group(1))
    except Exception:
        return None


def _default_pdf_schema_map() -> dict[str, Any]:
    return {
        "tables": {
            "patients": "SSO.PATIENTS",
            "admissions": "SSO.ADMISSIONS",
            "icustays": "SSO.ICUSTAYS",
            "diagnoses_icd": "SSO.DIAGNOSES_ICD",
            "measurements": "SSO.CHARTEVENTS",
        },
        "columns": {
            "subject_id": "subject_id",
            "hadm_id": "hadm_id",
            "stay_id": "stay_id",
            "admittime": "admittime",
            "icu_intime": "intime",
            "icu_outtime": "outtime",
            "icu_los_days": "los",
            "anchor_age": "anchor_age",
            "death_time": "deathtime",
            "icd_code": "icd_code",
            "icd_version": "icd_version",
            "meas_time": "charttime",
            "meas_itemid": "itemid",
            "meas_value": "valuenum",
            "hospital_expire_flag": "hospital_expire_flag",
        },
        "signal_map": {},
    }


def _merge_dict(base: dict[str, Any], override: Any) -> dict[str, Any]:
    if not isinstance(override, dict):
        return base
    merged = dict(base)
    for key, value in override.items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key] = _merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_pdf_schema_map() -> dict[str, Any]:
    schema = _default_pdf_schema_map()
    path = _PDF_SCHEMA_MAP_PATH if _PDF_SCHEMA_MAP_PATH.exists() else _PDF_SCHEMA_MAP_LOCAL
    if not path.exists():
        return schema
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("PDF schema map load failed (%s): %s", path, exc)
        return schema
    return _merge_dict(schema, payload if isinstance(payload, dict) else {})


def _schema_table(schema_map: dict[str, Any], key: str, default_value: str) -> str:
    tables = schema_map.get("tables") if isinstance(schema_map, dict) else {}
    if not isinstance(tables, dict):
        return default_value
    value = str(tables.get(key) or "").strip()
    return value or default_value


def _schema_col(schema_map: dict[str, Any], key: str, default_value: str) -> str:
    columns = schema_map.get("columns") if isinstance(schema_map, dict) else {}
    if not isinstance(columns, dict):
        return default_value
    value = str(columns.get(key) or "").strip()
    return value or default_value


def _load_reference_cohorts() -> str:
    """MongoDB에서 저장된 PDF 기반 코호트 정보를 읽어 RAG 예시로 활용"""
    try:
        store = get_state_store()
        payload = store.get(_SAVED_COHORTS_KEY) or {}
        # cohort.py의 _get_saved_cohorts()와 동일한 추출 로직 사용
        cohorts = payload.get("cohorts", []) if isinstance(payload, dict) else []
        
        # PDF 기반이고 SQL과 요약이 있는 것들을 추출
        pdf_cohorts = []
        for c in cohorts:
            if not isinstance(c, dict):
                continue
            if c.get("source_type") != "pdf":
                continue
            if not c.get("sql_query"):
                continue
            
            # cohort_definition이 None이거나 dict가 아닐 경우를 대비
            cd = c.get("cohort_definition")
            if not isinstance(cd, dict):
                continue
            
            if cd.get("summary_ko"):
                # 환자 수가 0명인 예시는 RAG에서 제외 (잘못된 쿼리 전파 방지)
                metrics = c.get("metrics") or {}
                p_count = metrics.get("patient_count")
                if p_count is not None:
                    try:
                        if int(p_count) <= 0:
                            continue
                    except:
                        pass
                pdf_cohorts.append(c)
        
        # 생성 일시(created_at) 기준 오름차순 정렬하여 '검증된 기초 사례'들이 우선적으로 프롬프트에 들어가도록 함
        pdf_cohorts.sort(key=lambda x: str(x.get("created_at", "")))
        samples = pdf_cohorts[:5]
        
        if not samples:
            return "No previous reference cohorts available."
            
        ref_texts = []
        for i, c in enumerate(samples):
            cd = c.get("cohort_definition") or {}
            ref_texts.append(f"### [Reference Example {i+1}]")
            ref_texts.append(f"- Title: {cd.get('title', 'N/A')}")
            ref_texts.append(f"- Summary (KR): {cd.get('summary_ko', '')[:150]}...")
            ref_texts.append(f"- Valid SQL:\n{c.get('sql_query')}\n")
            
        return "\n".join(ref_texts)
    except Exception as e:
        logger.warning("RAG 참조 데이터 로드 실패: %s", e)
        return "Error loading reference cohorts."


# clinical signal dictionary for template-based SQL generation
# Default clinical signals with complex logic (hardcoded fallback)
DEFAULT_CORE_SIGNALS = {
    "age": "SELECT a.hadm_id FROM SSO.PATIENTS p JOIN SSO.ADMISSIONS a ON p.subject_id = a.subject_id WHERE p.anchor_age >= {min} AND p.anchor_age <= {max}",
    "gender": "SELECT a.hadm_id FROM SSO.PATIENTS p JOIN SSO.ADMISSIONS a ON p.subject_id = a.subject_id WHERE p.gender = '{gender}'",
    "sex": "SELECT a.hadm_id FROM SSO.PATIENTS p JOIN SSO.ADMISSIONS a ON p.subject_id = a.subject_id WHERE p.gender = '{gender}'",
    "diagnosis": "SELECT HADM_ID FROM SSO.DIAGNOSES_ICD WHERE trim(icd_code) IN ({codes})",
    "icu_stay": "SELECT stay_id, hadm_id, intime as charttime FROM SSO.ICUSTAYS WHERE los >= {min_los}",
    "prescription": "SELECT hadm_id, starttime as charttime FROM SSO.PRESCRIPTIONS WHERE lower(drug) LIKE '%{drug}%'",
    # SOFA, OASIS, etc. are complex derived scores
    "sofa": "SELECT stay_id, charttime FROM SSO.CHARTEVENTS WHERE (itemid IN (220052, 220181, 225312) AND valuenum < 65) OR (itemid IN (223900, 223901) AND valuenum < 15)", 
    "rox": "SELECT stay_id, charttime FROM SSO.CHARTEVENTS WHERE (itemid IN (220277) AND valuenum < 90) OR (itemid IN (220210, 224690) AND valuenum > 25)",
    "oasis": "SELECT stay_id, charttime FROM SSO.CHARTEVENTS WHERE itemid IN (223900, 223901) AND valuenum < 13",
    "sae_diagnosis": "SELECT hadm_id, admittime as charttime FROM SSO.ADMISSIONS WHERE hadm_id IN (SELECT hadm_id FROM SSO.DIAGNOSES_ICD WHERE trim(icd_code) IN ('F05', 'R410', '2930', '3483')) UNION SELECT stay_id, charttime FROM SSO.CHARTEVENTS WHERE itemid IN (223900, 220739) AND valuenum < 15",
    # Special FIO2 logic
    "fio2": "SELECT stay_id, charttime FROM SSO.CHARTEVENTS WHERE itemid IN (223835) AND (CASE WHEN valuenum > 1 AND valuenum <= 100 THEN valuenum/100 WHEN valuenum > 0 AND valuenum <= 1 THEN valuenum ELSE NULL END) {operator} {value}",
    "body_temperature": "SELECT stay_id, charttime FROM SSO.CHARTEVENTS WHERE itemid IN (223761, 223762) AND valuenum {operator} {value} AND valuenum IS NOT NULL",
    "bun": "SELECT hadm_id, charttime FROM SSO.LABEVENTS WHERE itemid IN (51006) AND valuenum {operator} {value} AND valuenum IS NOT NULL",
    "creatinine": "SELECT hadm_id, charttime FROM SSO.LABEVENTS WHERE itemid IN (50912) AND valuenum {operator} {value} AND valuenum IS NOT NULL",
    "pao2": "SELECT hadm_id, charttime FROM SSO.LABEVENTS WHERE itemid IN (50821) AND valuenum {operator} {value} AND valuenum IS NOT NULL",
    "ph": "SELECT hadm_id, charttime FROM SSO.LABEVENTS WHERE itemid IN (50820) AND valuenum {operator} {value} AND valuenum IS NOT NULL",
    "anion_gap": "SELECT hadm_id, charttime FROM SSO.LABEVENTS WHERE itemid IN (50868) AND valuenum {operator} {value} AND valuenum IS NOT NULL",
    "urine_output": "SELECT stay_id, charttime FROM SSO.OUTPUTEVENTS WHERE itemid IN (226559, 226560, 226561, 226563, 226564, 226565, 226567, 226557, 226558, 226584, 227488) AND value {operator} {value}",
}

# Default metadata for frontend display
DEFAULT_SIGNAL_METADATA = {
    "age": {"target_table": "PATIENTS", "itemid": "anchor_age"},
    "gender": {"target_table": "PATIENTS", "itemid": "gender"},
    "sex": {"target_table": "PATIENTS", "itemid": "gender"},
    "sofa": {"target_table": "DERIVED", "itemid": "sofa_score"},
    "rox": {"target_table": "DERIVED", "itemid": "rox_index"},
    "oasis": {"target_table": "DERIVED", "itemid": "oasis_score"},
    "body_temperature": {"target_table": "CHARTEVENTS", "itemid": "223761,223762"},
    "bun": {"target_table": "LABEVENTS", "itemid": "51006"},
    "creatinine": {"target_table": "LABEVENTS", "itemid": "50912"},
    "pao2": {"target_table": "LABEVENTS", "itemid": "50821"},
    "ph": {"target_table": "LABEVENTS", "itemid": "50820"},
    "anion_gap": {"target_table": "LABEVENTS", "itemid": "50868"},
    "urine_output": {"target_table": "OUTPUTEVENTS", "itemid": "226559,226560,226561,226563,226564,226565,226567,226557,226558,226584,227488"},
    "hospital_los": {"target_table": "ADMISSIONS", "itemid": "dischtime-admittime"},
    "icu_los": {"target_table": "ICUSTAYS", "itemid": "los"},
    "in_hospital_mortality": {"target_table": "ADMISSIONS", "itemid": "hospital_expire_flag"},
}

WINDOW_TEMPLATES = {
    "icu_first_24h": "s.charttime BETWEEN p.intime AND p.intime + INTERVAL '24' HOUR",
    "admission_first_24h": "s.charttime BETWEEN p.admittime AND p.admittime + INTERVAL '24' HOUR",
    "icu_discharge_last_24h": "s.charttime BETWEEN p.outtime - INTERVAL '24' HOUR AND p.outtime"
}

class PDFCohortService:
    def __init__(self):
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = os.getenv("ENGINEER_MODEL", "gpt-4o")
        self.signal_map = {}
        self.signal_metadata = {}
        self._initialize_signal_maps()

    def _initialize_signal_maps(self):
        """Initialize signal maps by merging defaults with dynamic JSON metadata."""
        self.signal_map = DEFAULT_CORE_SIGNALS.copy()
        self.signal_metadata = DEFAULT_SIGNAL_METADATA.copy()
        
        try:
            # 1. Try absolute path (Docker environment standard)
            meta_path = "/app/var/metadata/mimic_rag_metadata_full.json"
            
            # 2. If not found, try relative path (Local development fallback)
            if not os.path.exists(meta_path):
                # backend/app/services/pdf_service.py -> backend/app/services -> backend/app -> backend -> root -> var
                meta_path = os.path.join(os.path.dirname(__file__), "../../../../var/metadata/mimic_rag_metadata_full.json")
            
            if os.path.exists(meta_path):
                with open(meta_path, "r", encoding="utf-8") as f:
                    full_meta = json.load(f)
                    
                for item in full_meta:
                    name = _normalize_signal_name(item.get("signal_name", ""))
                    if not name:
                        continue
                    mapping = item.get("mapping", {})
                    itemid = mapping.get("itemid")
                    table = mapping.get("target_table", "").upper()
                    
                    if itemid and table:
                        # Generate SQL Template
                        sql = None
                        if table == "CHARTEVENTS":
                            sql = f"SELECT stay_id, charttime FROM SSO.CHARTEVENTS WHERE itemid IN ({itemid}) AND valuenum {{operator}} {{value}} AND valuenum IS NOT NULL"
                        elif table == "LABEVENTS":
                             # Lab items often need joining with D_LABITEMS for readable labels, but if itemID is known, direct query is faster.
                             # However, to maintain compatibility with existing 'lab' template logic:
                             sql = f"SELECT hadm_id, charttime FROM SSO.LABEVENTS WHERE itemid IN ({itemid}) AND valuenum {{operator}} {{value}} AND valuenum IS NOT NULL"
                        
                        if sql:
                            self.signal_map[name] = sql
                            # Also add synonyms
                            for syn in item.get("synonyms", []):
                                syn_key = _normalize_signal_name(syn)
                                if not syn_key:
                                    continue
                                if syn_key not in self.signal_map: # Don't overwrite core signals
                                    self.signal_map[syn_key] = sql

                        # Update Metadata
                        if name not in self.signal_metadata:
                            self.signal_metadata[name] = {
                                "target_table": table,
                                "itemid": str(itemid)
                            }
                        for syn in item.get("synonyms", []):
                            syn_key = _normalize_signal_name(syn)
                            if not syn_key or syn_key in self.signal_metadata:
                                continue
                            self.signal_metadata[syn_key] = {
                                "target_table": table,
                                "itemid": str(itemid),
                            }
            else:
                logger.warning(f"RAG Metadata file not found at {meta_path}. Using defaults only.")
        except Exception as e:
            logger.error(f"Failed to load RAG metadata: {e}. Using defaults.")

    async def _extract_pdf_content_async(self, file_content: bytes) -> dict:
        """PDF 텍스트 및 자산(표, 이미지) 추출을 비동기로 실행"""
        import asyncio
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._extract_pdf_content, file_content)

    def _extract_pdf_content(self, file_content: bytes) -> dict:
        """PDF에서 구조화된 텍스트와 핵심 자산(표, 이미지)을 추출"""
        logger.info("PDF 정밀 분석 시작: %d bytes", len(file_content))
        try:
            doc = fitz.open(stream=file_content, filetype="pdf")
        except Exception as e:
            logger.error("PDF 열기 실패: %s", traceback.format_exc())
            raise RuntimeError(f"PDF 파일을 열 수 없습니다: {e}") from e

        triggers = [
            'eligibility criteria', 'figure', 'flowchart', 'table', 
            'inclusion', 'exclusion', 'missing data', '24 hours before discharge',
            'study population', 'participant selection'
        ]

        text_parts = []
        assets = {"figures": [], "tables": []}
        max_pages = min(_PDF_MAX_PAGES, len(doc))
        
        for i in range(max_pages):
            page_no = i + 1
            page = doc[i]
            page_text = (page.get_text("text") or "").strip()
            
            # 텍스트 섹션 추가
            if page_text:
                text_parts.append(f"\n=== PAGE {page_no} ===\n{page_text}")

            # 키워드 기반 자산 추출 (비용 및 성능 최적화)
            lower_text = page_text.lower()
            if any(kw in lower_text for kw in triggers):
                logger.info(f"Page {page_no}에서 트리거 키워드 감지. 자산 추출 시작.")
                
                # 표 추출
                try:
                    tabs = page.find_tables()
                    for tab in tabs.tables[:5]:
                        content = tab.extract()
                        if content and len(content) > 1: # 의미 있는 표만
                            assets["tables"].append({
                                "page": page_no,
                                "content": content
                            })
                except Exception as e:
                    logger.warning(f"Page {page_no} 표 추출 실패: {e}")

                # 이미지 추출
                try:
                    image_list = page.get_images(full=True)
                    if image_list:
                        assets["figures"].append({
                            "page": page_no,
                            "count": len(image_list),
                        })
                except Exception as e:
                    logger.warning(f"Page {page_no} 이미지 추출 실패: {e}")

        total_pages = len(doc)
        doc.close()
        return {
            "full_text": "\n".join(text_parts).strip(),
            "assets": assets,
            "page_count": total_pages,
            "pages_scanned": max_pages,
        }

    def _canonicalize_text(self, text: str) -> str:
        """텍스트 정규화: 기호 제거 및 공백 통합을 통해 해시 견고성 확보"""
        # 1. 소문자화
        t = text.lower()
        # 2. [Page X, Block Y] 마커 및 페이지 구분자 제거
        t = re.sub(r'\[page \d+, block \d+\]', '', t)
        t = re.sub(r'=== page \d+ ===', '', t)
        # 3. 특수문자 제거 (알파벳, 숫자, 한글만 남김)
        t = re.sub(r'[^a-z0-9가-힣]', ' ', t)
        # 4. 공백 통합
        t = re.sub(r'\s+', ' ', t).strip()
        return t

    def _build_focus_text(self, full_text: str) -> str:
        """조건 추출 정확도를 유지하면서 프롬프트 입력 길이를 줄이기 위한 핵심 문단 추출."""
        text = str(full_text or "").strip()
        if not text:
            return ""

        lines = [line.strip() for line in text.splitlines() if line and line.strip()]
        if not lines:
            return text[:_PDF_MAX_TEXT_CHARS]

        keywords = (
            "eligibility", "inclusion", "exclusion", "criteria", "population", "cohort",
            "flowchart", "figure", "table", "outcome", "endpoint", "methods",
            "selection", "baseline", "icu", "admission", "diagnosis",
            "선정", "제외", "기준", "코호트", "대상", "방법", "결과",
        )
        selected_indices: set[int] = set()
        for idx, line in enumerate(lines):
            lowered = line.lower()
            if any(keyword in lowered for keyword in keywords):
                start = max(0, idx - 2)
                end = min(len(lines), idx + 3)
                selected_indices.update(range(start, end))

        if not selected_indices:
            return text[:_PDF_MAX_TEXT_CHARS]

        focused_lines = [lines[idx] for idx in sorted(selected_indices)]
        focused_text = "\n".join(focused_lines).strip()
        if len(focused_text) < min(3000, len(text)):
            focused_text = f"{focused_text}\n\n{text}"
        return focused_text[:_PDF_MAX_TEXT_CHARS]

    def _split_text_by_pages(self, full_text: str) -> list[dict[str, Any]]:
        text = str(full_text or "")
        marker_re = re.compile(r"===\s*PAGE\s+(\d+)\s*===\n?", re.IGNORECASE)
        matches = list(marker_re.finditer(text))
        if not matches:
            return [{"page": 1, "text": text, "global_start": 0}]

        chunks: list[dict[str, Any]] = []
        for idx, match in enumerate(matches):
            page_no = _safe_int(match.group(1), default=idx + 1)
            content_start = match.end()
            content_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
            chunk_text = text[content_start:content_end].strip()
            if not chunk_text:
                continue
            chunks.append({"page": page_no, "text": chunk_text, "global_start": content_start})
        return chunks

    def _is_section_heading(self, line: str) -> bool:
        value = str(line or "").strip()
        if not value:
            return False
        if len(value) > 90:
            return False
        heading_patterns = (
            r"^(methods?|methodology|study population|population|eligibility|inclusion|exclusion)\b",
            r"^(results?|outcomes?|baseline characteristics?|discussion|conclusion)\b",
            r"^(연구대상|대상자|대상 환자|방법|결과|선정 기준|제외 기준|코호트)\b",
        )
        lowered = value.lower()
        return any(re.search(pattern, lowered, flags=re.IGNORECASE) for pattern in heading_patterns)

    def _extract_cohort_snippets(self, full_text: str, *, accuracy_mode: bool = False) -> list[dict[str, Any]]:
        base_keywords = [
            "cohort", "population", "study population", "eligibility", "inclusion", "exclusion",
            "diagnosis", "icd", "admission", "icu", "mortality", "within", "first admission",
            "first icu", "los", "length of stay", "follow-up", "endpoint", "outcome",
            "코호트", "대상자", "선정", "제외", "진단", "입원", "중환자실", "사망", "기간",
        ]
        if accuracy_mode and _PDF_ACCURACY_SECTION_KEYWORDS_EXPANDED:
            base_keywords.extend(
                [
                    "methods", "definitions", "appendix", "supplementary", "table", "flowchart",
                    "guideline", "protocol", "eligibility criteria", "study design", "outcomes",
                    "exposure", "index date", "index event", "first icu admission",
                    "measurement", "charttime", "itemid", "vital", "lab", "time window",
                    "방법", "정의", "부록", "보충", "표", "연구설계", "결과지표", "지표",
                ]
            )
        keywords = tuple(dict.fromkeys(base_keywords))
        table_hint_tokens = ("table", "tbl", "표")
        snippets: list[dict[str, Any]] = []
        seen: set[tuple[int, str]] = set()

        for page_chunk in self._split_text_by_pages(full_text):
            page_no = int(page_chunk.get("page") or 0)
            page_text = str(page_chunk.get("text") or "")
            global_start = int(page_chunk.get("global_start") or 0)
            lines = page_text.splitlines()
            if not lines:
                continue

            offsets: list[int] = []
            running = 0
            for line in lines:
                offsets.append(running)
                running += len(line) + 1

            current_section = "Unknown"
            for idx, raw_line in enumerate(lines):
                line = str(raw_line or "").strip()
                if not line:
                    continue
                if self._is_section_heading(line):
                    current_section = line[:80]
                    continue
                lowered = line.lower()
                if not any(keyword in lowered for keyword in keywords):
                    continue

                is_table_hint = any(token in lowered for token in table_hint_tokens)
                pad = 2
                if accuracy_mode and _PDF_ACCURACY_INCLUDE_TABLES and is_table_hint:
                    pad = 8
                start_idx = max(0, idx - pad)
                end_idx = min(len(lines), idx + pad + 1)
                snippet_text = "\n".join(lines[start_idx:end_idx]).strip()
                if not snippet_text:
                    continue
                if accuracy_mode and is_table_hint and len(snippet_text) > _PDF_ACCURACY_TABLE_CAPTURE_CHARS:
                    snippet_text = snippet_text[:_PDF_ACCURACY_TABLE_CAPTURE_CHARS]
                normalized_key = re.sub(r"\s+", " ", snippet_text.lower()).strip()
                dedup_key = (page_no, normalized_key)
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)

                span_start = offsets[start_idx]
                span_end_line_idx = max(start_idx, end_idx - 1)
                span_end = offsets[span_end_line_idx] + len(lines[span_end_line_idx])
                snippets.append(
                    {
                        "page": page_no,
                        "page_no": page_no,
                        "section": current_section,
                        "section_title": current_section,
                        "text": snippet_text[: (1200 if accuracy_mode else 700)],
                        "is_table_hint": bool(is_table_hint),
                        "char_range": [global_start + span_start, global_start + span_end],
                        "span": [global_start + span_start, global_start + span_end],
                    }
                )
                if len(snippets) >= _PDF_SNIPPET_MAX_COUNT:
                    break
            if len(snippets) >= _PDF_SNIPPET_MAX_COUNT:
                break

        snippets.sort(key=lambda item: (int(item.get("page") or 0), int((item.get("char_range") or [0])[0])))
        return snippets

    def _pick_evidence_refs(self, snippets: list[dict[str, Any]], clue_text: str, limit: int = 2) -> list[dict[str, Any]]:
        if not snippets:
            return []
        clue = str(clue_text or "").lower()
        tokens = {
            token
            for token in re.findall(r"[a-z0-9가-힣]{2,}", clue)
            if token not in {"study", "patient", "cohort", "criteria", "조건", "환자", "코호트"}
        }

        scored: list[tuple[float, dict[str, Any]]] = []
        for idx, snippet in enumerate(snippets):
            text = str(snippet.get("text") or "")
            lowered = text.lower()
            overlap = sum(1 for token in tokens if token in lowered)
            has_number = 1 if re.search(r"\d", lowered) else 0
            score = float(overlap * 3 + has_number - idx * 0.01)
            scored.append((score, snippet))

        scored.sort(key=lambda pair: pair[0], reverse=True)
        picked: list[dict[str, Any]] = []
        for _, snippet in scored[: max(1, limit)]:
            quote = str(snippet.get("text") or "").replace("\n", " ").strip()
            picked.append(
                {
                    "page": int(snippet.get("page") or 0),
                    "section": str(snippet.get("section") or "Unknown"),
                    "quote": quote[:220],
                    "char_range": snippet.get("char_range") or [],
                }
            )
        return picked

    def _enrich_conditions_with_evidence(
        self,
        conditions: dict[str, Any],
        snippets: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if not isinstance(conditions, dict):
            conditions = {}
        cohort_def = conditions.get("cohort_definition")
        if not isinstance(cohort_def, dict):
            cohort_def = {}
            conditions["cohort_definition"] = cohort_def

        extraction = cohort_def.get("extraction_details")
        if not isinstance(extraction, dict):
            extraction = {}
            cohort_def["extraction_details"] = extraction

        cohort_criteria = extraction.get("cohort_criteria")
        if not isinstance(cohort_criteria, dict):
            cohort_criteria = {}
            extraction["cohort_criteria"] = cohort_criteria

        population = cohort_criteria.get("population")
        if not isinstance(population, list):
            population = []
            cohort_criteria["population"] = population

        evidence_enriched = 0
        for item in population:
            if not isinstance(item, dict):
                continue
            criterion = str(item.get("criterion") or item.get("operational_definition") or "").strip()
            refs = self._pick_evidence_refs(snippets, criterion, limit=2)
            if refs:
                item["evidence_refs"] = refs
                if not str(item.get("evidence") or "").strip() or str(item.get("evidence")).startswith("[Source]"):
                    item["evidence"] = refs[0]["quote"]
                if not isinstance(item.get("evidence_source"), dict):
                    item["evidence_source"] = {}
                item["evidence_source"]["page"] = refs[0]["page"]
                item["evidence_source"]["type"] = item["evidence_source"].get("type") or "text"
                evidence_enriched += 1

        diagnosis_criteria = extraction.get("diagnosis_criteria")
        if isinstance(diagnosis_criteria, dict):
            diag_refs = self._pick_evidence_refs(
                snippets,
                json.dumps(diagnosis_criteria, ensure_ascii=False),
                limit=2,
            )
            if diag_refs:
                diagnosis_criteria["evidence_refs"] = diag_refs
                if not str(diagnosis_criteria.get("evidence") or "").strip() or str(diagnosis_criteria.get("evidence")).startswith("[Source]"):
                    diagnosis_criteria["evidence"] = diag_refs[0]["quote"]
                if not isinstance(diagnosis_criteria.get("evidence_source"), dict):
                    diagnosis_criteria["evidence_source"] = {}
                diagnosis_criteria["evidence_source"]["page"] = diag_refs[0]["page"]
                diagnosis_criteria["evidence_source"]["type"] = diagnosis_criteria["evidence_source"].get("type") or "text"

        inferred_ambiguities = self._build_ambiguities(cohort_def, snippets)
        llm_ambiguities = cohort_def.get("ambiguities")
        merged: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        if isinstance(llm_ambiguities, list):
            for item in llm_ambiguities:
                if not isinstance(item, dict):
                    continue
                amb_id = str(item.get("id") or "").strip() or f"amb_llm_{len(merged)+1}"
                if amb_id in seen_ids:
                    continue
                seen_ids.add(amb_id)
                item = dict(item)
                item["id"] = amb_id
                item["status"] = str(item.get("status") or "unresolved")
                merged.append(item)
        for item in inferred_ambiguities:
            amb_id = str(item.get("id") or "").strip()
            if not amb_id or amb_id in seen_ids:
                continue
            seen_ids.add(amb_id)
            merged.append(item)

        ambiguities = merged
        cohort_def["ambiguities"] = ambiguities
        cohort_def["snippet_count"] = len(snippets)
        cohort_def["evidence_coverage_count"] = evidence_enriched
        return conditions, ambiguities

    def _build_ambiguities(self, cohort_def: dict[str, Any], snippets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ambiguities: list[dict[str, Any]] = []
        extraction = cohort_def.get("extraction_details") if isinstance(cohort_def, dict) else {}
        criteria = extraction.get("cohort_criteria") if isinstance(extraction, dict) else {}

        index_unit_raw = str((criteria or {}).get("index_unit") or "").strip().lower()
        first_stay = _normalize_yn((criteria or {}).get("first_stay_only"))
        if index_unit_raw not in {"patient", "icu_stay"}:
            ambiguities.append(
                {
                    "id": "amb_index_unit",
                    "question": "코호트 기준 단위(index_unit)가 patient 인지 icu_stay 인지 불명확합니다.",
                    "options": ["patient", "icu_stay"],
                    "default_policy": "require_user_choice",
                    "status": "unresolved",
                }
            )
        if not first_stay:
            ambiguities.append(
                {
                    "id": "amb_first_stay_only",
                    "question": "first stay only 적용 여부가 불명확합니다.",
                    "options": ["Yes", "No"],
                    "default_policy": "require_user_choice",
                    "status": "unresolved",
                }
            )
        if index_unit_raw == "patient" and first_stay == "yes":
            ambiguities.append(
                {
                    "id": "amb_first_icu_unit",
                    "question": "first ICU admission 기준이 subject 기준인지 hadm 기준인지 선택이 필요합니다.",
                    "options": ["per_subject", "per_hadm"],
                    "default_policy": "require_user_choice",
                    "status": "unresolved",
                }
            )

        diagnosis_criteria = extraction.get("diagnosis_criteria") if isinstance(extraction, dict) else {}
        if isinstance(diagnosis_criteria, dict):
            codes = _extract_codes(diagnosis_criteria.get("codes", []))
            coding_system = str(diagnosis_criteria.get("coding_system") or "").strip().upper()
            if codes and coding_system not in {"ICD-9", "ICD-10", "ICD-9/10", "ICD9", "ICD10"}:
                ambiguities.append(
                    {
                        "id": "amb_icd_version",
                        "question": "진단 코드의 ICD 버전(ICD-9/ICD-10)이 명확하지 않습니다.",
                        "options": ["ICD-9", "ICD-10", "ICD-9/10"],
                        "default_policy": "require_user_choice",
                        "status": "unresolved",
                    }
                )

        population = criteria.get("population") if isinstance(criteria, dict) else []
        if isinstance(population, list):
            missing_evidence = 0
            for item in population:
                if not isinstance(item, dict):
                    continue
                refs = item.get("evidence_refs")
                if not isinstance(refs, list) or not refs:
                    missing_evidence += 1
            if population and missing_evidence > 0 and snippets:
                ambiguities.append(
                    {
                        "id": "amb_missing_evidence_links",
                        "question": "일부 조건의 근거 문장 연결이 충분하지 않습니다.",
                        "options": ["재추출", "현재 근거로 진행"],
                        "default_policy": "require_user_choice",
                        "status": "unresolved",
                    }
                )
        return ambiguities

    def _derive_population_policy(self, conditions: dict[str, Any], *, accuracy_mode: bool = False) -> dict[str, Any]:
        cohort_def = conditions.get("cohort_definition") if isinstance(conditions, dict) else {}
        extraction = cohort_def.get("extraction_details") if isinstance(cohort_def, dict) else {}
        criteria = extraction.get("cohort_criteria") if isinstance(extraction, dict) else {}
        first_stay_raw = _normalize_yn((criteria or {}).get("first_stay_only"))
        index_unit = str((criteria or {}).get("index_unit") or "").strip().lower()
        normalized_unit = _normalize_population_unit(index_unit)

        require_icu = index_unit == "icu_stay"
        episode_selector = "first" if first_stay_raw == "yes" else "all"
        episode_unit = normalized_unit or "per_hadm"
        if episode_selector == "first" and index_unit == "patient":
            episode_unit = "per_subject"
        if episode_selector == "first" and index_unit == "icu_stay":
            episode_unit = "per_hadm"

        policy = {
            "require_icu": require_icu,
            "episode_selector": episode_selector,
            "episode_unit": episode_unit,
            "index_unit": index_unit or "patient",
            "measurement_window": "icu_discharge_last_24h",
            "accuracy_mode": bool(accuracy_mode),
        }
        if accuracy_mode:
            # Accuracy-first policy defaults:
            # - first ICU per subject
            # - ICU required
            # - measurement window fixed to outtime-24h ~ outtime
            policy["require_icu"] = True
            policy["episode_selector"] = "first"
            policy["episode_unit"] = "per_subject"
            policy["index_unit"] = "icu_stay"
            policy["measurement_window"] = "icu_discharge_last_24h"
            policy["icu_los_min_days_default"] = 1.0
        return policy

    def _build_canonical_spec(
        self,
        conditions: dict[str, Any],
        *,
        file_hash: str,
        snippets: list[dict[str, Any]],
        ambiguities: list[dict[str, Any]],
        accuracy_mode: bool = False,
    ) -> dict[str, Any]:
        cohort_def = conditions.get("cohort_definition") if isinstance(conditions, dict) else {}
        extraction = cohort_def.get("extraction_details") if isinstance(cohort_def, dict) else {}
        criteria = extraction.get("cohort_criteria") if isinstance(extraction, dict) else {}
        policy = self._derive_population_policy(conditions, accuracy_mode=accuracy_mode)

        population_items = criteria.get("population") if isinstance(criteria, dict) else []
        if not isinstance(population_items, list):
            population_items = []

        inclusion: list[dict[str, Any]] = []
        exclusion: list[dict[str, Any]] = []
        requirements: list[dict[str, Any]] = []
        candidate_condition_count = 0

        for idx, item in enumerate(population_items, start=1):
            if not isinstance(item, dict):
                continue
            criterion_text = str(item.get("criterion") or item.get("operational_definition") or "").strip()
            if not criterion_text:
                continue
            candidate_condition_count += 1
            normalized = criterion_text.lower()
            refs = item.get("evidence_refs") if isinstance(item.get("evidence_refs"), list) else []
            condition_type = "criterion_text"
            payload: dict[str, Any] = {
                "id": f"cond_{idx:03d}",
                "type": condition_type,
                "criterion": criterion_text,
                "evidence_refs": refs,
            }

            if "age" in normalized or "연령" in normalized:
                condition_type = "age_range"
                nums = re.findall(r"\d+(?:\.\d+)?", criterion_text)
                if len(nums) >= 2:
                    payload["min"] = _safe_int(nums[0], 0)
                    payload["max"] = _safe_int(nums[1], 120)
                payload["type"] = condition_type
                payload["age_field_hint"] = "patients.anchor_age"
            elif "icd" in normalized or "진단" in normalized or "diagnos" in normalized:
                condition_type = "diagnosis_icd_prefix"
                payload["type"] = condition_type
                diag = extraction.get("diagnosis_criteria") if isinstance(extraction, dict) else {}
                if isinstance(diag, dict):
                    codes = _extract_codes(diag.get("codes", []))
                    coding_system = str(diag.get("coding_system") or "").upper()
                    version_hint = 9 if "9" in coding_system else (10 if "10" in coding_system else None)
                    payload["codes"] = [
                        {
                            "prefix": code,
                            "icd_version": version_hint,
                        }
                        for code in codes
                    ]
                    payload["scope"] = "hadm"
                    for code in codes:
                        if code.isdigit() and len(code) <= 3:
                            ambiguities.append(
                                {
                                    "id": f"amb_icd_{code}_scope",
                                    "question": f"ICD 코드 '{code}'는 prefix({code}%)인지 range 해석인지 확인이 필요합니다.",
                                    "options": [f"prefix:{code}%", "range:manual_confirm"],
                                    "default_policy": "require_user_choice",
                                    "status": "unresolved",
                                }
                            )
            elif "los" in normalized or "length of stay" in normalized:
                condition_type = "icu_los_min_days"
                payload["type"] = condition_type
                min_days = _extract_min_days(criterion_text)
                if min_days is not None:
                    payload["min_days"] = min_days
            elif ("death" in normalized or "사망" in normalized) and ("within" in normalized or "이내" in normalized):
                condition_type = "death_within_days_of_index_event"
                payload["type"] = condition_type
                day_match = re.search(r"(\d+)\s*(day|days|일)", normalized)
                payload["days"] = _safe_int(day_match.group(1), default=3) if day_match else 3
                payload["death_time_field_hint"] = "admissions.deathtime|hospital_expire_flag"

            signals: list[str] = []
            if "heart rate" in normalized or "hr" in normalized:
                signals.append("HR")
            if "sbp" in normalized or "systolic" in normalized or "bp sys" in normalized:
                signals.append("BP_SYS")
            if "dbp" in normalized or "diastolic" in normalized or "bp dia" in normalized:
                signals.append("BP_DIA")
            if ("measurement" in normalized or "vital" in normalized or "chart" in normalized) and signals:
                req_payload = {
                    "id": f"req_{idx:03d}",
                    "type": "measurement_required",
                    "signals": sorted(set(signals)),
                    "window": {
                        "anchor": "icu_outtime",
                        "start_offset_hours": -24,
                        "end_offset_hours": 0,
                    },
                    "evidence_refs": refs,
                }
                requirements.append(req_payload)

            target = exclusion if str(item.get("type") or "").lower() == "exclusion" else inclusion
            target.append(payload)

        unique_ambiguities: list[dict[str, Any]] = []
        seen_amb_ids: set[str] = set()
        for amb in ambiguities:
            if not isinstance(amb, dict):
                continue
            amb_id = str(amb.get("id") or "").strip()
            if not amb_id or amb_id in seen_amb_ids:
                continue
            seen_amb_ids.add(amb_id)
            unique_ambiguities.append(amb)

        canonical = {
            "metadata": {
                "source_doc": {
                    "title": str(cohort_def.get("title") or "Untitled PDF"),
                    "hash": file_hash,
                },
                "created_at": str(datetime.utcnow().isoformat() + "Z"),
                "db_dialect": "oracle",
                "accuracy_mode": bool(accuracy_mode),
            },
            "population": {
                "index_event": "icu_intime" if policy.get("require_icu") else "admittime",
                "require_icu": bool(policy.get("require_icu")),
                "episode_unit": policy.get("episode_unit"),
                "episode_selector": policy.get("episode_selector"),
                "measurement_window": str(policy.get("measurement_window") or "icu_discharge_last_24h"),
            },
            "inclusion": inclusion,
            "exclusion": exclusion,
            "requirements": requirements,
            "ambiguities": unique_ambiguities,
            "evidence_snippets": snippets[: min(20, len(snippets))],
            "candidates": {
                "condition_candidates": candidate_condition_count,
            },
        }
        return canonical

    def _strip_fetch_first_clause(self, sql: str) -> str:
        text = str(sql or "").strip().rstrip(";")
        return re.sub(
            r"\s+fetch\s+first\s+\d+\s+rows\s+only\s*$",
            "",
            text,
            flags=re.IGNORECASE,
        ).strip()

    def _enforce_evidence_refs(self, canonical_spec: dict[str, Any]) -> dict[str, Any]:
        spec = dict(canonical_spec or {})
        ambiguities = spec.get("ambiguities") if isinstance(spec.get("ambiguities"), list) else []
        seen_amb_ids = {str(item.get("id") or "").strip() for item in ambiguities if isinstance(item, dict)}
        kept_by_section: dict[str, list[dict[str, Any]]] = {"inclusion": [], "exclusion": [], "requirements": []}

        for section in ("inclusion", "exclusion", "requirements"):
            items = spec.get(section)
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                refs = item.get("evidence_refs")
                if isinstance(refs, list) and refs:
                    kept_by_section[section].append(item)
                    continue
                cond_id = str(item.get("id") or f"{section}_missing_evidence")
                amb_id = f"amb_missing_evidence_{cond_id}"
                if amb_id in seen_amb_ids:
                    continue
                seen_amb_ids.add(amb_id)
                ambiguities.append(
                    {
                        "id": amb_id,
                        "question": f"{cond_id} 조건의 근거 문장(evidence_refs)이 없습니다. 근거 보강 후 재실행이 필요합니다.",
                        "options": ["재추출", "사용자 근거 수동입력"],
                        "default_policy": "require_user_choice",
                        "status": "unresolved",
                    }
                )
        for section in kept_by_section:
            spec[section] = kept_by_section[section]
        spec["ambiguities"] = ambiguities
        return spec

    async def _critic_cohort_spec(
        self,
        *,
        snippets: list[dict[str, Any]],
        canonical_spec: dict[str, Any],
    ) -> tuple[dict[str, Any], list[str]]:
        warnings: list[str] = []
        prompt = f"""아래 Snippets와 CohortSpec을 검토하여 정확도 관점에서 누락/의미 붕괴를 수정하세요.
규칙:
1) SQL은 절대 출력하지 마세요.
2) 근거 없는 조건은 확정하지 말고 ambiguities로 이동하세요.
3) within X days는 event-to-event 조건으로 유지하세요(LOS 치환 금지).
4) first ICU admission 단위(subject/hadm)가 모호하면 ambiguities에 남기세요.

반환 형식(JSON):
{{
  "canonical_spec": {{ ...수정된 CohortSpec... }},
  "critic_notes": ["..."]
}}

Snippets:
{json.dumps(snippets[: min(40, len(snippets))], ensure_ascii=False)}

CohortSpec:
{json.dumps(canonical_spec, ensure_ascii=False)}
"""
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "당신은 임상 코호트 스펙 검증기입니다. JSON만 반환하세요."},
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0,
                seed=42,
            )
            parsed = json.loads(response.choices[0].message.content)
            if _contains_sql_like_text(parsed):
                warnings.append("Spec critic output contained SQL-like text and was discarded.")
                return canonical_spec, warnings
            revised = parsed.get("canonical_spec") if isinstance(parsed, dict) else None
            if not isinstance(revised, dict):
                warnings.append("Spec critic returned invalid format. Original spec is used.")
                return canonical_spec, warnings
            notes = parsed.get("critic_notes")
            if isinstance(notes, list):
                warnings.extend([str(n) for n in notes if str(n or "").strip()])
            return revised, warnings
        except Exception as exc:
            warnings.append(f"Spec critic skipped due to error: {exc}")
            return canonical_spec, warnings

    def _build_accuracy_metrics(
        self,
        *,
        canonical_spec: dict[str, Any],
        validation_report: dict[str, Any],
    ) -> dict[str, Any]:
        inclusion = canonical_spec.get("inclusion") if isinstance(canonical_spec.get("inclusion"), list) else []
        exclusion = canonical_spec.get("exclusion") if isinstance(canonical_spec.get("exclusion"), list) else []
        requirements = canonical_spec.get("requirements") if isinstance(canonical_spec.get("requirements"), list) else []
        conditions = [*inclusion, *exclusion, *requirements]
        with_evidence = 0
        for item in conditions:
            if not isinstance(item, dict):
                continue
            refs = item.get("evidence_refs")
            if isinstance(refs, list) and refs:
                with_evidence += 1

        candidate_count = _safe_int(
            (
                canonical_spec.get("candidates", {})
                if isinstance(canonical_spec.get("candidates"), dict)
                else {}
            ).get("condition_candidates"),
            default=len(conditions),
        )
        completeness = float(len(conditions) / max(1, candidate_count))
        evidence_coverage = float(with_evidence / max(1, len(conditions)))
        validation_status = str(validation_report.get("status") or "").strip().lower()
        return {
            "spec_completeness": round(completeness, 4),
            "evidence_coverage": round(evidence_coverage, 4),
            "validation_pass": validation_status == "passed",
            "condition_count": len(conditions),
            "condition_candidate_count": candidate_count,
            "evidence_condition_count": with_evidence,
        }

    def _execute_scalar_count(self, sql: str, *, accuracy_mode: bool = False) -> int | None:
        try:
            res = execute_sql(sql, accuracy_mode=accuracy_mode, query_tag="pdf_validation_scalar")
        except Exception:
            return None
        rows = res.get("rows")
        if not isinstance(rows, list) or not rows:
            return None
        first = rows[0]
        if isinstance(first, (list, tuple)):
            return _safe_int(first[0], default=0)
        if isinstance(first, dict):
            if first:
                return _safe_int(next(iter(first.values())), default=0)
            return 0
        return _safe_int(first, default=0)

    def _execute_rows_sample(self, sql: str, *, accuracy_mode: bool = False) -> list[dict[str, Any]]:
        try:
            res = execute_sql(sql, accuracy_mode=accuracy_mode, query_tag="pdf_validation_sample")
        except Exception:
            return []
        cols = [str(c or "").lower() for c in (res.get("columns") or [])]
        rows = res.get("rows") or []
        sample: list[dict[str, Any]] = []
        for row in rows[:_PDF_VALIDATION_SAMPLE_ROWS]:
            if isinstance(row, (list, tuple)):
                sample.append({col: value for col, value in zip(cols, row)})
            elif isinstance(row, dict):
                sample.append(row)
        return sample

    def _build_stepwise_anomalies(
        self,
        step_counts: list[dict[str, Any]],
        intent_steps: list[dict[str, Any]],
        *,
        accuracy_mode: bool = False,
    ) -> list[dict[str, Any]]:
        if not step_counts:
            return []
        anomalies: list[dict[str, Any]] = []
        numeric_counts: list[int] = []
        labels: list[str] = []
        for row in step_counts:
            if not isinstance(row, dict):
                continue
            label = str(row.get("step_name") or row.get("STEP_NAME") or "").strip()
            count = _safe_int(row.get("cnt", row.get("CNT", 0)), default=0)
            labels.append(label)
            numeric_counts.append(count)

        for idx in range(1, len(numeric_counts)):
            prev_count = numeric_counts[idx - 1]
            now_count = numeric_counts[idx]
            if now_count > prev_count:
                anomalies.append(
                    {
                        "type": "non_monotonic_increase",
                        "step": labels[idx] if idx < len(labels) else f"step_{idx+1}",
                        "previous_count": prev_count,
                        "current_count": now_count,
                        "message": "필터 단계에서 row count가 증가했습니다. 조인 중복 가능성을 점검하세요.",
                    }
                )
            if prev_count > 0:
                drop_ratio = (prev_count - now_count) / float(prev_count)
                drop_warn_ratio = 0.80 if accuracy_mode else 0.95
                if drop_ratio >= drop_warn_ratio:
                    anomalies.append(
                        {
                            "type": "sharp_drop",
                            "step": labels[idx] if idx < len(labels) else f"step_{idx+1}",
                            "previous_count": prev_count,
                            "current_count": now_count,
                            "drop_ratio": round(drop_ratio, 4),
                            "message": f"특정 단계에서 {int(drop_warn_ratio*100)}% 이상 급감했습니다. 조건 해석/코드 범위를 확인하세요.",
                        }
                    )
                if accuracy_mode and abs(drop_ratio) < 0.001:
                    anomalies.append(
                        {
                            "type": "no_effect",
                            "step": labels[idx] if idx < len(labels) else f"step_{idx+1}",
                            "previous_count": prev_count,
                            "current_count": now_count,
                            "drop_ratio": round(drop_ratio, 6),
                            "message": "단계 적용 후 변화가 0.1% 미만입니다. 조건이 실제로 적용됐는지 확인하세요.",
                        }
                    )

        if intent_steps and len(intent_steps) + 1 != len(step_counts):
            anomalies.append(
                {
                    "type": "step_count_mismatch",
                    "message": "Intent step 수와 debug step count 수가 일치하지 않습니다.",
                    "intent_steps": len(intent_steps),
                    "debug_steps": len(step_counts),
                }
            )
        return anomalies

    def _build_validation_report(
        self,
        *,
        cohort_sql: str,
        db_result: dict[str, Any],
        step_counts: list[dict[str, Any]],
        intent: dict[str, Any],
        population_policy: dict[str, Any],
        canonical_spec: dict[str, Any],
        schema_map: dict[str, Any],
        accuracy_mode: bool = False,
    ) -> dict[str, Any]:
        report = {
            "enabled": _PDF_VALIDATION_ENABLED,
            "accuracy_mode": bool(accuracy_mode),
            "status": "skipped",
            "invariants": [],
            "stepwise_counts": step_counts,
            "anomalies": [],
            "negative_samples": [],
            "messages": [],
        }
        if not _PDF_VALIDATION_ENABLED:
            report["messages"].append("Validation is disabled by env(PDF_VALIDATION_ENABLED=false).")
            return report
        if not str(cohort_sql or "").strip():
            report["messages"].append("Validation skipped: cohort_sql is empty.")
            return report
        if db_result.get("error"):
            report["status"] = "failed"
            report["messages"].append(f"Validation blocked by SQL execution error: {db_result.get('error')}")
            return report

        base_sql = self._strip_fetch_first_clause(cohort_sql)
        negative_sample_limit = _PDF_ACCURACY_NEGATIVE_SAMPLE_N if accuracy_mode else _PDF_VALIDATION_SAMPLE_ROWS
        invariants: list[dict[str, Any]] = []
        negative_samples: list[dict[str, Any]] = []

        def add_invariant(name: str, passed: bool, detail: str, violation_count: int | None = None) -> None:
            invariants.append(
                {
                    "name": name,
                    "passed": bool(passed),
                    "detail": detail,
                    "violation_count": violation_count,
                }
            )

        cols = _normalize_result_columns(db_result.get("columns", []))
        add_invariant(
            "result_has_identifier",
            _has_identifier_columns(cols),
            "결과에 subject_id/hadm_id/stay_id 중 최소 1개 식별자 컬럼이 있어야 합니다.",
            0 if _has_identifier_columns(cols) else 1,
        )

        if population_policy.get("require_icu"):
            null_stay_count = self._execute_scalar_count(
                f"SELECT COUNT(*) AS cnt FROM ({base_sql}) q WHERE q.stay_id IS NULL",
                accuracy_mode=accuracy_mode,
            )
            if null_stay_count is None:
                add_invariant(
                    "require_icu_implies_non_null_stay",
                    True,
                    "require_icu 검증 쿼리를 실행하지 못했습니다(권한/타임아웃 가능).",
                    None,
                )
                passed = True
            else:
                passed = null_stay_count == 0
                add_invariant(
                    "require_icu_implies_non_null_stay",
                    passed,
                    "require_icu=true이면 최종 결과의 stay_id NULL이 없어야 합니다.",
                    null_stay_count,
                )
            if not passed and null_stay_count:
                negative_samples.extend(
                    self._execute_rows_sample(
                        f"SELECT subject_id, hadm_id, stay_id FROM ({base_sql}) q "
                        f"WHERE q.stay_id IS NULL FETCH FIRST {negative_sample_limit} ROWS ONLY",
                        accuracy_mode=accuracy_mode,
                    )
                )

            if accuracy_mode and "LEFT JOIN" in str(cohort_sql or "").upper():
                add_invariant(
                    "require_icu_join_pattern",
                    False,
                    "accuracy_mode에서는 require_icu=true일 때 LEFT JOIN 패턴을 금지합니다.",
                    1,
                )
                if _PDF_ACCURACY_FAIL_FAST_ON_INVARIANTS:
                    report["status"] = "failed"
                    report["invariants"] = invariants
                    report["negative_samples"] = negative_samples[:negative_sample_limit]
                    report["messages"].append("Fail-fast: require_icu join invariant failed.")
                    return report

        episode_selector = str(population_policy.get("episode_selector") or "all")
        episode_unit = str(population_policy.get("episode_unit") or "per_hadm")
        if episode_selector == "first":
            key = "subject_id" if episode_unit == "per_subject" else "hadm_id"
            dup_count = self._execute_scalar_count(
                "SELECT COUNT(*) AS cnt FROM ("
                f"SELECT q.{key}, COUNT(*) AS c FROM ({base_sql}) q "
                f"GROUP BY q.{key} HAVING COUNT(*) > 1"
                ") t",
                accuracy_mode=accuracy_mode,
            )
            if dup_count is None:
                add_invariant(
                    "first_episode_uniqueness",
                    True,
                    f"{key} 중복 검증 쿼리를 실행하지 못했습니다(권한/타임아웃 가능).",
                    None,
                )
                passed = True
            else:
                passed = dup_count == 0
                add_invariant(
                    "first_episode_uniqueness",
                    passed,
                    f"episode_selector=first이면 {key} 중복이 없어야 합니다.",
                    dup_count,
                )
            if not passed and dup_count:
                negative_samples.extend(
                    self._execute_rows_sample(
                        f"SELECT q.{key}, COUNT(*) AS dup_cnt FROM ({base_sql}) q "
                        f"GROUP BY q.{key} HAVING COUNT(*) > 1 FETCH FIRST {negative_sample_limit} ROWS ONLY",
                        accuracy_mode=accuracy_mode,
                    )
                )
                if _PDF_ACCURACY_FAIL_FAST_ON_INVARIANTS and accuracy_mode:
                    report["status"] = "failed"
                    report["invariants"] = invariants
                    report["negative_samples"] = negative_samples[:negative_sample_limit]
                    report["messages"].append("Fail-fast: first_episode_uniqueness invariant failed.")
                    return report

        intent_steps = intent.get("steps") if isinstance(intent, dict) else []
        if isinstance(intent_steps, list):
            los_thresholds: list[float] = []
            for step in intent_steps:
                if not isinstance(step, dict):
                    continue
                if str(step.get("type") or "").strip().lower() != "icu_stay":
                    continue
                if not bool(step.get("is_exclusion", False)):
                    continue
                min_los = step.get("params", {}).get("min_los") if isinstance(step.get("params"), dict) else None
                try:
                    min_los_val = float(min_los)
                except Exception:
                    min_los_val = 0.0
                if min_los_val > 0:
                    los_thresholds.append(min_los_val)
            if los_thresholds:
                min_los = min(los_thresholds)
                icustays_table = _schema_table(schema_map, "icustays", "SSO.ICUSTAYS")
                stay_col = _schema_col(schema_map, "stay_id", "stay_id")
                icu_los_col = _schema_col(schema_map, "icu_los_days", "los")
                los_violation = self._execute_scalar_count(
                    "SELECT COUNT(*) AS cnt FROM ("
                    f"SELECT q.stay_id FROM ({base_sql}) q "
                    f"JOIN {icustays_table} i ON i.{stay_col} = q.stay_id "
                    f"WHERE i.{icu_los_col} < {min_los:g}"
                    ") t",
                    accuracy_mode=accuracy_mode,
                )
                if los_violation is None:
                    add_invariant(
                        "icu_los_min_days",
                        True,
                        "ICU LOS 검증 쿼리를 실행하지 못했습니다(권한/타임아웃 가능).",
                        None,
                    )
                    passed = True
                else:
                    passed = los_violation == 0
                    add_invariant(
                        "icu_los_min_days",
                        passed,
                        f"제외조건 icu_los_min_days({min_los:g})를 위반한 stay가 없어야 합니다.",
                        los_violation,
                    )
                if not passed and los_violation:
                    negative_samples.extend(
                        self._execute_rows_sample(
                            f"SELECT q.subject_id, q.hadm_id, q.stay_id, i.{icu_los_col} "
                            f"FROM ({base_sql}) q JOIN {icustays_table} i ON i.{stay_col} = q.stay_id "
                            f"WHERE i.{icu_los_col} < {min_los:g} FETCH FIRST {negative_sample_limit} ROWS ONLY",
                            accuracy_mode=accuracy_mode,
                        )
                    )
                    if _PDF_ACCURACY_FAIL_FAST_ON_INVARIANTS and accuracy_mode:
                        report["status"] = "failed"
                        report["invariants"] = invariants
                        report["negative_samples"] = negative_samples[:negative_sample_limit]
                        report["messages"].append("Fail-fast: icu_los_min_days invariant failed.")
                        return report

        exclusion = canonical_spec.get("exclusion") if isinstance(canonical_spec.get("exclusion"), list) else []
        death_rule = next(
            (
                item
                for item in exclusion
                if isinstance(item, dict)
                and str(item.get("type") or "").strip().lower() == "death_within_days_of_index_event"
            ),
            None,
        )
        if isinstance(death_rule, dict):
            days = max(1, _safe_int(death_rule.get("days"), default=3))
            admissions_table = _schema_table(schema_map, "admissions", "SSO.ADMISSIONS")
            hadm_col = _schema_col(schema_map, "hadm_id", "hadm_id")
            death_col = _schema_col(schema_map, "death_time", "deathtime")
            death_violation = self._execute_scalar_count(
                "SELECT COUNT(*) AS cnt FROM ("
                f"SELECT q.hadm_id FROM ({base_sql}) q "
                f"JOIN {admissions_table} a ON a.{hadm_col} = q.hadm_id "
                f"WHERE a.{death_col} IS NOT NULL "
                f"AND a.{death_col} <= COALESCE(q.intime, q.admittime) + NUMTODSINTERVAL({days}, 'DAY')"
                ") t",
                accuracy_mode=accuracy_mode,
            )
            if death_violation is None:
                add_invariant(
                    "death_within_days_exclusion",
                    True,
                    "death_within_days 검증 쿼리를 실행하지 못했습니다(권한/타임아웃 가능).",
                    None,
                )
            else:
                passed = death_violation == 0
                add_invariant(
                    "death_within_days_exclusion",
                    passed,
                    "death_within_days exclusion은 event-to-event 비교를 만족해야 합니다.",
                    death_violation,
                )
                if not passed:
                    negative_samples.extend(
                        self._execute_rows_sample(
                            f"SELECT q.subject_id, q.hadm_id, q.stay_id, a.{death_col} "
                            f"FROM ({base_sql}) q JOIN {admissions_table} a ON a.{hadm_col} = q.hadm_id "
                            f"WHERE a.{death_col} IS NOT NULL "
                            f"AND a.{death_col} <= COALESCE(q.intime, q.admittime) + NUMTODSINTERVAL({days}, 'DAY') "
                            f"FETCH FIRST {negative_sample_limit} ROWS ONLY",
                            accuracy_mode=accuracy_mode,
                        )
                    )
                    if _PDF_ACCURACY_FAIL_FAST_ON_INVARIANTS and accuracy_mode:
                        report["status"] = "failed"
                        report["invariants"] = invariants
                        report["negative_samples"] = negative_samples[:negative_sample_limit]
                        report["messages"].append("Fail-fast: death_within_days_exclusion invariant failed.")
                        return report

            if accuracy_mode and " LOS " in f" {str(cohort_sql or '').upper()} " and "DEATHTIME" not in str(cohort_sql or "").upper():
                add_invariant(
                    "within_days_not_los_substitution",
                    False,
                    "within-days 조건이 LOS로 대체된 흔적이 있습니다.",
                    1,
                )

        requirements = canonical_spec.get("requirements") if isinstance(canonical_spec.get("requirements"), list) else []
        if requirements:
            measurements_table = _schema_table(schema_map, "measurements", "SSO.CHARTEVENTS")
            stay_col = _schema_col(schema_map, "stay_id", "stay_id")
            meas_itemid_col = _schema_col(schema_map, "meas_itemid", "itemid")
            meas_time_col = _schema_col(schema_map, "meas_time", "charttime")
            for req in requirements:
                if not isinstance(req, dict):
                    continue
                if str(req.get("type") or "").strip().lower() != "measurement_required":
                    continue
                signals = req.get("signals") if isinstance(req.get("signals"), list) else []
                for signal in signals:
                    itemids = self._resolve_signal_itemids(schema_map, str(signal))
                    if not itemids:
                        add_invariant(
                            f"measurement_required_{signal}",
                            False,
                            f"signal_map에서 '{signal}' itemid를 찾지 못했습니다.",
                            1,
                        )
                        continue
                    itemids_text = ", ".join(str(item) for item in itemids)
                    miss_count = self._execute_scalar_count(
                        "SELECT COUNT(*) AS cnt FROM ("
                        f"SELECT q.stay_id FROM ({base_sql}) q "
                        f"WHERE NOT EXISTS ("
                        f"SELECT 1 FROM {measurements_table} m "
                        f"WHERE m.{stay_col} = q.stay_id "
                        f"AND m.{meas_itemid_col} IN ({itemids_text}) "
                        f"AND m.{meas_time_col} BETWEEN q.outtime - INTERVAL '24' HOUR AND q.outtime"
                        ")"
                        ") t",
                        accuracy_mode=accuracy_mode,
                    )
                    if miss_count is None:
                        add_invariant(
                            f"measurement_required_{signal}",
                            True,
                            f"measurement_required({signal}) 검증 쿼리를 실행하지 못했습니다.",
                            None,
                        )
                    else:
                        passed = miss_count == 0
                        add_invariant(
                            f"measurement_required_{signal}",
                            passed,
                            f"필수 시그널({signal})의 window 내 측정치가 있어야 합니다.",
                            miss_count,
                        )
                        if not passed:
                            negative_samples.extend(
                                self._execute_rows_sample(
                                    f"SELECT q.subject_id, q.hadm_id, q.stay_id FROM ({base_sql}) q "
                                    f"WHERE NOT EXISTS ("
                                    f"SELECT 1 FROM {measurements_table} m "
                                    f"WHERE m.{stay_col} = q.stay_id "
                                    f"AND m.{meas_itemid_col} IN ({itemids_text}) "
                                    f"AND m.{meas_time_col} BETWEEN q.outtime - INTERVAL '24' HOUR AND q.outtime"
                                    f") FETCH FIRST {negative_sample_limit} ROWS ONLY",
                                    accuracy_mode=accuracy_mode,
                                )
                            )

        anomalies = self._build_stepwise_anomalies(
            step_counts,
            intent_steps if isinstance(intent_steps, list) else [],
            accuracy_mode=accuracy_mode,
        )
        report["anomalies"] = anomalies
        report["invariants"] = invariants
        report["negative_samples"] = negative_samples[:negative_sample_limit]

        failed = any(not inv.get("passed", False) for inv in invariants)
        if failed:
            report["status"] = "failed"
        elif anomalies:
            report["status"] = "warning"
        else:
            report["status"] = "passed"

        if canonical_spec.get("ambiguities"):
            report["messages"].append("Ambiguities detected in CohortSpec. Resolve for deterministic behavior.")
        return report

    def _table_preview_text(self, rows: Any) -> str:
        if not isinstance(rows, list):
            return ""
        preview_rows = rows[:_PDF_ASSET_TABLE_ROWS]
        lines: list[str] = []
        for row in preview_rows:
            if isinstance(row, list):
                values = row[:_PDF_ASSET_TABLE_COLS]
            else:
                values = [row]
            cleaned = [str(cell or "").replace("\n", " ").strip() for cell in values]
            lines.append(" | ".join(cleaned))
        preview = "\n".join(lines).strip()
        if len(rows) > len(preview_rows):
            preview += f"\n... ({len(rows) - len(preview_rows)} more rows)"
        return preview[:_PDF_ASSET_TABLE_CHARS]

    async def _describe_image(self, image_bytes: bytes, page_no: int) -> str:
        """Vision 모델을 사용하여 이미지를 요약 설명합니다."""
        try:
            base64_image = base64.b64encode(image_bytes).decode('utf-8')
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini", # 비용 효율을 위해 mini 사용
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": f"이 이미지는 논문의 {page_no}페이지에서 추출되었습니다. 코호트 선정 기준(Flowchart, Inclusion/Exclusion)이나 환자 특성(Baseline)과 관련된 내용이 있다면 핵심만 요약해 주세요. 관련 없다면 'No clinical relevance'라고 답하세요."},
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"},
                            },
                        ],
                    }
                ],
                max_tokens=300,
            )
            summary = response.choices[0].message.content.strip()
            return summary if "No clinical relevance" not in summary else ""
        except Exception as e:
            logger.error(f"이미지 분석 실패: {e}")
            return ""

    async def _get_assets_summary(self, assets: dict) -> str:
        """추출된 표와 이미지 요약을 프롬프트용 텍스트로 변환"""
        summaries = []
        
        if assets.get("tables"):
            summaries.append("\n## EXTRACTED TABLES (PREVIEW)")
            for t in assets["tables"]:
                table_str = self._table_preview_text(t.get("content"))
                if table_str:
                    summaries.append(f"### [Page {t['page']}] Table\n{table_str}")

        if assets.get("figures"):
            summaries.append("\n## EXTRACTED FIGURES (ANALYSIS SKIPPED)")
            # 그림 분석(LLM)은 속도를 위해 생략하고 개수만 표시
            total_figures = 0
            for fig in assets["figures"]:
                if isinstance(fig, dict):
                    total_figures += int(fig.get("count") or 0)
            summaries.append(f"Total {total_figures} figures detected in this PDF.")
                    
        return "\n".join(summaries)

    def _load_rag_metadata(self, detected_vars: list = None) -> str:
        """RAG 프롬프트 강화를 위해 var/metadata의 핵심 정의들을 로드합니다."""
        meta_dir = Path("var/metadata")
        context_parts = []

        # 1. Derived Variables (SOFA, ROX, OASIS 등)
        try:
            dv_path = meta_dir / "derived_variables.json"
            if dv_path.exists():
                dv_data = json.loads(dv_path.read_text(encoding="utf-8"))
                defs = []
                for dv in dv_data.get("derived_variables", []):
                    derived_name = str(dv.get("derived_name") or "").strip()
                    if not derived_name:
                        continue
                    description = str(dv.get("description") or dv.get("definition") or "").strip()
                    sql_pattern = (
                        str(dv.get("sql_pattern") or "").strip()
                        or str((dv.get("oracle_template") or {}).get("strategy") or "").strip()
                        or "Complex logic"
                    )
                    defs.append(f"- {derived_name}: {description} (SQL: {sql_pattern})")
                context_parts.append("\n## DERIVED CLINICAL SCORES (USE THESE PATTERNS):\n" + "\n".join(defs))
        except Exception as e:
            logger.warning(f"Failed to load derived_variables.json: {e}")

        # 2. Comorbidity Specs (Charlson Index 등)
        try:
            cm_path = meta_dir / "cohort_comorbidity_specs.json"
            if cm_path.exists():
                cm_data = json.loads(cm_path.read_text(encoding="utf-8"))
                specs = []
                for cm in cm_data:
                    specs.append(f"- {cm['group_key']} ({cm['group_label']}): {cm.get('map_terms', [])}")
                context_parts.append("\n## COMORBIDITY MAPPING (ICD GROUPS):\n" + "\n".join(specs))
        except Exception as e:
            logger.warning(f"Failed to load cohort_comorbidity_specs.json: {e}")

        # 3. Schema Hints (Postprocess Rules)
        try:
            pp_path = meta_dir / "sql_postprocess_schema_hints.json"
            if pp_path.exists():
                pp_data = json.loads(pp_path.read_text(encoding="utf-8"))
                hints = []
                for table, cols in pp_data.get("tables", {}).items():
                    hints.append(f"- {table}: {', '.join(cols)}")
                context_parts.append("\n## KEY SCHEMA HINTS (TABLE COLUMNS):\n" + "\n".join(hints))
        except Exception as e:
            logger.warning(f"Failed to load sql_postprocess_schema_hints.json: {e}")

        # 4. [NEW] Detailed Variable Metadata (from mimic_rag_metadata_full.json)
        if detected_vars:
            try:
                # Docker path first, then local fallback (relative from this file)
                full_path_str = "/app/var/metadata/mimic_rag_metadata_full.json"
                if not os.path.exists(full_path_str):
                     full_path_str = os.path.join(os.path.dirname(__file__), "../../../../var/metadata/mimic_rag_metadata_full.json")
                
                full_path = Path(full_path_str)
                if full_path.exists():
                    full_data = json.loads(full_path.read_text(encoding="utf-8"))
                    var_hints = []
                    
                    # Create a lookup set for efficiency (normalize names)
                    detected_names = {_normalize_signal_name(v.get("signal_name", "")) for v in detected_vars}
                    
                    for item in full_data:
                        s_name = _normalize_signal_name(item.get("signal_name", ""))
                        # Check if this variable is relevant
                        if s_name in detected_names:
                            desc = item.get("description", "")
                            mapping = item.get("mapping", {})
                            hint = f"- **{item['signal_name']}**: {desc} (Table: {mapping.get('target_table')}, ItemID: {mapping.get('itemid')})"
                            var_hints.append(hint)
                            
                    if var_hints:
                        context_parts.append("\n## DETAILED VARIABLE SPECS (RELEVANT):\n" + "\n".join(var_hints))
            except Exception as e:
                logger.warning(f"Failed to load mimic_rag_metadata_full.json for RAG context: {e}")

        return "\n".join(context_parts)




    async def _extract_conditions(self, full_text: str, assets_summary: str = "", deterministic: bool = True) -> dict:
        """1단계: PDF 텍스트 및 자산 요약에서 코호트 선정 조건을 정규화된 JSON으로 추출"""
        rag_context = ""
        if not deterministic:
            rag_context = f"\n## REFERENCE COHORT EXAMPLES (RAG)\n{_load_reference_cohorts()}\n"

        prompt = f"""당신은 세계 최고의 임상 연구 정보 추출 전문가입니다.
제공된 논문 텍스트와 추출된 시각적 자산(표, 그림 요약)에서 '코호트 선정 조건(Eligibility/Inclusion/Exclusion)'을 누락 없이 정밀하게 추출하세요.

{rag_context}

[필수 요구사항]
1. **신속 정확한 추출**: 긴 설명보다는 JSON 필드를 정확히 채우는 데 집중하세요.
2. **시각적 정보 우선**: 텍스트와 표/그림의 수치가 다를 경우 표/그림(Flowchart)을 따르세요.
3. **핵심 요약**: `summary_ko`와 `criteria_summary_ko`는 각각 3문장 내외로 핵심만 요약하세요. (속도 최적화)
4. **논리적 분해**: 각 조건을 DB 필터링 로직 위주로 설명하세요.
5. **임상 변수 추출**: 주요 수치형 임상 변수를 찾아 `variables` 리스트에 담고, 반드시 단위(Unit)를 함께 명시하세요.
6. **모호성 표시**: first admission 기준(unit), ICD 버전, death time 필드가 불명확하면 `ambiguities`에 질문 형태로 추가하세요.

## 추출 대상 정보
### 1. TEXT CONTENT
{full_text}

### 2. VISUAL ASSETS (TABLES & FIGURES)
{assets_summary if assets_summary else "No additional assets extracted."}

[주의 사항 (Medical Guardrails)]
- **ICD 코드 변환**: 진단 조건이 나오면 질환명(Text)을 그대로 쓰지 말고, 반드시 상응하는 **ICD-9/10 코드(예: 850, 486)**를 찾아서 `variables`의 `codes` 파라미터에 배열로 담으세요. (예: `["850", "851"]`)
- **임상적 상식**: SOFA 점수는 패혈증 진단 시 통상 **2점 이상**을 의미합니다. 문맥 없이 0점이나 비상식적인 수치를 추출하지 마세요. 불명확하면 `is_mandatory: false`로 설정하세요.
- **원천 데이터 우선**: 복합 점수(SOFA, ROX)보다는 측정 가능한 원천 변수(혈압, 의식 수준, 호흡수) 추출에 집중하세요.

## 출력 JSON 스키마
{{
  "cohort_definition": {{
    "title": "논문 제목",
    "description": "Short description (English)",
    "summary_ko": "연구 요약 (핵심 3문장, 150~300자)",
    "criteria_summary_ko": "선정/제외 기준 요약 (핵심 3문장, 150~300자)",
    "extraction_details": {{
      "study_context": {{
        "data_source": "데이터 출처",
        "database_version": "버전",
        "study_period": "연구 기간",
        "setting": "설정"
      }},
      "cohort_criteria": {{
        "population": [
          {{
            "criterion": "선정/제외 기준 텍스트",
            "type": "inclusion|exclusion",
            "operational_definition": "DB 구현 로직 설명",
            "evidence": "[Source] 인용 원문 또는 요약",
            "evidence_source": {{
              "type": "text|figure|table",
              "page": "페이지 번호 (예: 1)"
            }}
          }}
        ],
        "index_unit": "patient|icu_stay",
        "first_stay_only": "Yes|No"
      }},
      "diagnosis_criteria": {{
        "coding_system": "ICD-10|ICD-9",
        "codes": ["코드 리스트"],
        "evidence": "[Source] 인용 원문",
        "evidence_source": {{
          "type": "text|figure|table",
          "page": "페이지 번호"
        }}
      }}
    }},
    "methods_summary": {{
      "structured_summary": {{
        "study_design_setting": "연구 설계",
        "data_source": "데이터 원천",
        "population_selection": "대상자 선정",
        "variables": "주요 변수",
        "outcomes": "결과 지표"
      }}
    }},
    "variables": [
      {{
        "signal_name": "변수명 (예: heart_rate)",
        "description": "변동성 설명 (예: Heart rate measured hourly during ICU stay)"
      }}
    ],
    "ambiguities": [
      {{
        "id": "amb_xxx",
        "question": "모호한 기준 질문",
        "options": ["옵션1", "옵션2"],
        "default_policy": "require_user_choice"
      }}
    ]
  }}
}}
"""
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "의료 논문 데이터 추출 전문가입니다. 반드시 유효한 JSON만 반환하세요."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0,
            seed=42
        )
        data = json.loads(response.choices[0].message.content)
        if not isinstance(data, dict):
            raise RuntimeError("Cohort extraction response is not a JSON object.")
        if _contains_sql_like_text(data):
            raise RuntimeError("Cohort extraction contains SQL-like text. SQL generation must be compiled, not authored by LLM.")
        if not isinstance(data.get("cohort_definition"), dict):
            data["cohort_definition"] = {}
        return data

    def _canonical_spec_to_intent(self, canonical_spec: dict[str, Any]) -> dict[str, Any]:
        inclusion = canonical_spec.get("inclusion") if isinstance(canonical_spec.get("inclusion"), list) else []
        exclusion = canonical_spec.get("exclusion") if isinstance(canonical_spec.get("exclusion"), list) else []
        requirements = canonical_spec.get("requirements") if isinstance(canonical_spec.get("requirements"), list) else []
        population = canonical_spec.get("population") if isinstance(canonical_spec.get("population"), dict) else {}
        metadata = canonical_spec.get("metadata") if isinstance(canonical_spec.get("metadata"), dict) else {}
        accuracy_mode = bool(metadata.get("accuracy_mode"))
        default_window = "icu_discharge_last_24h"
        if str(population.get("measurement_window") or "").strip() in WINDOW_TEMPLATES:
            default_window = str(population.get("measurement_window")).strip()
        steps: list[dict[str, Any]] = []

        def append_step(step: dict[str, Any]) -> None:
            if not isinstance(step, dict):
                return
            step.setdefault("is_mandatory", True)
            step.setdefault("window", "")
            steps.append(step)

        for item in inclusion:
            if not isinstance(item, dict):
                continue
            item_type = str(item.get("type") or "").strip().lower()
            item_id = str(item.get("id") or f"inc_{len(steps)+1}")
            if item_type in {"age_range", "age_rule"}:
                append_step(
                    {
                        "name": item_id,
                        "type": "age",
                        "params": {
                            "min": _safe_int(item.get("min"), default=0),
                            "max": _safe_int(item.get("max"), default=120),
                        },
                        "is_exclusion": False,
                    }
                )
            elif item_type == "diagnosis_icd_prefix":
                codes = item.get("codes") if isinstance(item.get("codes"), list) else []
                prefixes: list[str] = []
                icd_version = None
                for code in codes:
                    if not isinstance(code, dict):
                        continue
                    prefix = str(code.get("prefix") or "").strip().upper()
                    if prefix:
                        prefixes.append(prefix)
                    if code.get("icd_version") is not None:
                        icd_version = _safe_int(code.get("icd_version"), default=0) or None
                append_step(
                    {
                        "name": item_id,
                        "type": "diagnosis",
                        "params": {
                            "codes": prefixes,
                            "icd_version": icd_version,
                        },
                        "is_exclusion": False,
                    }
                )
            elif item_type == "measurement_required":
                append_step(
                    {
                        "name": item_id,
                        "type": "measurement_required",
                        "params": {
                            "signals": item.get("signals") if isinstance(item.get("signals"), list) else [],
                        },
                        "window": default_window,
                        "is_exclusion": False,
                    }
                )

        for item in exclusion:
            if not isinstance(item, dict):
                continue
            item_type = str(item.get("type") or "").strip().lower()
            item_id = str(item.get("id") or f"exc_{len(steps)+1}")
            if item_type == "icu_los_min_days":
                append_step(
                    {
                        "name": item_id,
                        "type": "icu_stay",
                        "params": {"min_los": float(item.get("min_days") or 1)},
                        "is_exclusion": True,
                    }
                )
            elif item_type == "death_within_days_of_index_event":
                append_step(
                    {
                        "name": item_id,
                        "type": "death_within_days",
                        "params": {"days": _safe_int(item.get("days"), default=3)},
                        "is_exclusion": True,
                    }
                )

        for item in requirements:
            if not isinstance(item, dict):
                continue
            if str(item.get("type") or "").strip().lower() != "measurement_required":
                continue
            item_id = str(item.get("id") or f"req_{len(steps)+1}")
            append_step(
                {
                    "name": item_id,
                    "type": "measurement_required",
                    "params": {
                        "signals": item.get("signals") if isinstance(item.get("signals"), list) else [],
                    },
                    "window": default_window,
                    "is_exclusion": False,
                }
            )
        if accuracy_mode:
            has_icu_los_exclusion = any(
                isinstance(step, dict)
                and str(step.get("type") or "").strip().lower() == "icu_stay"
                and bool(step.get("is_exclusion", False))
                for step in steps
            )
            if not has_icu_los_exclusion:
                append_step(
                    {
                        "name": "accuracy_default_icu_los_min_days",
                        "type": "icu_stay",
                        "params": {"min_los": 1.0},
                        "is_exclusion": True,
                    }
                )
        return {"steps": steps}

    async def _generate_sql_from_conditions(
        self,
        conditions_json: dict,
        *,
        population_policy: dict[str, Any] | None = None,
        canonical_spec: dict[str, Any] | None = None,
        schema_map: dict[str, Any] | None = None,
        accuracy_mode: bool = False,
        relax_mode: bool = False,
        deterministic: bool = True,
    ) -> dict:
        """2단계: 추출된 코호트 조건(JSON)을 바탕으로 'Intent JSON'을 생성하고 SQL로 컴파일"""
        derived_meta = _load_metadata_json(_DERIVED_VAR_PATH, _DERIVED_VAR_LOCAL)
        derived_names = [v.get("derived_name") for v in derived_meta.get("derived_variables", [])]
        intent: dict[str, Any]
        if accuracy_mode and isinstance(canonical_spec, dict):
            intent = self._canonical_spec_to_intent(canonical_spec)
        else:
            prompt = f"""당신은 MIMIC-IV 데이터베이스 전문가입니다.
제공된 코호트 정의를 바탕으로, SQL을 직접 쓰지 말고 아래 규칙에 따라 'Cohort Intent JSON'을 생성하세요.

## 규칙
1. **시그널 매핑 강제 (Guardrail)**: 너의 상식으로 itemid를 추측하지 마세요. 반드시 제공된 SIGNAL_MAP의 키워드만 사용하세요.
2. **타입 엄격 적용**: Vital Signs(HR, SBP, SpO2 등)은 `vital` 타입을, Lab 결과는 `lab` 타입을 사용하세요.
3. **파생 지표 토큰화**: SOFA, ROX 등 복잡한 지표는 `derived` 타입의 `name` 파라미터에 표준 토큰(sofa, rox, oasis)을 입력하세요.
4. **시간창(Window) 엄격 적용**: 날짜 계산을 직접 하지 말고, `window` 필드에 지정된 템플릿 이름(`icu_first_24h` 등)을 정확히 기입하세요.
5. **제외 로직 명시 (Exclusion)**: 제외 기준(Exclusion)에 해당하는 단계는 `"is_exclusion": true` 속성을 반드시 부여하세요.
6. **필수/권장 여부 (Relaxation)**: 연구의 핵심이 아닌 보조적 조건(예: 특정 Lab 수치 범위 등)은 `"is_mandatory": false`로 설정하여, 0명일 때 자동 완화될 수 있게 하세요.
7. **논리**: 신호들은 기본적으로 AND로 결합됩니다.
8. **ICU 체류시간 규칙 (중요)**:
   - "ICU stay < 24h 제외" 문구는 반드시 `type: "icu_stay"`, `params: {{"min_los": 1}}`, `is_exclusion: true`로 표현하세요.
   - `min_los`는 0보다 큰 값으로 넣으세요(일 단위, 24h=1).
9. **within-days 규칙**:
   - `death within X days` 조건은 `type: "death_within_days"` 와 `params.days`로 표현하고, LOS 기반으로 치환하지 마세요.
10. **측정치 필수 조건**:
   - 필수 측정치는 `type: "measurement_required"` 와 `params.signals` 배열로 표현하세요.

## 출력 JSON 형식
{{
  "steps": [
    {{ 
      "name": "단계 이름 (영어)", 
      "type": "age|gender|diagnosis|lab|icu_stay|vital|derived|death_within_days|measurement_required", 
      "params": {{ ... }},
      "window": "icu_first_24h|admission_first_24h|icu_discharge_last_24h",
      "is_exclusion": true/false,
      "is_mandatory": true/false
    }}
  ]
}}

COHORT JSON:
{json.dumps(conditions_json, ensure_ascii=False, indent=2)}
"""
            # 속도 최적화를 위해 Intent 생성 단계는 빠르고 정형화된 gpt-4o-mini 모델 사용
            mini_model = "gpt-4o-mini"
            response = await self.client.chat.completions.create(
                model=mini_model,
                messages=[
                    {"role": "system", "content": "MIMIC-IV 코호트 설계 전문가입니다. 인텐트 기반 JSON만 반환하세요."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0,
                seed=42
            )
            intent = json.loads(response.choices[0].message.content)
        if _contains_sql_like_text(intent):
            raise RuntimeError("Intent response contains SQL-like text. SQL must be compiled from intent.")
        intent = self._sanitize_intent(intent)
        
        # 0명 발생 시 완화 로직 (Relax Mode)
        if relax_mode:
            # 필수 조건만 남기거나 범위를 넓히는 로직 (현재는 LLM 가이드에 is_optional 위임)
            logger.info("완화 모드 활성화됨: 선택적 조건 필터링 검토")

        compiled = self.compile_oracle_sql(intent, population_policy=population_policy, schema_map=schema_map)
        compiled["intent"] = intent
        return compiled

    def _get_best_join_key(self, s_type, s_params) -> str:
        """가이드라인 3: 테이블 성격에 맞는 최적의 조인 키 선택"""
        # [Hospital Level Tables] -> hadm_id 사용
        # lab, diagnosis, prescription, microbiology, inputevents(일부), admissions
        hospital_tables = ["lab", "diagnosis", "prescription", "microbiology", "admissions", "procedures"]
        
        if s_type in hospital_tables:
            return "hadm_id"
            
        # [Stay Level Tables] -> stay_id 사용 (기본값)
        # vital, derived, icu_stay, chartevents, outputevents
        return "stay_id"

    def _sanitize_step_slug(self, value: Any) -> str:
        slug = re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower())
        slug = re.sub(r"_+", "_", slug).strip("_")
        return slug or "unknown"

    def _pick_first_text(self, value: Any) -> str:
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, (list, tuple, set)):
            for item in value:
                picked = self._pick_first_text(item)
                if picked:
                    return picked
            return ""
        if value is None:
            return ""
        return str(value).strip()

    def _normalize_step_type(self, raw_type: Any) -> str:
        step_type = self._pick_first_text(raw_type).lower()
        aliases = {
            "vitals": "vital",
            "vital_sign": "vital",
            "vital_signs": "vital",
            "icu_los": "icu_stay",
            "icu_los_days": "icu_stay",
            "diagnoses": "diagnosis",
            "dx": "diagnosis",
            "measurement": "measurement_required",
            "measurements": "measurement_required",
            "measurement_required": "measurement_required",
            "death_within_days_of_index_event": "death_within_days",
            "death_within_days": "death_within_days",
        }
        return aliases.get(step_type, step_type)

    def _normalize_window_key(self, raw_window: Any) -> str:
        window = self._pick_first_text(raw_window)
        if not window:
            return ""
        aliases = {
            "first_24h": "icu_first_24h",
            "icu_24h": "icu_first_24h",
            "admission_24h": "admission_first_24h",
            "pre_discharge_24h": "icu_discharge_last_24h",
        }
        normalized = aliases.get(window, window)
        if normalized in WINDOW_TEMPLATES:
            return normalized
        return ""

    def _sanitize_intent_step(self, step: Any) -> dict[str, Any] | None:
        if not isinstance(step, dict):
            return None
        step_type = self._normalize_step_type(step.get("type"))
        if not step_type:
            return None
        safe_params = step.get("params") if isinstance(step.get("params"), dict) else {}
        safe_step = {
            "name": self._pick_first_text(step.get("name")) or f"{step_type}_step",
            "type": step_type,
            "params": safe_params,
            "window": self._normalize_window_key(step.get("window")),
            "is_exclusion": bool(step.get("is_exclusion", False)),
            "is_mandatory": bool(step.get("is_mandatory", True)),
        }
        return safe_step

    def _sanitize_intent(self, intent: Any) -> dict[str, Any]:
        if not isinstance(intent, dict):
            return {"steps": []}
        raw_steps = intent.get("steps")
        if not isinstance(raw_steps, list):
            raw_steps = []
        sanitized_steps: list[dict[str, Any]] = []
        for step in raw_steps:
            safe = self._sanitize_intent_step(step)
            if safe:
                sanitized_steps.append(safe)
        return {"steps": sanitized_steps}

    def _resolve_signal_itemids(self, schema_map: dict[str, Any], signal_name: str) -> list[int]:
        normalized = _normalize_signal_name(signal_name)
        mapping = schema_map.get("signal_map") if isinstance(schema_map.get("signal_map"), dict) else {}
        raw = mapping.get(normalized) or mapping.get(signal_name) or {}
        itemids: list[int] = []
        if isinstance(raw, dict):
            raw_itemids = raw.get("itemids")
            if isinstance(raw_itemids, list):
                for item in raw_itemids:
                    try:
                        itemids.append(int(item))
                    except Exception:
                        continue
        if itemids:
            return sorted(set(itemids))

        meta = self.signal_metadata.get(normalized) if isinstance(self.signal_metadata, dict) else None
        itemid_text = str((meta or {}).get("itemid") or "").strip()
        for token in itemid_text.split(","):
            try:
                itemids.append(int(token.strip()))
            except Exception:
                continue
        return sorted(set(itemids))

    def _validate_schema_map_requirements(
        self,
        *,
        schema_map: dict[str, Any],
        canonical_spec: dict[str, Any],
        intent: dict[str, Any],
    ) -> list[str]:
        missing: list[str] = []
        required_tables = {"patients", "admissions", "diagnoses_icd"}
        required_columns = {"subject_id", "hadm_id", "anchor_age", "admittime", "icd_code"}
        population = canonical_spec.get("population") if isinstance(canonical_spec.get("population"), dict) else {}
        if bool(population.get("require_icu")):
            required_tables.add("icustays")
            required_columns.update({"stay_id", "icu_intime", "icu_outtime", "icu_los_days"})

        exclusion = canonical_spec.get("exclusion") if isinstance(canonical_spec.get("exclusion"), list) else []
        if any(isinstance(item, dict) and str(item.get("type") or "").strip().lower() == "death_within_days_of_index_event" for item in exclusion):
            required_columns.add("death_time")

        requirements = canonical_spec.get("requirements") if isinstance(canonical_spec.get("requirements"), list) else []
        for req in requirements:
            if not isinstance(req, dict):
                continue
            if str(req.get("type") or "").strip().lower() != "measurement_required":
                continue
            required_tables.add("measurements")
            required_columns.update({"meas_time", "meas_itemid"})
            for signal in req.get("signals") if isinstance(req.get("signals"), list) else []:
                if not self._resolve_signal_itemids(schema_map, str(signal)):
                    missing.append(f"signal_map.{signal}")

        tables = schema_map.get("tables") if isinstance(schema_map.get("tables"), dict) else {}
        cols = schema_map.get("columns") if isinstance(schema_map.get("columns"), dict) else {}
        for key in sorted(required_tables):
            if not str(tables.get(key) or "").strip():
                missing.append(f"tables.{key}")
        for key in sorted(required_columns):
            if not str(cols.get(key) or "").strip():
                missing.append(f"columns.{key}")

        steps = intent.get("steps") if isinstance(intent, dict) else []
        if isinstance(steps, list) and any(str((s or {}).get("type") or "").strip().lower() == "measurement_required" for s in steps if isinstance(s, dict)):
            required_tables.add("measurements")
            if not str(tables.get("measurements") or "").strip():
                missing.append("tables.measurements")

        return sorted(set(missing))

    def _sanitize_sql_params(self, raw_params: Any) -> dict[str, Any]:
        defaults: dict[str, Any] = {
            "min": 0,
            "max": 150,
            "operator": "=",
            "value": 0,
            "min_los": 0,
            "days": 0,
            "drug": "",
            "gender": "all",
            "codes": "''",
            "label": "",
            "signals": [],
            "icd_version": None,
        }
        params = raw_params if isinstance(raw_params, dict) else {}
        safe: dict[str, Any] = {**defaults}
        safe.update(params)

        def _coerce_num(value: Any, default: float) -> float:
            if isinstance(value, (int, float)):
                return float(value)
            text = str(value or "").strip().replace(",", "")
            if not text:
                return default
            try:
                return float(text)
            except (TypeError, ValueError):
                return default

        safe["min"] = _coerce_num(safe.get("min"), 0.0)
        safe["max"] = _coerce_num(safe.get("max"), 150.0)
        safe["value"] = _coerce_num(safe.get("value"), 0.0)
        safe["min_los"] = _coerce_num(safe.get("min_los"), 0.0)
        safe["days"] = _coerce_num(safe.get("days"), 0.0)
        if safe["min"] > safe["max"]:
            safe["min"], safe["max"] = safe["max"], safe["min"]

        operator = str(safe.get("operator") or "").strip()
        if operator not in {">", ">=", "<", "<=", "=", "!=", "<>"}:
            operator = "="
        safe["operator"] = operator

        safe["drug"] = re.sub(r"[\"'`]", "", str(safe.get("drug") or "").strip())
        safe["gender"] = str(safe.get("gender") or "all").strip().lower() or "all"
        safe["label"] = re.sub(r"[\"'`]", "", str(safe.get("label") or "").strip())
        if not isinstance(safe.get("signals"), list):
            picked = self._pick_first_text(safe.get("signals"))
            safe["signals"] = [picked] if picked else []
        if safe.get("icd_version") in {"", None}:
            safe["icd_version"] = None
        else:
            try:
                safe["icd_version"] = int(float(str(safe.get("icd_version")).strip()))
            except Exception:
                safe["icd_version"] = None
        return safe

    def _normalize_gender_filter(self, raw_gender: Any) -> str | None:
        """Normalize gender filter to Oracle PATIENTS.gender codes.
        Returns:
            - 'M' or 'F' for a concrete filter
            - None for broad/no-op filters (all/any/both/unknown)
        """
        broad_tokens = {
            "",
            "all",
            "any",
            "both",
            "both_sexes",
            "male_and_female",
            "m_f",
            "f_m",
            "na",
            "n_a",
            "n/a",
            "none",
            "unknown",
            "전체",
            "남녀",
            "모두",
        }
        male_tokens = {"m", "male", "man", "boy", "남", "남성"}
        female_tokens = {"f", "female", "woman", "girl", "여", "여성"}

        def _flatten(value: Any) -> list[str]:
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    return []
                parts = re.split(r"[,/|]+", text)
                return [p.strip() for p in parts if p and p.strip()]
            if isinstance(value, (list, tuple, set)):
                out: list[str] = []
                for item in value:
                    out.extend(_flatten(item))
                return out
            if value is None:
                return []
            return _flatten(str(value))

        normalized_codes: set[str] = set()
        for token in _flatten(raw_gender):
            key = re.sub(r"[^a-zA-Z0-9가-힣]+", "_", token.strip().lower()).strip("_")
            if key in broad_tokens:
                continue
            if key in male_tokens:
                normalized_codes.add("M")
                continue
            if key in female_tokens:
                normalized_codes.add("F")
                continue
            if key in {"m", "f"}:
                normalized_codes.add(key.upper())

        if not normalized_codes:
            return None
        if len(normalized_codes) > 1:
            # Requested both sexes -> equivalent to no filter
            return None
        return next(iter(normalized_codes))

    def _extract_select_keys(self, sql: str) -> set[str]:
        sql_text = str(sql or "")
        m = re.search(r"select\s+(.*?)\s+from\b", sql_text, flags=re.IGNORECASE | re.DOTALL)
        select_part = m.group(1).lower() if m else sql_text.lower()
        if "*" in select_part:
            return {"subject_id", "hadm_id", "stay_id"}
        available: set[str] = set()
        for key in ("subject_id", "hadm_id", "stay_id"):
            if re.search(rf"\b{key}\b", select_part):
                available.add(key)
        return available

    def _resolve_join_key(self, preferred_key: str, signal_sql: str) -> str | None:
        available = self._extract_select_keys(signal_sql)
        if not available:
            return None
        if preferred_key in available:
            return preferred_key
        for fallback in ("hadm_id", "stay_id", "subject_id"):
            if fallback in available:
                return fallback
        return next(iter(available))

    def compile_oracle_sql(
        self,
        intent: dict,
        population_policy: dict[str, Any] | None = None,
        schema_map: dict[str, Any] | None = None,
    ) -> dict:
        """Intent JSON을 바탕으로 실제 Oracle SQL(MIMIC-IV)을 조립합니다. (CTE 단계 누적)"""
        policy = population_policy if isinstance(population_policy, dict) else {}
        resolved_schema = _merge_dict(_default_pdf_schema_map(), schema_map if isinstance(schema_map, dict) else {})
        patients_table = _schema_table(resolved_schema, "patients", "SSO.PATIENTS")
        admissions_table = _schema_table(resolved_schema, "admissions", "SSO.ADMISSIONS")
        icustays_table = _schema_table(resolved_schema, "icustays", "SSO.ICUSTAYS")
        diagnoses_table = _schema_table(resolved_schema, "diagnoses_icd", "SSO.DIAGNOSES_ICD")
        measurements_table = _schema_table(resolved_schema, "measurements", "SSO.CHARTEVENTS")

        subject_col = _schema_col(resolved_schema, "subject_id", "subject_id")
        hadm_col = _schema_col(resolved_schema, "hadm_id", "hadm_id")
        stay_col = _schema_col(resolved_schema, "stay_id", "stay_id")
        admittime_col = _schema_col(resolved_schema, "admittime", "admittime")
        icu_intime_col = _schema_col(resolved_schema, "icu_intime", "intime")
        icu_outtime_col = _schema_col(resolved_schema, "icu_outtime", "outtime")
        icu_los_col = _schema_col(resolved_schema, "icu_los_days", "los")
        death_time_col = _schema_col(resolved_schema, "death_time", "deathtime")
        icd_code_col = _schema_col(resolved_schema, "icd_code", "icd_code")
        icd_version_col = _schema_col(resolved_schema, "icd_version", "icd_version")
        meas_time_col = _schema_col(resolved_schema, "meas_time", "charttime")
        meas_itemid_col = _schema_col(resolved_schema, "meas_itemid", "itemid")

        accuracy_mode = bool(policy.get("accuracy_mode", False))
        require_icu = bool(policy.get("require_icu", False))
        episode_selector = str(policy.get("episode_selector") or "all").strip().lower()
        if episode_selector not in {"first", "last", "all"}:
            episode_selector = "all"
        episode_unit = str(policy.get("episode_unit") or "per_hadm").strip().lower()
        if episode_unit not in {"per_subject", "per_hadm"}:
            episode_unit = "per_hadm"
        default_measurement_window = str(policy.get("measurement_window") or "icu_discharge_last_24h").strip()
        if default_measurement_window not in WINDOW_TEMPLATES:
            default_measurement_window = "icu_discharge_last_24h"
        if accuracy_mode:
            require_icu = True
            episode_selector = "first"
            episode_unit = "per_subject"
            default_measurement_window = "icu_discharge_last_24h"

        steps = intent.get("steps", [])
        ctes = []
        step_labels = []
        step_refs = []
        compile_warnings: list[str] = []

        def _safe_render_sql_template(
            template: str,
            params: dict[str, Any],
            *,
            step_name: str,
            signal_name: str,
        ) -> str | None:
            try:
                return str(template).format(**params)
            except (KeyError, ValueError, IndexError) as exc:
                logger.warning(
                    "Step '%s': SQL template render failed for '%s' (%s). Skipping step.",
                    step_name,
                    signal_name,
                    exc,
                )
                return None
        
        join_keyword = "JOIN" if require_icu else "LEFT JOIN"
        ctes.append(f"""population AS (
    SELECT
        a.{subject_col} AS subject_id,
        a.{hadm_col} AS hadm_id,
        i.{stay_col} AS stay_id,
        i.{icu_intime_col} AS intime,
        i.{icu_outtime_col} AS outtime,
        i.{icu_los_col} AS los,
        a.{admittime_col} AS admittime
    FROM {admissions_table} a
    {join_keyword} (
        SELECT
            {hadm_col},
            {stay_col},
            {icu_intime_col},
            {icu_outtime_col},
            {icu_los_col},
            ROW_NUMBER() OVER (PARTITION BY {hadm_col} ORDER BY {icu_intime_col}) AS rn
        FROM {icustays_table}
    ) i ON i.{hadm_col} = a.{hadm_col} AND i.rn = 1
)""")
        step_labels.append("Initial Population (Admissions)")
        step_refs.append("population")

        current_prev = "population"
        if require_icu:
            ctes.append("""population_require_icu AS (
    SELECT *
    FROM population
    WHERE stay_id IS NOT NULL
)""")
            current_prev = "population_require_icu"
            step_labels.append("Require ICU Stay")
            step_refs.append(current_prev)

        if episode_selector in {"first", "last"}:
            partition_key = "subject_id" if episode_unit == "per_subject" else "hadm_id"
            order_dir = "ASC" if episode_selector == "first" else "DESC"
            ctes.append(f"""population_episode_selector AS (
    SELECT *
    FROM (
        SELECT
            p.*,
            ROW_NUMBER() OVER (
                PARTITION BY p.{partition_key}
                ORDER BY p.intime {order_dir} NULLS LAST, p.admittime {order_dir} NULLS LAST
            ) AS rn_episode
        FROM {current_prev} p
    )
    WHERE rn_episode = 1
)""")
            current_prev = "population_episode_selector"
            step_labels.append(f"Episode Selector ({episode_selector} / {episode_unit})")
            step_refs.append(current_prev)
        
        for i, step in enumerate(steps):
            s_type = self._normalize_step_type(step.get("type"))
            if not s_type:
                logger.warning("Step '%s': missing type. Skipping step.", i + 1)
                continue
            s_params = step.get("params", {})
            window_key = self._normalize_window_key(step.get("window"))
            s_name = f"step_{i+1}_{self._sanitize_step_slug(s_type)}"
            is_exclusion = bool(step.get("is_exclusion", False))
            
            # Guard against malformed/empty params from LLM intent JSON.
            safe_params = self._sanitize_sql_params(s_params)
            
            # 정확도 우선 특수 규칙: death_within_days는 사건-사건 비교로만 처리
            if s_type == "death_within_days":
                days = max(0, int(float(safe_params.get("days") or 0)))
                if days <= 0:
                    days = 3
                exists_sql = (
                    f"SELECT 1 FROM {admissions_table} a "
                    f"WHERE a.{hadm_col} = p.hadm_id "
                    f"AND a.{death_time_col} IS NOT NULL "
                    f"AND a.{death_time_col} <= COALESCE(p.intime, p.admittime) + NUMTODSINTERVAL({days}, 'DAY')"
                )
                operator_exists = "NOT EXISTS" if is_exclusion else "EXISTS"
                cte_query = f"""SELECT p.*
FROM {current_prev} p
WHERE {operator_exists} (
    {exists_sql}
)"""
                ctes.append(f"{s_name} AS ({cte_query})")
                step_labels.append(self._pick_first_text(step.get("name")) or s_name)
                step_refs.append(s_name)
                current_prev = s_name
                continue

            if s_type == "measurement_required":
                raw_signals = safe_params.get("signals")
                signals = [str(sig).strip() for sig in raw_signals] if isinstance(raw_signals, list) else []
                signals = [sig for sig in signals if sig]
                if not signals:
                    logger.warning("Step '%s': measurement_required has no signals. Skipping step.", s_name)
                    continue
                effective_window_key = window_key or default_measurement_window
                if accuracy_mode:
                    effective_window_key = "icu_discharge_last_24h"
                window_expr = WINDOW_TEMPLATES.get(effective_window_key)
                if not window_expr:
                    window_expr = WINDOW_TEMPLATES[default_measurement_window]
                window_expr = window_expr.replace("s.charttime", f"m.{meas_time_col}").replace("p.", "c.")
                signal_itemids: dict[str, list[int]] = {}
                injected_itemids: set[int] = set()
                for signal in signals:
                    itemids = self._resolve_signal_itemids(resolved_schema, signal)
                    if not itemids:
                        logger.warning("Step '%s': signal_map for '%s' is empty. Skipping signal.", s_name, signal)
                        continue
                    signal_itemids[signal] = sorted(set(itemids))
                    injected_itemids.update(signal_itemids[signal])
                if not signal_itemids or not injected_itemids:
                    logger.warning("Step '%s': no valid signal itemids for measurement_required. Skipping step.", s_name)
                    continue
                injected_itemids_text = ", ".join(str(v) for v in sorted(injected_itemids))
                having_parts = []
                for signal, itemids in signal_itemids.items():
                    itemids_text = ", ".join(str(v) for v in itemids)
                    having_parts.append(
                        f"SUM(CASE WHEN m.{meas_itemid_col} IN ({itemids_text}) THEN 1 ELSE 0 END) > 0"
                    )
                meas_cte_name = f"{s_name}_meas_ok"
                ctes.append(f"""{meas_cte_name} AS (
    SELECT /*+ MATERIALIZE */ c.stay_id
    FROM {current_prev} c
    JOIN {measurements_table} m
      ON m.{stay_col} = c.stay_id
     AND {window_expr}
     AND m.{meas_itemid_col} IN ({injected_itemids_text})
    GROUP BY c.stay_id
    HAVING {' AND '.join(having_parts)}
)""")
                if is_exclusion:
                    filter_clause = (
                        f"NOT EXISTS (SELECT 1 FROM {meas_cte_name} x WHERE x.stay_id = p.stay_id)"
                    )
                else:
                    filter_clause = (
                        f"EXISTS (SELECT 1 FROM {meas_cte_name} x WHERE x.stay_id = p.stay_id)"
                    )
                cte_query = f"""SELECT p.*
FROM {current_prev} p
WHERE {filter_clause}"""
                ctes.append(f"{s_name} AS ({cte_query})")
                step_labels.append(self._pick_first_text(step.get("name")) or s_name)
                step_refs.append(s_name)
                current_prev = s_name
                continue

            # 가드레일: Vital은 ChartEvents 우선
            if s_type == "vital":
                v_signal = _normalize_signal_name(safe_params.get("signal"))
                if not isinstance(v_signal, str):
                    v_signal = _normalize_signal_name(self._pick_first_text(v_signal))
                if not isinstance(v_signal, str):
                    v_signal = str(v_signal or "").strip()
                if v_signal in self.signal_map:
                    raw_sql = self.signal_map[v_signal]
                    signal_sql = _safe_render_sql_template(
                        raw_sql,
                        safe_params,
                        step_name=s_name,
                        signal_name=str(v_signal or s_type),
                    )
                    if not signal_sql:
                        continue
                else:
                    logger.warning(f"Unknown vital signal: {v_signal}")
                    continue
            elif s_type == "derived":
                d_name = _normalize_signal_name(safe_params.get("name"))
                if not isinstance(d_name, str):
                    d_name = _normalize_signal_name(self._pick_first_text(d_name))
                if not isinstance(d_name, str):
                    d_name = str(d_name or "").strip()
                if d_name in self.signal_map:
                    signal_sql = _safe_render_sql_template(
                        self.signal_map[d_name],
                        safe_params,
                        step_name=s_name,
                        signal_name=d_name or s_type,
                    )
                    if not signal_sql:
                        continue
                else:
                    # 유효하지 않은 derived 변수의 경우, SSO 스키마를 붙여 admissions 조인으로 우회 (에러 방지)
                    logger.warning(f"Unknown derived signal: {d_name}. Falling back to admissions.")
                    # Fallback to ICUSTAYS for derived scores to prevent ORA-00942 (Missing Table)
                    signal_sql = (
                        f"SELECT {stay_col} AS stay_id, {icu_intime_col} AS charttime "
                        f"FROM {icustays_table} WHERE {stay_col} IS NOT NULL"
                    )
            elif s_type in self.signal_map:
                raw_sql = self.signal_map[s_type]
                if s_type in {"gender", "sex"}:
                    normalized_gender = self._normalize_gender_filter(
                        s_params.get("gender", safe_params.get("gender"))
                    )
                    if not normalized_gender:
                        logger.info(
                            "Step '%s': broad/empty gender filter detected (%s). Skipping no-op gender step.",
                            s_name,
                            s_params.get("gender", safe_params.get("gender")),
                        )
                        continue
                    safe_params["gender"] = normalized_gender
                if s_type == "icu_stay":
                    min_los_raw = s_params.get("min_los", safe_params.get("min_los", 0))
                    try:
                        min_los = float(str(min_los_raw).strip())
                    except (TypeError, ValueError):
                        min_los = 0.0

                    # Exclusion with los<=0 degenerates to "exclude everyone".
                    # Use a safe default (24h == 1 day) when threshold is missing/invalid.
                    if is_exclusion and min_los <= 0:
                        min_los = 1.0
                        logger.warning(
                            "Step '%s': exclusion icu_stay min_los is invalid (%s). Defaulting to 1 day.",
                            s_name,
                            min_los_raw,
                        )

                    if is_exclusion:
                        signal_sql = (
                            f"SELECT {stay_col} AS stay_id, {hadm_col} AS hadm_id, {icu_intime_col} AS charttime "
                            f"FROM {icustays_table} WHERE {icu_los_col} < {min_los:g}"
                        )
                    else:
                        safe_params["min_los"] = min_los
                        signal_sql = _safe_render_sql_template(
                            raw_sql,
                            safe_params,
                            step_name=s_name,
                            signal_name=s_type,
                        )
                        if not signal_sql:
                            continue
                elif s_type == "diagnosis":
                    raw_codes = s_params.get("codes", [])
                    if isinstance(raw_codes, list):
                        code_candidates: list[Any] = []
                        stack = list(raw_codes)
                        while stack:
                            item = stack.pop(0)
                            if isinstance(item, (list, tuple, set)):
                                stack.extend(list(item))
                            else:
                                code_candidates.append(item)
                    else:
                        raw_text = str(raw_codes or "")
                        code_candidates = raw_text.split(",") if "," in raw_text else [raw_text]

                    cleaned_codes: list[str] = []
                    for code in code_candidates:
                        normalized = re.sub(r"[^A-Za-z0-9]+", "", str(code or "")).upper().strip()
                        if normalized and normalized not in cleaned_codes:
                            cleaned_codes.append(normalized)

                    if not cleaned_codes:
                        logger.warning(
                            "Step '%s': diagnosis codes are empty. Skipping step to avoid invalid IN () SQL.",
                            s_name,
                        )
                        continue

                    code_conditions = [f"{icd_code_col} LIKE '{code}%'" for code in cleaned_codes]
                    version_filter = ""
                    if safe_params.get("icd_version") is not None:
                        raw_columns = schema_map.get("columns") if isinstance(schema_map, dict) else {}
                        has_explicit_icd_version = isinstance(raw_columns, dict) and bool(
                            str(raw_columns.get("icd_version") or "").strip()
                        )
                        if has_explicit_icd_version:
                            version_filter = f" AND {icd_version_col} = {int(safe_params['icd_version'])}"
                        else:
                            warning_msg = (
                                "icd_version 매핑이 없어 diagnosis_icd_prefix 필터를 버전 분리 없이 컴파일했습니다. "
                                "오탐 가능성이 있습니다."
                            )
                            compile_warnings.append(warning_msg)
                            logger.warning("Step '%s': %s", s_name, warning_msg)
                    signal_sql = (
                        f"SELECT {hadm_col} AS hadm_id FROM {diagnoses_table} "
                        f"WHERE ({' OR '.join(code_conditions)}){version_filter}"
                    )
                else:
                    signal_sql = _safe_render_sql_template(
                        raw_sql,
                        safe_params,
                        step_name=s_name,
                        signal_name=s_type,
                    )
                    if not signal_sql:
                        continue
            else:
                continue



            # 가이드라인 3: 동적 조인 키 적용 및 무결성 검사
            preferred_key = self._get_best_join_key(s_type, s_params)
            join_key = self._resolve_join_key(preferred_key, signal_sql)
            if not join_key:
                logger.warning(
                    "Step '%s': no identifier key (subject_id/hadm_id/stay_id) in SELECT list. Skipping step.",
                    s_name,
                )
                continue
            if join_key != preferred_key:
                logger.info(
                    "Step '%s': join key adjusted from '%s' to '%s' based on projected columns.",
                    s_name,
                    preferred_key,
                    join_key,
                )

            # 제외(Exclusion) 여부
            operator_exists = "NOT EXISTS" if is_exclusion else "EXISTS"

            # 가이드라인 2 & 3: EXISTS 기반의 정교한 시간창 비교 및 조인
            condition_parts = [f"s.{join_key} = p.{join_key}"]
            
            # 시간 정보(charttime)가 있는 경우에만 윈도우 필터 적용 (가드 로직)
            if window_key:
                if "charttime" in signal_sql.lower():
                    window_template = WINDOW_TEMPLATES.get(window_key)
                    if window_template:
                        condition_parts.append(window_template)
                else:
                    logger.info(f"Step '{s_name}' skipped window filter: No 'charttime' in SQL.")
            
            where_clause = " AND ".join(condition_parts)

            cte_query = f"""SELECT p.* 
FROM {current_prev} p
WHERE {operator_exists} (
    SELECT 1 FROM ({signal_sql}) s
    WHERE {where_clause}
)"""
            ctes.append(f"{s_name} AS ({cte_query})")
            step_labels.append(self._pick_first_text(step.get("name")) or s_name)
            step_refs.append(s_name)
            current_prev = s_name

        # 최종 쿼리 조립
        cte_str = ",\n".join(ctes)
        
        cohort_sql = f"WITH {cte_str}\nSELECT * FROM {current_prev} FETCH FIRST 100 ROWS ONLY"
        count_sql = f"WITH {cte_str}\nSELECT count(*) as patient_count FROM {current_prev}"
        
        # Funnel SQL (Step Counts)
        debug_parts = []
        for label, cte_ref in zip(step_labels, step_refs):
            safe_label = str(label or "").replace("'", "''")
            debug_parts.append(f"SELECT '{safe_label}' as step_name, count(*) as cnt FROM {cte_ref}")
        debug_parts.append(f"SELECT 'Final Cohort' as step_name, count(*) as cnt FROM {current_prev}")
        
        debug_count_sql = f"WITH {cte_str}\n" + " UNION ALL ".join(debug_parts)

        return {
            "cohort_sql": cohort_sql,
            "count_sql": count_sql,
            "debug_count_sql": debug_count_sql,
            "warning": compile_warnings,
        }

    def _map_clinical_variables(self, extracted_vars: list) -> list:
        """추출된 임상 변수들을 self.signal_metadata와 대조하여 실제 DB 매칭 정보 추가"""
        mapped_vars = []
        derived_meta = _load_metadata_json(_DERIVED_VAR_PATH, _DERIVED_VAR_LOCAL)
        derived_vars = derived_meta.get("derived_variables", [])
        
        for v in extracted_vars:
            raw_signal_name = str(v.get("signal_name", ""))
            signal_name = _normalize_signal_name(raw_signal_name)
            if not signal_name:
                continue
            # 1. self.signal_metadata 직접 매핑 확인
            mapping = self.signal_metadata.get(signal_name)
            
            # 2. 파생 변수(Derived Variables) 메타데이터 확인
            if not mapping:
                matched_d = next(
                    (
                        d for d in derived_vars
                        if _normalize_signal_name(d.get("derived_name", "")) == signal_name
                        or signal_name in {_normalize_signal_name(a) for a in d.get("aliases", [])}
                    ),
                    None,
                )
                if matched_d:
                    mapping = {"target_table": "DERIVED", "itemid": f"Score: {matched_d['derived_name']}"}
            
            # 3. 근접 매칭 (퍼지) - 단순 구현
            if not mapping:
                matches = get_close_matches(signal_name, self.signal_metadata.keys(), n=1, cutoff=0.7)
                if matches:
                    mapping = self.signal_metadata[matches[0]]

            if mapping:
                v["mapping"] = {
                    "target_table": str(mapping.get("target_table") or "Unknown"),
                    "itemid": str(mapping.get("itemid") or "N/A"),
                }
            else:
                v["mapping"] = {"target_table": "Unknown", "itemid": "N/A"}
            mapped_vars.append(v)

        def _sort_key(item: dict[str, Any]) -> tuple[int, str, str, str]:
            mapping = item.get("mapping") if isinstance(item.get("mapping"), dict) else {}
            table_name = str(mapping.get("target_table") or "Unknown").strip()
            signal_name = str(item.get("signal_name") or "").strip()
            item_id = str(mapping.get("itemid") or "N/A").strip()
            unknown_rank = 1 if table_name.lower() in {"", "unknown", "n/a"} else 0
            return (unknown_rank, table_name.lower(), signal_name.lower(), item_id.lower())

        mapped_vars.sort(key=_sort_key)

        logger.info(f"Clinical variables mapped: {len(mapped_vars)} items found.")
        return mapped_vars

    def _build_features(self, mapped_variables: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """프론트엔드 배너/카드용 features 배열 생성 및 정렬"""
        features: list[dict[str, Any]] = []
        for item in mapped_variables:
            if not isinstance(item, dict):
                continue

            signal_name = str(item.get("signal_name") or "").strip()
            if not signal_name:
                continue

            description = str(item.get("description") or "").strip()
            mapping = item.get("mapping") if isinstance(item.get("mapping"), dict) else {}
            table_name = str(mapping.get("target_table") or "Unknown").strip() or "Unknown"
            item_id = str(mapping.get("itemid") or "N/A").strip() or "N/A"
            features.append({
                "name": signal_name,
                "description": description,
                "table_name": table_name,
                "itemid": item_id,
            })

        features.sort(
            key=lambda row: (
                1 if str(row.get("table_name") or "").lower() in {"", "unknown", "n/a"} else 0,
                str(row.get("table_name") or "").lower(),
                str(row.get("name") or "").lower(),
                str(row.get("itemid") or "").lower(),
            )
        )
        return features


    def _calculate_prompt_hash(self, relax_mode: bool, deterministic: bool) -> str:
        """프롬프트 지시문의 해시를 계산하여 로직 변경 여부를 추적"""
        # 실제 텍스트가 아닌 '지침(Instruction)' 부분만 취합하여 해싱
        instructions = [
            "당신은 세계 최고의 임상 연구 정보 추출 전문가입니다.",
            "3. 엄격한 준수: 논문에 명시된 조건을 절대 변경하지 마세요." if not relax_mode else "3. 결과 보장: 조건을 유연하게 적용하세요.",
            "Deterministic" if deterministic else "RAG-enabled",
            _load_schema_for_prompt()[:100] # 스키마 일부 포함
        ]
        return hashlib.sha256("".join(instructions).encode("utf-8")).hexdigest()[:12]

    async def verify_sql_integrity(self, sql: str) -> tuple[bool, str]:
        """가이드라인 5: schema_catalog 기반 사후 검증"""
        catalog = _load_metadata_json(_SCHEMA_CATALOG_PATH, _SCHEMA_CATALOG_LOCAL)
        if not catalog:
            return True, "No catalog found for verification"

        tables_meta = catalog.get("tables", {})
        
        # 간단한 정규식으로 사용하는 테이블명 추출 (SSO.TABLE_NAME)
        found_tables = re.findall(r'SSO\.([A-Za-z0-9_]+)', sql.upper())
        for tname in set(found_tables):
            if tname not in tables_meta:
                return False, f"Table '{tname}' does not exist in schema_catalog."
            
            # 해당 테이블의 컬럼 존재 확인 (기본적인 것만 체크)
            # 여기서는 쿼리 전체에서 컬럼명을 추출하기 어려우므로 테이블 존재 여부를 우선으로 함
            
        return True, "Integrity check passed"

    def _should_run_rag_refinement(self, db_result: dict[str, Any]) -> bool:
        """RAG 고도화 실행 여부를 결정합니다.
        - always: 항상 실행
        - off: 항상 생략
        - auto: 실패/0건/식별자 누락일 때만 실행
        """
        if _PDF_RAG_REFINEMENT_MODE == "always":
            return True
        if _PDF_RAG_REFINEMENT_MODE == "off":
            return False

        if db_result.get("error"):
            return True
        row_count = int(db_result.get("row_count") or 0)
        if row_count <= 0:
            return True
        cols = _normalize_result_columns(db_result.get("columns", []))
        if not _has_identifier_columns(cols):
            return True
        return False

    async def analyze_and_generate_sql(
        self,
        file_content: bytes,
        *,
        filename: str | None = None,
        user_id: str | None = None,
        relax_mode: bool = False,
        deterministic: bool = True,
        reuse_existing: bool = True,
        accuracy_mode: bool | None = None,
    ) -> dict:
        """
        PDF 분석을 2단계(추출 -> SQL 생성)로 수행하고 결과 집계.
        reuse_existing=True이면 동일 환경(Version/Mode/Hash)의 캐시를 즉시 반환합니다.
        False이면 캐시를 무시하고 강제로 재생성하여 업데이트합니다.
        """
        # Bump cache version when SQL assembly logic changes to avoid stale results.
        pipeline_version = "v59"
        accuracy_on = _PDF_ACCURACY_MODE_DEFAULT if accuracy_mode is None else bool(accuracy_mode)
        file_hash = hashlib.sha256(file_content).hexdigest()
        model_name = self.model
        prompt_hash = self._calculate_prompt_hash(relax_mode, deterministic)
        
        store = get_state_store()
        
        # 1-0. 최종 확정 데이터(pdf_confirmed_cohorts) 확인 - 최우선 순위
        if store:
            from app.services.runtime.state_store import AppStateStore
            confirmed_store = AppStateStore(collection_name="pdf_confirmed_cohorts")
            confirmed = confirmed_store.get(file_hash)
            if confirmed and confirmed.get("status") == "confirmed":
                logger.info(f"최종 확정된 코호트 데이터 발견 및 반환 (File Hash: {file_hash})")
                confirmed["pdf_hash"] = file_hash # 보장
                return confirmed

        # PK 생성: pdf_hash + relax_mode + deterministic + pipeline_version
        cache_key = f"pdf_analysis::{pipeline_version}::{file_hash}::{relax_mode}::{deterministic}::{accuracy_on}"
        
        # 1-1. Primary Cache 확인 (reuse_existing=True일 때만)
        if store and reuse_existing:
            cached = store.get(cache_key)
            if cached:
                logger.info(f"PDF 분석 결과 임시 캐시 적중 (File Hash: {file_hash}, Version: {pipeline_version})")
                cached["pdf_hash"] = file_hash # 보장
                return cached

        # 0 & 1-3. 병렬 처리 시작: 텍스트 추출과 시각적 자산 요약을 동시에 대기
        logger.info("병렬 작업 시작: 텍스트 추출 및 자산 요약 대기")
        import asyncio
        
        # 0. 텍스트 및 자산 기본 추출 (File IO/Parsing)
        extracted_task = self._extract_pdf_content_async(file_content)
        
        # 1-2. Secondary Cache 확인 (Canonical Hash 기반 - reuse_existing=True일 때만)
        # 이 부분은 extracted_task가 완료되어 full_text를 얻어야 canonical_text를 만들 수 있으므로,
        # 병렬 처리 후 또는 extracted_task 완료 직후에 수행해야 합니다.
        # 현재 구조에서는 extracted_task 완료 후 진행하는 것이 자연스럽습니다.
        
        extracted = await extracted_task # Wait for extraction to complete
        full_text = extracted["full_text"]
        assets = extracted["assets"]
        page_count = int(extracted.get("page_count") or 0)
        pages_scanned = int(extracted.get("pages_scanned") or 0)
        
        canonical_text = self._canonicalize_text(full_text)
        canonical_hash = hashlib.sha256(canonical_text.encode("utf-8")).hexdigest()

        # 1-2. Secondary Cache 확인 (Canonical Hash 기반 - reuse_existing=True일 때만)
        if store and reuse_existing:
            matched = store.find_one({
                "value.canonical_hash": canonical_hash,
                "value.relax_mode": relax_mode,
                "value.deterministic": deterministic,
                "value.pipeline_version": pipeline_version,
                "value.accuracy_mode": accuracy_on,
            })
            if matched:
                logger.info(f"PDF 분석 결과 Canonical 캐시 적중 (Canonical Hash: {canonical_hash})")
                result = matched.get("value", {})
                result["pdf_hash"] = file_hash
                store.set(cache_key, result)
                return result

        # 1-3. 시각적 자산 요약 생성 (비동기 처리)
        logger.info("자산(표/그림) 요약 생성 시작")
        assets_summary_task = self._get_assets_summary(assets)
        
        if not reuse_existing:
            logger.info(f"PDF 신규 분석 강제 실행 (reuse_existing=False, File Hash: {file_hash})")
            
        # Wait for assets_summary_task to complete
        try:
            assets_summary = await assets_summary_task
        except Exception as e:
            logger.warning(f"자산 요약 생성 실패 (Text-only fallback 실행): {e}")
            assets_summary = "" # 실패 시 빈 문자열로 유지하여 텍스트 분석으로 진행

        focused_text = self._build_focus_text(full_text)
        logger.info(
            "조건 추출 입력 길이 최적화: raw=%d chars, focused=%d chars",
            len(full_text),
            len(focused_text),
        )

        # 1단계: 코호트 조건 추출
        logger.info(f"1단계: 코호트 조건 추출 시작 (Deterministic: {deterministic})")
        conditions = await self._extract_conditions(
            focused_text,
            assets_summary=assets_summary,
            deterministic=deterministic,
        )
        adaptive_extract = run_adaptive_extraction(
            focused_text,
            start_level="fast",
        )
        snippets = adaptive_extract.get("snippets") if isinstance(adaptive_extract.get("snippets"), list) else []
        adaptive_level = str(adaptive_extract.get("level") or "fast")
        adaptive_logs = adaptive_extract.get("log") if isinstance(adaptive_extract.get("log"), list) else []
        risk_score = int((adaptive_extract.get("risk") or {}).get("risk_score") or 0)
        risk_flags = (adaptive_extract.get("risk") or {}).get("flags") if isinstance(adaptive_extract.get("risk"), dict) else {}
        conditions, ambiguities = self._enrich_conditions_with_evidence(conditions, snippets)
        population_policy = self._derive_population_policy(conditions, accuracy_mode=accuracy_on)
        critic_notes: list[str] = []

        canonical_spec = self._build_canonical_spec(
            conditions,
            file_hash=file_hash,
            snippets=snippets,
            ambiguities=ambiguities,
            accuracy_mode=accuracy_on,
        )
        canonical_spec, evidence_summary = enforce_condition_evidence(canonical_spec)
        schema_validated_spec, schema_errors = validate_cohort_spec(canonical_spec)
        canonical_spec = schema_validated_spec if isinstance(schema_validated_spec, dict) else canonical_spec
        if schema_errors:
            critic_notes.extend([f"spec_schema:{msg}" for msg in schema_errors])
        type_warnings = validate_supported_types(canonical_spec)
        if type_warnings:
            critic_notes.extend([f"type_catalog:{msg}" for msg in type_warnings])

        if accuracy_on and adaptive_level == "fast":
            if should_upgrade_to_accurate(
                ambiguity_count=len(ambiguities),
                evidence_coverage=float(evidence_summary.get("coverage") or 0.0),
                risk_score=risk_score,
            ):
                upgraded = run_adaptive_extraction(focused_text, start_level=adaptive_level, force_level="accurate")
                snippets = upgraded.get("snippets") if isinstance(upgraded.get("snippets"), list) else snippets
                adaptive_level = str(upgraded.get("level") or adaptive_level)
                if isinstance(upgraded.get("log"), list):
                    adaptive_logs.extend(upgraded["log"])
                conditions, ambiguities = self._enrich_conditions_with_evidence(conditions, snippets)
                canonical_spec = self._build_canonical_spec(
                    conditions,
                    file_hash=file_hash,
                    snippets=snippets,
                    ambiguities=ambiguities,
                    accuracy_mode=accuracy_on,
                )
                canonical_spec, evidence_summary = enforce_condition_evidence(canonical_spec)

        if accuracy_on and should_upgrade_to_strict(
            ambiguity_count=len(ambiguities),
            evidence_coverage=float(evidence_summary.get("coverage") or 0.0),
            validator_failed=False,
            measurement_required=has_measurement_required(canonical_spec),
            has_icd_shorthand=bool((risk_flags or {}).get("code_normalization")) or has_icd_shorthand_risk(canonical_spec),
        ):
            if adaptive_level != "strict":
                strict_extracted = run_adaptive_extraction(
                    focused_text,
                    start_level=adaptive_level,
                    force_level="strict",
                )
                snippets = strict_extracted.get("snippets") if isinstance(strict_extracted.get("snippets"), list) else snippets
                adaptive_level = str(strict_extracted.get("level") or "strict")
                if isinstance(strict_extracted.get("log"), list):
                    adaptive_logs.extend(strict_extracted["log"])
                conditions, ambiguities = self._enrich_conditions_with_evidence(conditions, snippets)
                canonical_spec = self._build_canonical_spec(
                    conditions,
                    file_hash=file_hash,
                    snippets=snippets,
                    ambiguities=ambiguities,
                    accuracy_mode=accuracy_on,
                )
                canonical_spec, evidence_summary = enforce_condition_evidence(canonical_spec)

        if accuracy_on:
            revised_spec, critic_extra = await self._critic_cohort_spec(
                snippets=snippets,
                canonical_spec=canonical_spec,
            )
            critic_notes.extend(critic_extra)
            canonical_spec, evidence_summary = enforce_condition_evidence(
                revised_spec if isinstance(revised_spec, dict) else canonical_spec
            )
            schema_validated_spec, schema_errors = validate_cohort_spec(canonical_spec)
            canonical_spec = schema_validated_spec if isinstance(schema_validated_spec, dict) else canonical_spec
            if schema_errors:
                critic_notes.extend([f"spec_schema:{msg}" for msg in schema_errors])
            type_warnings = validate_supported_types(canonical_spec)
            if type_warnings:
                critic_notes.extend([f"type_catalog:{msg}" for msg in type_warnings])

        schema_map = _load_pdf_schema_map()
        resolved_ambiguity = resolve_ambiguities(
            spec=canonical_spec,
            schema_map=schema_map,
            pdf_hash=file_hash,
            limit=3,
        )
        canonical_spec = resolved_ambiguity.get("spec") if isinstance(resolved_ambiguity.get("spec"), dict) else canonical_spec
        ambiguities = resolved_ambiguity.get("ambiguities") if isinstance(resolved_ambiguity.get("ambiguities"), list) else []
        ambiguity_questions = resolved_ambiguity.get("questions") if isinstance(resolved_ambiguity.get("questions"), list) else []
        strict_ambiguity_mode = _PDF_STRICT_AMBIGUITY_MODE or accuracy_on

        if strict_ambiguity_mode and ambiguities:
            blocked_result = {
                "columns": [],
                "rows": [],
                "step_counts": [],
                "row_count": 0,
                "total_count": None,
                "error": "Ambiguity resolution required before SQL compilation.",
                "warning": ["모호한 조건이 있어 SQL 생성을 중단했습니다. ambiguity를 먼저 확정하세요."],
            }
            final_response = {
                "status": "needs_user_input",
                "pdf_hash": file_hash,
                "canonical_hash": canonical_hash,
                "filename": str(filename or "").strip() or "uploaded.pdf",
                "user_id": str(user_id or "").strip() or None,
                "relax_mode": relax_mode,
                "deterministic": deterministic,
                "accuracy_mode": accuracy_on,
                "pdf_page_count": page_count,
                "pdf_pages_scanned": pages_scanned,
                "pipeline_version": pipeline_version,
                "model": model_name,
                "prompt_hash": prompt_hash,
                "cohort_definition": conditions.get("cohort_definition", {}),
                "cohort_spec": canonical_spec,
                "ambiguity_resolution_required": True,
                "ambiguities": ambiguities,
                "ambiguity_questions": ambiguity_questions,
                "snippets": snippets,
                "adaptive_extract": {
                    "level": adaptive_level,
                    "log": adaptive_logs,
                    "risk": adaptive_extract.get("risk") if isinstance(adaptive_extract.get("risk"), dict) else {},
                    "evidence_summary": evidence_summary,
                },
                "mapped_variables": [],
                "cohort_conditions": conditions.get("cohort_definition", {}).get("extraction_details", {}).get("cohort_criteria", {}).get("population", []),
                "features": [],
                "generated_sql": {"cohort_sql": None, "count_sql": None, "debug_count_sql": None},
                "validation_report": {
                    "enabled": _PDF_VALIDATION_ENABLED,
                    "status": "blocked",
                    "accuracy_mode": accuracy_on,
                    "invariants": [],
                    "stepwise_counts": [],
                    "anomalies": [],
                    "negative_samples": [],
                    "messages": ["Ambiguity resolution is required before SQL compilation.", *critic_notes],
                },
                "accuracy_report": self._build_accuracy_metrics(
                    canonical_spec=canonical_spec,
                    validation_report={"status": "blocked"},
                ),
                "db_result": blocked_result,
                "next_action": {"type": "resolve_ambiguities", "ambiguities": ambiguity_questions or ambiguities},
            }
            return final_response

        precheck_intent = self._canonical_spec_to_intent(canonical_spec) if accuracy_on else {"steps": []}
        if accuracy_on:
            schema_missing = self._validate_schema_map_requirements(
                schema_map=schema_map,
                canonical_spec=canonical_spec,
                intent=precheck_intent,
            )
            if schema_missing:
                blocked_result = {
                    "columns": [],
                    "rows": [],
                    "step_counts": [],
                    "row_count": 0,
                    "total_count": None,
                    "error": "SchemaMap is incomplete for accuracy mode.",
                    "warning": ["SchemaMap 필수 항목이 누락되어 SQL 생성을 중단했습니다."],
                }
                return {
                    "status": "needs_user_input",
                    "pdf_hash": file_hash,
                    "canonical_hash": canonical_hash,
                    "filename": str(filename or "").strip() or "uploaded.pdf",
                    "user_id": str(user_id or "").strip() or None,
                    "relax_mode": relax_mode,
                    "deterministic": deterministic,
                    "accuracy_mode": accuracy_on,
                    "pdf_page_count": page_count,
                    "pdf_pages_scanned": pages_scanned,
                    "pipeline_version": pipeline_version,
                    "model": model_name,
                    "prompt_hash": prompt_hash,
                    "cohort_definition": conditions.get("cohort_definition", {}),
                    "cohort_spec": canonical_spec,
                    "ambiguity_resolution_required": True,
                    "ambiguities": ambiguities,
                    "ambiguity_questions": ambiguity_questions,
                    "snippets": snippets,
                    "adaptive_extract": {
                        "level": adaptive_level,
                        "log": adaptive_logs,
                        "risk": adaptive_extract.get("risk") if isinstance(adaptive_extract.get("risk"), dict) else {},
                        "evidence_summary": evidence_summary,
                    },
                    "mapped_variables": [],
                    "cohort_conditions": conditions.get("cohort_definition", {}).get("extraction_details", {}).get("cohort_criteria", {}).get("population", []),
                    "features": [],
                    "generated_sql": {"cohort_sql": None, "count_sql": None, "debug_count_sql": None, "intent": precheck_intent},
                    "validation_report": {
                        "enabled": _PDF_VALIDATION_ENABLED,
                        "status": "blocked",
                        "accuracy_mode": True,
                        "invariants": [],
                        "stepwise_counts": [],
                        "anomalies": [],
                        "negative_samples": [],
                        "messages": ["SchemaMap is incomplete.", *critic_notes],
                    },
                    "accuracy_report": self._build_accuracy_metrics(
                        canonical_spec=canonical_spec,
                        validation_report={"status": "blocked"},
                    ),
                    "db_result": blocked_result,
                    "next_action": {"type": "schema_map_required", "missing": schema_missing},
                }
        
        # 2단계: SQL 생성
        logger.info(f"2단계: SQL 생성 시작 (Relax Mode: {relax_mode}, Deterministic: {deterministic})")
        sql_result = await self._generate_sql_from_conditions(
            conditions,
            population_policy=population_policy,
            canonical_spec=canonical_spec,
            schema_map=schema_map,
            accuracy_mode=accuracy_on,
            relax_mode=relax_mode,
            deterministic=deterministic,
        )
        intent_payload = sql_result.get("intent") if isinstance(sql_result, dict) else {}
        
        # 3. SQL 정제 및 최적화 힌트 추가
        for key in ["cohort_sql", "count_sql", "debug_count_sql"]:
            if key in sql_result:
                sql = str(sql_result[key]).strip().rstrip(";").replace("`", "")
                # 대량 데이터 처리를 위한 Parallel 힌트 강제 삽입 (Oracle 최적화)
                if "SELECT" in sql and "/*+" not in sql:
                    sql = sql.replace("SELECT", "SELECT /*+ PARALLEL(4) */", 1)
                sql = re.sub(r'"([A-Za-z_]+)"', r'\1', sql)
                sql_result[key] = sql

        # 3.5 SQL Integrity Verification (Guideline 5)
        if "cohort_sql" in sql_result:
            is_valid, msg = await self.verify_sql_integrity(sql_result["cohort_sql"])
            if not is_valid:
                logger.warning(f"SQL Integrity Warning: {msg}")
                # 에러 메시지를 결과에 포함시켜 UI에서 인지 가능하게 함
                if not sql_result.get("warning"):
                    sql_result["warning"] = []
                sql_result["warning"].append(msg)

        sql_result = apply_oracle_compiler_guards(sql_result, accuracy_mode=accuracy_on)
        compiler_guard = sql_result.get("compiler_guard") if isinstance(sql_result.get("compiler_guard"), dict) else {}
        if accuracy_on and bool(sql_result.get("blocked")):
            blocked_result = {
                "columns": [],
                "rows": [],
                "step_counts": [],
                "row_count": 0,
                "total_count": None,
                "error": "SQL compile blocked by anti-pattern guard.",
                "warning": [str(v.get("message") or "") for v in compiler_guard.get("violations", []) if isinstance(v, dict)],
            }
            return {
                "status": "validation_failed",
                "pdf_hash": file_hash,
                "canonical_hash": canonical_hash,
                "filename": str(filename or "").strip() or "uploaded.pdf",
                "user_id": str(user_id or "").strip() or None,
                "relax_mode": relax_mode,
                "deterministic": deterministic,
                "accuracy_mode": accuracy_on,
                "pdf_page_count": page_count,
                "pdf_pages_scanned": pages_scanned,
                "pipeline_version": pipeline_version,
                "model": model_name,
                "prompt_hash": prompt_hash,
                "cohort_definition": conditions.get("cohort_definition", {}),
                "cohort_spec": canonical_spec,
                "ambiguity_resolution_required": bool(ambiguities),
                "ambiguities": ambiguities,
                "ambiguity_questions": ambiguity_questions,
                "snippets": snippets,
                "adaptive_extract": {
                    "level": adaptive_level,
                    "log": adaptive_logs,
                    "risk": adaptive_extract.get("risk") if isinstance(adaptive_extract.get("risk"), dict) else {},
                    "evidence_summary": evidence_summary,
                },
                "mapped_variables": [],
                "cohort_conditions": conditions.get("cohort_definition", {}).get("extraction_details", {}).get("cohort_criteria", {}).get("population", []),
                "features": [],
                "generated_sql": {
                    "cohort_sql": sql_result.get("cohort_sql"),
                    "count_sql": sql_result.get("count_sql"),
                    "debug_count_sql": sql_result.get("debug_count_sql"),
                    "intent": intent_payload if isinstance(intent_payload, dict) else {},
                    "compiler_guard": compiler_guard,
                },
                "validation_report": {
                    "enabled": _PDF_VALIDATION_ENABLED,
                    "status": "failed",
                    "accuracy_mode": True,
                    "invariants": [],
                    "stepwise_counts": [],
                    "anomalies": [],
                    "negative_samples": [],
                    "messages": ["Compiler guard blocked anti-pattern SQL in accuracy mode."],
                },
                "accuracy_report": self._build_accuracy_metrics(
                    canonical_spec=canonical_spec,
                    validation_report={"status": "failed"},
                ),
                "db_result": blocked_result,
                "next_action": {"type": "fix_compiler_anti_patterns", "violations": compiler_guard.get("violations", [])},
            }

        # 4. DB 실행 (Auto-Relaxation Loop applied)
        logger.info("3단계: SQL 실행 및 결과 집계 (Auto-Relaxation)")
        db_result = {
            "columns": [],
            "rows": [],
            "step_counts": [],
            "row_count": 0,
            "total_count": None,
            "error": None,
            "warning": sql_result.get("warning", []),
        }
        
        # 1st Attempt
        try:
            main_res = await asyncio.to_thread(
                execute_sql,
                sql_result["cohort_sql"],
                accuracy_mode=accuracy_on,
                query_tag="pdf_cohort_main",
            )
            
            # 0명인 경우 & Relax Mode가 아닌 경우에도, 시스템적으로 자동 완화 시도
            if (not main_res.get("rows")) and (len(main_res.get("rows", [])) == 0):
                logger.info("결과 0건 감지: Auto-Relaxation 시도")
                # 여기서 Intent를 다시 생성하는 것은 비효율적이므로, 
                # 추출된 intent 내에서 is_mandatory=False인 스텝을 제외한 SQL을 다시 compile하는 것이 이상적.
                # 하지만 현재 구조 제한상, 사용자에게 "조건을 완화해보세요" 경고를 주는 것으로 1차 대응.
                _append_warning_once(db_result, "검색된 환자가 0명입니다. '완화 모드'를 켜거나 일부 조건을 제외해 보세요.")
            
            if "error" in main_res:
                db_result["error"] = main_res["error"]
            else:
                db_result["columns"] = main_res.get("columns", [])
                db_result["rows"] = main_res.get("rows", [])[:100]
                db_result["row_count"] = int(main_res.get("row_count") or 0)
                db_result["total_count"] = main_res.get("total_count")

            # 단계별 카운트 조회
            debug_res = await asyncio.to_thread(
                execute_sql,
                sql_result["debug_count_sql"],
                accuracy_mode=accuracy_on,
                query_tag="pdf_cohort_debug_counts",
            )
            if "error" not in debug_res:
                debug_cols = [str(col or "").lower() for col in (debug_res.get("columns") or [])]
                step_rows: list[dict[str, Any]] = []
                for row in debug_res.get("rows", []) or []:
                    if isinstance(row, (list, tuple)):
                        step_rows.append({col: value for col, value in zip(debug_cols, row)})
                    elif isinstance(row, dict):
                        step_rows.append(row)
                db_result["step_counts"] = step_rows
        except Exception as e:
            logger.error(f"DB 실행 중 오류: {e}")
            logger.error(
                "cohort_sql preview (first 800 chars): %s",
                str(sql_result.get("cohort_sql") or "")[:800],
            )
            db_result["error"] = str(e)

        # === 4.5 AI RAG 고도화 (Automatic) ===
        logger.info("4.5단계: AI RAG 고도화 실행 조건 판단 (mode=%s)", _PDF_RAG_REFINEMENT_MODE)
        try:
            if not self._should_run_rag_refinement(db_result):
                logger.info(
                    "4.5단계 생략: 기본 SQL 결과 사용 (rows=%s, error=%s)",
                    int(db_result.get("row_count") or 0),
                    bool(db_result.get("error")),
                )
                raise _SkipRagRefinement()

            logger.info("4.5단계: AI RAG 쿼리 고도화 자동 실행")
            summary_ko = conditions.get("cohort_definition", {}).get("summary_ko", "")
            criteria_summary = conditions.get("cohort_definition", {}).get("criteria_summary_ko", "")
            
            # Load mapped variables from Step 1 (Prioritized for RAG context)
            mapped_vars = self._map_clinical_variables(conditions.get("cohort_definition", {}).get("variables", []))
            mapped_str = "\n".join([f"- {v['signal_name']}: {v.get('description', '')} (Mapped: {v.get('mapping', {}).get('target_table')} / {v.get('mapping', {}).get('itemid')})" for v in mapped_vars])

            # Load rich metadata for RAG context (Using detected mapped_vars)
            rag_hints = self._load_rag_metadata(mapped_vars)

            question = (
                f"**[ESSENTIAL SQL RULES]**\n"
                f"1. **ID Propagation (CRITICAL)**: In every CTE, SELECT ALL identifiers (`subject_id`, `hadm_id`, `stay_id`). Even if unused, carry them forward.\n"
                f"2. **Strict Join-Key Mapping**:\n"
                f"   - HOSPITAL Tables (ADMISSIONS, LAB, DIAGNOSIS): Use `hadm_id`.\n"
                f"   - ICU Tables (ICUSTAY, CHART): Use `stay_id`.\n"
                f"   - To bridge, ensure CTEs have both IDs.\n"
                f"3. **Research Guidelines (Apply ONLY if consistent with summary)**:\n"
                f"   - **First-Stay**: Apply `rn=1` filtering ONLY IF the text explicitly mentions 'first admission/stay'. Otherwise, allow all stays.\n"
                f"   - **Minimal Stay**: Apply `los >= 1` (24h) ONLY IF text mentions time criteria.\n"
                f"   - **Age Filter**: Apply `anchor_age` limits based on text. If vague, assume adult (>=18).\n"
                f"4. **Syntax Rules**:\n"
                f"   - `anchor_age` is in `PATIENTS` (p.anchor_age), `hospital_expire_flag` is in `ADMISSIONS`.\n"
                f"   - Use `NOT EXISTS` for exclusions.\n"
                f"   - **Diagnosis Codes**: ALWAYS use `LIKE '123%'` or `IN` for broader matching. Do not use strict `=` for ICD codes.\n"
                f"   - **Window Functions**: MUST use `OVER (PARTITION BY ... ORDER BY ...)`.\n"
                f"     - CORRECT: `ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY charttime ASC)`\n"
                f"     - WRONG: `ROW_NUMBER() OVER (ORDER BY charttime)` (Missing PARTITION BY in strict mode causes ORA-00924)\n\n"
                f"5. **Output Shape (CRITICAL)**:\n"
                f"   - Final SELECT must return patient-level rows including `subject_id`, `hadm_id`, `stay_id`.\n"
                f"   - Do NOT return aggregate-only metrics (COUNT/AVG/RATE only).\n\n"
                f"## REFERENCE KNOWLEDGE (METADATA):\n{rag_hints}\n\n"
                f"## DETECTED CLINICAL SIGNALS (FROM PDF):\n{mapped_str}\n\n"
                f"연구 요약: {summary_ko}\n"
                f"선정 및 제외 기준: {criteria_summary}\n\n"
                f"위 연구 디자인을 SQL 쿼리로 변환해줘. "
                f"MIMIC-IV 스키마를 사용하고, 단계별로 환자가 필터링되는 Funnel 형태의 CTE 구조를 만들어줘."
            )
            
            import asyncio
            loop = asyncio.get_running_loop()
            rag_payload = await loop.run_in_executor(
                None, 
                lambda: run_oneshot(question, translate=False, rag_multi=True, enable_clarification=False)
            )
            
            rag_final_sql = ""
            if "final" in rag_payload:
                rag_final_sql = rag_payload["final"].get("final_sql", "")
            elif "draft" in rag_payload:
                rag_final_sql = rag_payload["draft"].get("final_sql", "")
                
            if rag_final_sql:
                logger.info("RAG 고도화 SQL 생성 성공")
                candidate_sql = rag_final_sql
                candidate_count_sql = f"SELECT COUNT(*) FROM ({candidate_sql.replace('FETCH FIRST 100 ROWS ONLY', '')})"

                # DB 재실행 (고도화된 쿼리로)
                rag_db_res = await asyncio.to_thread(
                    execute_sql,
                    candidate_sql,
                    accuracy_mode=accuracy_on,
                    query_tag="pdf_rag_candidate",
                )
                
                # [Error Recovery Logic] If RAG SQL fails, try to auto-repair
                if "error" in rag_db_res:
                    logger.warning(f"RAG SQL Execution Error: {rag_db_res['error']}. Attempting auto-repair...")
                    fixed_sql = await self.fix_sql_with_error_async(candidate_sql, rag_db_res["error"])
                    if fixed_sql:
                        logger.info(f"Auto-Repaired SQL: {fixed_sql[:100]}...")
                        # 재실행
                        retry_res = await asyncio.to_thread(
                            execute_sql,
                            fixed_sql,
                            accuracy_mode=accuracy_on,
                            query_tag="pdf_rag_repair",
                        )
                        if "error" not in retry_res:
                            candidate_sql = fixed_sql
                            candidate_count_sql = f"SELECT COUNT(*) FROM ({fixed_sql.replace('FETCH FIRST 100 ROWS ONLY', '')})"
                            rag_db_res = retry_res
                        else:
                            logger.error(f"Repair Failed: {retry_res['error']}")
                            
                # [Zero-Result Relaxation Logic]
                # If result is 0 rows (and no error), user likely wants broader criteria.
                if "error" not in rag_db_res and "rows" in rag_db_res and len(rag_db_res["rows"]) == 0:
                     logger.info("RAG SQL returned 0 rows. Attempting relaxation (removing strict filters)...")
                     relax_prompt = (
                         f"The previous SQL executed successfully but returned 0 rows. This is too strict.\n"
                         f"Please RELAX the constraints:\n"
                         f"1. Remove `rn=1` (First-Stay) filter.\n"
                         f"2. Use broader ICD code matching (e.g. `LIKE '850%'` instead of specific codes).\n"
                         f"3. Remove non-essential lab value filters.\n"
                         f"4. Keep ID propagation and Join Keys correct.\n"
                         f"Rewrite the SQL to be more inclusive."
                     )
                     # Reuse repair function for relaxation as it handles SQL generation
                     relaxed_sql = await self.fix_sql_with_error_async(candidate_sql, relax_prompt)
                     if relaxed_sql:
                         logger.info("Executing Relaxed SQL...")
                         relaxed_res = await asyncio.to_thread(
                             execute_sql,
                             relaxed_sql,
                             accuracy_mode=accuracy_on,
                             query_tag="pdf_rag_relaxed",
                         )
                         if "error" not in relaxed_res and len(relaxed_res.get("rows", [])) > 0:
                             candidate_sql = relaxed_sql
                             candidate_count_sql = f"SELECT COUNT(*) FROM ({relaxed_sql.replace('FETCH FIRST 100 ROWS ONLY', '')})"
                             rag_db_res = relaxed_res
                         elif "rows" in relaxed_res and len(relaxed_res["rows"]) == 0:
                             logger.warning("Relaxed SQL also returned 0 rows.")


                if "error" in rag_db_res:
                    logger.warning("RAG SQL 실행 오류로 인해 기본 코호트 SQL 결과를 유지합니다.")
                    _append_warning_once(db_result, "RAG SQL 실행 오류로 기본 코호트 결과를 유지했습니다.")
                else:
                    rag_columns = _normalize_result_columns(rag_db_res.get("columns", []))

                    if not _has_identifier_columns(rag_columns):
                        logger.warning("RAG SQL이 집계형 결과를 반환했습니다. 환자 단위 출력으로 자동 재작성 시도.")
                        mapped_signal_names = sorted({
                            _normalize_signal_name(v.get("signal_name"))
                            for v in mapped_vars
                            if isinstance(v, dict) and str(v.get("signal_name") or "").strip()
                        })
                        mapped_signal_names = [name for name in mapped_signal_names if name]
                        rewrite_prompt = (
                            "The previous SQL returned aggregate-only metrics without patient identifiers.\n"
                            "Rewrite it to patient-level cohort output.\n"
                            "Requirements:\n"
                            "1. Include subject_id, hadm_id, stay_id in final SELECT.\n"
                            "2. Do not return COUNT/AVG-only aggregate output.\n"
                            "3. Include mapped clinical variable columns when possible.\n"
                            "4. Keep Oracle-compatible SQL and use FETCH FIRST 200 ROWS ONLY."
                        )
                        if mapped_signal_names:
                            rewrite_prompt += f"\nMapped clinical variables: {', '.join(mapped_signal_names)}"

                        row_level_sql = await self.fix_sql_with_error_async(candidate_sql, rewrite_prompt)
                        if row_level_sql:
                            row_level_res = await asyncio.to_thread(
                                execute_sql,
                                row_level_sql,
                                accuracy_mode=accuracy_on,
                                query_tag="pdf_rag_row_level_rewrite",
                            )
                            row_level_columns = _normalize_result_columns(row_level_res.get("columns", []))
                            if "error" not in row_level_res and _has_identifier_columns(row_level_columns):
                                logger.info("환자 단위 SQL 재작성 성공. 결과를 환자 행 기반으로 교체합니다.")
                                candidate_sql = row_level_sql
                                candidate_count_sql = f"SELECT COUNT(*) FROM ({row_level_sql.replace('FETCH FIRST 100 ROWS ONLY', '')})"
                                rag_db_res = row_level_res
                                rag_columns = row_level_columns

                        if not _has_identifier_columns(rag_columns):
                            logger.warning("환자 식별자 컬럼이 없는 집계형 SQL로 판단되어 기본 코호트 결과를 유지합니다.")
                            _append_warning_once(db_result, "집계형 SQL이 생성되어 환자 단위 기본 코호트 결과를 유지했습니다.")
                            rag_db_res = {}

                    if rag_db_res:
                        sql_result["cohort_sql"] = candidate_sql
                        sql_result["count_sql"] = candidate_count_sql
                        db_result["error"] = None
                        db_result["columns"] = rag_db_res.get("columns", [])
                        db_result["rows"] = rag_db_res.get("rows", [])[:100]
                        db_result["row_count"] = int(rag_db_res.get("row_count") or 0)
                        db_result["total_count"] = rag_db_res.get("total_count")
                        # step_counts는 파싱하기 어려울 수 있으므로 비움
                        db_result["step_counts"] = []
            else:
                logger.warning("RAG 고도화 SQL 생성 실패 (빈 결과)")
        except _SkipRagRefinement:
            pass
        except Exception as e:
            logger.error(f"RAG 고도화 중 오류 발생 (기존 템플릿 결과 유지): {e}")

        # 5. 임상 변수 리스트 매핑 강화
        extracted_vars = (conditions.get("cohort_definition") or {}).get("variables") or []
        mapped_variables = self._map_clinical_variables(extracted_vars)
        features = self._build_features(mapped_variables)
        validation_report = self._build_validation_report(
            cohort_sql=str((sql_result or {}).get("cohort_sql") or ""),
            db_result=db_result,
            step_counts=db_result.get("step_counts", []) if isinstance(db_result.get("step_counts"), list) else [],
            intent=intent_payload if isinstance(intent_payload, dict) else {},
            population_policy=population_policy,
            canonical_spec=canonical_spec,
            schema_map=schema_map,
            accuracy_mode=accuracy_on,
        )
        final_status = "completed"
        if validation_report.get("status") == "failed":
            final_status = "validation_failed"
        elif ambiguities:
            final_status = "completed_with_ambiguities"
        if accuracy_on and str(validation_report.get("status") or "").strip().lower() != "passed":
            final_status = "validation_failed"
        accuracy_report = self._build_accuracy_metrics(
            canonical_spec=canonical_spec,
            validation_report=validation_report,
        )
        validation_summary = summarize_validation(validation_report)
        validation_markdown = build_validation_markdown(validation_report, accuracy_mode=accuracy_on)
        
        # 6. 프론트엔드용 최종 스키마 조립 (메타데이터 포함)
        final_response = {
            "status": final_status,
            "pdf_hash": file_hash,
            "canonical_hash": canonical_hash,
            "filename": str(filename or "").strip() or "uploaded.pdf",
            "user_id": str(user_id or "").strip() or None,
            "relax_mode": relax_mode,
            "deterministic": deterministic,
            "accuracy_mode": accuracy_on,
            "pdf_page_count": page_count,
            "pdf_pages_scanned": pages_scanned,
            "pipeline_version": pipeline_version,
            "model": model_name,
            "prompt_hash": prompt_hash,
            "cohort_definition": conditions.get("cohort_definition", {}),
            "cohort_spec": canonical_spec,
            "ambiguity_resolution_required": bool(ambiguities),
            "ambiguities": ambiguities,
            "ambiguity_questions": ambiguity_questions,
            "snippets": snippets,
            "adaptive_extract": {
                "level": adaptive_level,
                "log": adaptive_logs,
                "risk": adaptive_extract.get("risk") if isinstance(adaptive_extract.get("risk"), dict) else {},
                "evidence_summary": evidence_summary,
            },
            "mapped_variables": mapped_variables, # 별도 필드로 추가
            "cohort_conditions": conditions.get("cohort_definition", {}).get("extraction_details", {}).get("cohort_criteria", {}).get("population", []),
            "features": features,
            "generated_sql": {
                "cohort_sql": sql_result.get("cohort_sql"),
                "count_sql": sql_result.get("count_sql"),
                "debug_count_sql": sql_result.get("debug_count_sql"),
                "intent": intent_payload if isinstance(intent_payload, dict) else {},
                "compiler_guard": sql_result.get("compiler_guard") if isinstance(sql_result.get("compiler_guard"), dict) else {},
            },
            "validation_report": validation_report,
            "validation_summary": validation_summary,
            "validation_markdown": validation_markdown,
            "accuracy_report": {
                **accuracy_report,
                "critic_notes": critic_notes,
            },
            "db_result": db_result
        }
        # cohort_definition 내부에도 변수 정보를 업데이트하여 프론트엔드의 cd.variables 접근 지원
        final_response["cohort_definition"]["variables"] = mapped_variables

        # 임시 저장 (에러가 없을 때만 캐싱하여 재시도 가능하게 함)
        if store:
            error_msg = db_result.get("error")
            if not error_msg:
                store.set(cache_key, final_response)
                logger.info(f"PDF 분석 결과 임시 캐시 저장 완료 (Key: {cache_key})")
            else:
                logger.warning(f"SQL 실행 에러로 인해 캐시 저장 건너뜀 (Key: {cache_key}): {error_msg}")
            
        return final_response

    async def fix_sql_with_error_async(self, failed_sql: str, error_message: str) -> str:
        """실행 실패한 SQL과 Oracle 에러 메시지를 GPT에게 보내 수정된 SQL을 반환."""
        schema_text = _load_schema_for_prompt()
        prompt = f"""아래 Oracle SQL이 실행 시 오류가 발생했습니다. 오류를 분석하고 수정된 SQL만 반환하세요.

## Oracle DB 스키마 (정확한 컬럼명)
{schema_text}

## 실패한 SQL
{failed_sql}

## Oracle 오류 메시지
{error_message}

## 수정 규칙
1. 위 스키마에 나열된 테이블명과 컬럼명만 정확히 사용하세요.
2. ICD_CODE는 CHAR 타입이므로 TRIM() 사용.
3. 테이블 별칭: DIAGNOSES_ICD->dx, D_ICD_DIAGNOSES->dd, ADMISSIONS->a, PATIENTS->p, ICUSTAYS->icu
4. 세미콜론(;), 백틱(`), 큰따옴표로 감싼 컬럼명 사용 금지.
5. 서브쿼리 내부에서만 사용한 컬럼을 외부에서 참조하지 마세요.
6. 결과 행 수를 FETCH FIRST 200 ROWS ONLY로 제한.

수정된 SQL만 JSON으로 반환하세요:
{{"fixed_sql": "수정된 SELECT 쿼리"}}
"""
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "당신은 Oracle SQL 디버깅 전문가입니다. 스키마에 맞게 SQL을 수정하세요. 반드시 유효한 JSON만 출력하세요."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0,
                seed=42
            )
            data = json.loads(response.choices[0].message.content)
            fixed = str(data.get("fixed_sql", "")).strip().rstrip(";").replace("`", "")
            fixed = re.sub(r'"([A-Za-z_]+)"', r'\1', fixed)
            logger.info("SQL 자동수정 완료: %s", fixed[:200])
            return fixed
        except Exception as e:
            logger.error("SQL 자동수정 실패: %s", e)
            return ""

    async def fix_sql_with_error(self, failed_sql: str, error_message: str) -> str:
        """비동기 방식으로 SQL 오류 수정 실행"""
        return await self.fix_sql_with_error_async(failed_sql, error_message)
