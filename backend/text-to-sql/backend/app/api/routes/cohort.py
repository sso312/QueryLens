from __future__ import annotations

import copy
from datetime import datetime
from functools import lru_cache
import hashlib
import logging
import math
import os
from pathlib import Path
import random
import re
import time
from typing import Any, Literal
import uuid
import json

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.core.config import get_settings
from app.core.paths import project_path
from app.services.agents.json_utils import extract_json_object
from app.services.agents.llm_client import LLMClient
from app.services.agents.orchestrator import run_oneshot
from app.services.oracle.executor import execute_sql
from app.services.runtime.diagnosis_map_store import load_diagnosis_icd_map, map_prefixes_for_terms
from app.services.runtime.request_context import get_request_user_id, use_request_user
from app.services.runtime.state_store import AppStateStore, get_state_store
from app.services.runtime.user_scope import scoped_state_key


router = APIRouter()
logger = logging.getLogger(__name__)

DEFAULT_PARAMS = {
    "readmit_days": 30,
    "age_threshold": 65,
    "los_threshold": 7,
    "gender": "all",
    "icu_only": False,
    "entry_filter": "all",
    "outcome_filter": "all",
}
_SAVED_COHORTS_KEY = "cohort::saved"
_FALLBACK_SAVED_COHORTS: dict[str, list[dict[str, Any]]] = {}
_SAVED_COHORT_LIBRARY_KEY = "cohort::library"
_FALLBACK_SAVED_COHORT_LIBRARY: dict[str, list[dict[str, Any]]] = {}
_SURVIVAL_TIME_POINTS = [0, 7, 14, 21, 30, 45, 60, 75, 90, 120, 150, 180]
_COHORT_COMORBIDITY_SPECS_PATH = project_path("var/metadata/cohort_comorbidity_specs.json")
_SIM_CACHE_MAXSIZE = 256
_SQL_WRITE_KEYWORDS = re.compile(r"\b(delete|update|insert|merge|drop|alter|truncate)\b", re.IGNORECASE)


class CohortParams(BaseModel):
    readmit_days: int = Field(30, ge=7, le=90)
    age_threshold: int = Field(65, ge=18, le=95)
    los_threshold: int = Field(7, ge=1, le=30)
    gender: str = Field("all", pattern="^(all|M|F)$")
    icu_only: bool = False
    entry_filter: str = Field("all", pattern="^(all|er|non_er)$")
    outcome_filter: str = Field("all", pattern="^(all|survived|expired)$")


class SimulationRequest(BaseModel):
    user: str | None = None
    params: CohortParams = Field(default_factory=CohortParams)
    include_baseline: bool = True


class CohortSqlRequest(BaseModel):
    user: str | None = None
    params: CohortParams = Field(default_factory=CohortParams)


class SaveCohortRequest(BaseModel):
    user: str | None = None
    name: str = Field(min_length=1, max_length=120)
    params: CohortParams = Field(default_factory=CohortParams)
    status: str = Field(default="active", pattern="^(active|archived)$")


class ConfirmPdfCohortRequest(BaseModel):
    user: str | None = None
    pdf_hash: str = Field(min_length=64, max_length=64)
    data: dict[str, Any]
    status: str = Field(default="confirmed", pattern="^(confirmed|draft)$")


class SmartSqlRequest(BaseModel):
    user: str | None = None
    summary: str
    criteria: str
    filename: str | None = None


class CohortSourcePayload(BaseModel):
    created_from: Literal["CROSS_SECTIONAL_PAGE", "PDF_ANALYSIS_PAGE", "IMPORT"] = "IMPORT"
    pdf_name: str | None = Field(default=None, max_length=260)
    pdf_analysis_id: str | None = Field(default=None, max_length=120)


class CohortPdfDetailInclusionExclusion(BaseModel):
    id: str = Field(min_length=1, max_length=120)
    title: str = Field(min_length=1, max_length=240)
    operational_definition: str = Field(min_length=1, max_length=2000)
    evidence: str | None = Field(default=None, max_length=2000)


class CohortPdfDetailVariable(BaseModel):
    key: str = Field(min_length=1, max_length=120)
    label: str = Field(min_length=1, max_length=240)
    table: str | None = Field(default=None, max_length=240)
    mapping_id: str | None = Field(default=None, max_length=120)


class CohortPdfDetailsPayload(BaseModel):
    paper_summary: str | None = Field(default=None, max_length=4000)
    inclusion_exclusion: list[CohortPdfDetailInclusionExclusion] = Field(default_factory=list)
    variables: list[CohortPdfDetailVariable] = Field(default_factory=list)


class CohortLibraryCreateRequest(BaseModel):
    user: str | None = None
    type: Literal["CROSS_SECTIONAL", "PDF_DERIVED"]
    name: str = Field(min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=1000)
    cohort_sql: str = Field(min_length=1, max_length=200000)
    count: int | None = Field(default=None, ge=0)
    sql_filter_summary: str | None = Field(default=None, max_length=4000)
    human_summary: str | None = Field(default=None, max_length=4000)
    source: CohortSourcePayload = Field(default_factory=CohortSourcePayload)
    pdf_details: CohortPdfDetailsPayload | None = None
    status: str = Field(default="active", pattern="^(active|archived)$")
    params: dict[str, Any] | None = None
    metrics: dict[str, Any] | None = None


class CohortLibraryPatchRequest(BaseModel):
    user: str | None = None
    name: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=1000)


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def _to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def _iso_now() -> str:
    return datetime.now().isoformat()


def _strip_terminal_semicolon(sql: str) -> str:
    return str(sql or "").strip().rstrip(";").strip()


def _validate_readonly_sql(sql: str) -> None:
    text = _strip_terminal_semicolon(sql)
    if not text:
        raise HTTPException(status_code=400, detail="cohort_sql is empty")
    if _SQL_WRITE_KEYWORDS.search(text):
        raise HTTPException(status_code=403, detail="Write operations are not allowed in cohort SQL")
    if not re.match(r"^\s*(select|with)\b", text, re.IGNORECASE):
        raise HTTPException(status_code=400, detail="Only SELECT/CTE queries are allowed")


def _extract_rows_columns(result: dict[str, Any]) -> tuple[list[Any], list[str]]:
    rows = result.get("rows") if isinstance(result, dict) else None
    columns = result.get("columns") if isinstance(result, dict) else None
    safe_rows = rows if isinstance(rows, list) else []
    safe_cols = [str(col or "").upper() for col in (columns or [])] if isinstance(columns, list) else []
    return safe_rows, safe_cols


def _best_effort_count_from_sql(cohort_sql: str, req_user: str | None) -> int | None:
    clean_sql = _strip_terminal_semicolon(cohort_sql)
    if not clean_sql:
        return None
    count_sql = f"SELECT COUNT(*) AS CNT FROM ({clean_sql}) cohort_src"
    with use_request_user(req_user):
        result = execute_sql(count_sql)
    rows, _ = _extract_rows_columns(result)
    first_row = rows[0] if rows and isinstance(rows[0], (list, tuple)) else None
    if not first_row:
        return None
    count_value = _to_int(first_row[0], default=-1)
    if count_value < 0:
        return None
    return count_value


def _normalize_cohort_source(value: Any) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    created_from = str(raw.get("created_from") or raw.get("createdFrom") or "IMPORT").strip().upper()
    if created_from not in {"CROSS_SECTIONAL_PAGE", "PDF_ANALYSIS_PAGE", "IMPORT"}:
        created_from = "IMPORT"
    pdf_name = str(raw.get("pdf_name") or raw.get("pdfName") or "").strip() or None
    pdf_analysis_id = str(raw.get("pdf_analysis_id") or raw.get("pdfAnalysisId") or "").strip() or None
    return {
        "created_from": created_from,
        "pdf_name": pdf_name,
        "pdf_analysis_id": pdf_analysis_id,
    }


def _normalize_pdf_details(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    paper_summary = str(value.get("paper_summary") or value.get("paperSummary") or "").strip() or None

    inclusion_exclusion: list[dict[str, Any]] = []
    raw_ie = value.get("inclusion_exclusion") or value.get("inclusionExclusion") or []
    if isinstance(raw_ie, list):
        for idx, item in enumerate(raw_ie):
            if not isinstance(item, dict):
                continue
            entry = {
                "id": str(item.get("id") or f"ie-{idx + 1}").strip(),
                "title": str(item.get("title") or f"조건 {idx + 1}").strip(),
                "operational_definition": str(
                    item.get("operational_definition") or item.get("operationalDefinition") or ""
                ).strip(),
                "evidence": str(item.get("evidence") or "").strip() or None,
            }
            if not entry["operational_definition"]:
                continue
            inclusion_exclusion.append(entry)

    variables: list[dict[str, Any]] = []
    raw_vars = value.get("variables") or []
    if isinstance(raw_vars, list):
        for item in raw_vars:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key") or "").strip()
            label = str(item.get("label") or "").strip()
            if not key and not label:
                continue
            variables.append({
                "key": key or label,
                "label": label or key,
                "table": str(item.get("table") or "").strip() or None,
                "mapping_id": str(item.get("mapping_id") or item.get("mappingId") or "").strip() or None,
            })

    if not paper_summary and not inclusion_exclusion and not variables:
        return None
    return {
        "paper_summary": paper_summary,
        "inclusion_exclusion": inclusion_exclusion,
        "variables": variables,
    }


def _cross_sectional_filter_summary(params: CohortParams) -> str:
    parts = [
        f"ANCHOR_AGE >= {int(params.age_threshold)}",
        f"LOS >= {int(params.los_threshold)}",
        f"READMIT_WINDOW = {int(params.readmit_days)} days",
    ]
    gender = str(params.gender or "all").upper()
    if gender in {"M", "F"}:
        parts.append(f"GENDER = '{gender}'")
    if params.icu_only:
        parts.append("ICU_ONLY = TRUE")
    entry_filter = str(params.entry_filter or "all").lower()
    if entry_filter == "er":
        parts.append("ENTRY = ER")
    elif entry_filter == "non_er":
        parts.append("ENTRY = NON_ER")
    outcome_filter = str(params.outcome_filter or "all").lower()
    if outcome_filter == "survived":
        parts.append("OUTCOME = SURVIVED")
    elif outcome_filter == "expired":
        parts.append("OUTCOME = EXPIRED")
    return " AND ".join(parts)


def _cross_sectional_human_summary(params: CohortParams) -> str:
    gender_text = "전체" if params.gender == "all" else str(params.gender).upper()
    icu_text = "ICU 포함" if params.icu_only else "전체 입원"
    return (
        f"{int(params.age_threshold)}세 이상 · LOS {int(params.los_threshold)}일 이상 · "
        f"재입원 {int(params.readmit_days)}일 기준 · 성별 {gender_text} · {icu_text}"
    )


def _cross_sectional_cohort_sql(params: CohortParams) -> str:
    sql_bundle = _cohort_sql_bundle(params)
    cte = str(sql_bundle.get("cohort_cte") or "").strip()
    if not cte:
        return ""
    return f"{cte}SELECT DISTINCT c.SUBJECT_ID, c.HADM_ID, CAST(NULL AS NUMBER) AS STAY_ID FROM cohort c"


def _saved_cohort_library_key(user: str | None) -> str:
    return scoped_state_key(_SAVED_COHORT_LIBRARY_KEY, user)


def _normalize_saved_cohort_item(raw_item: Any) -> dict[str, Any] | None:
    if not isinstance(raw_item, dict):
        return None
    item_id = str(raw_item.get("id") or "").strip()
    item_type = str(raw_item.get("type") or "").strip().upper()
    item_name = str(raw_item.get("name") or "").strip()
    if not item_id or item_type not in {"CROSS_SECTIONAL", "PDF_DERIVED"} or not item_name:
        return None
    created_at = str(raw_item.get("created_at") or raw_item.get("createdAt") or "").strip() or _iso_now()
    updated_at = str(raw_item.get("updated_at") or raw_item.get("updatedAt") or "").strip() or created_at
    cohort_sql = _strip_terminal_semicolon(
        str(raw_item.get("cohort_sql") or raw_item.get("cohortSql") or "")
    )
    if not cohort_sql:
        return None
    count_raw = raw_item.get("count")
    count_value = _to_int(count_raw, default=-1)
    status_value = str(raw_item.get("status") or "active").strip().lower()
    if status_value not in {"active", "archived"}:
        status_value = "active"
    return {
        "id": item_id,
        "type": item_type,
        "name": item_name,
        "description": str(raw_item.get("description") or "").strip() or None,
        "cohort_sql": cohort_sql,
        "count": count_value if count_value >= 0 else None,
        "sql_filter_summary": str(
            raw_item.get("sql_filter_summary") or raw_item.get("sqlFilterSummary") or ""
        ).strip() or None,
        "human_summary": str(raw_item.get("human_summary") or raw_item.get("humanSummary") or "").strip() or None,
        "source": _normalize_cohort_source(raw_item.get("source")),
        "pdf_details": _normalize_pdf_details(raw_item.get("pdf_details") or raw_item.get("pdfDetails")),
        "params": raw_item.get("params") if isinstance(raw_item.get("params"), dict) else None,
        "metrics": raw_item.get("metrics") if isinstance(raw_item.get("metrics"), dict) else None,
        "status": status_value,
        "created_at": created_at,
        "updated_at": updated_at,
    }


def _cohort_library_from_legacy_saved(user: str | None = None) -> list[dict[str, Any]]:
    legacy_items = _get_saved_cohorts(user)
    mapped: list[dict[str, Any]] = []
    for item in legacy_items:
        if not isinstance(item, dict):
            continue
        params_raw = item.get("params")
        if not isinstance(params_raw, dict):
            continue
        try:
            params = CohortParams(**params_raw)
        except Exception:
            continue
        cohort_sql = _cross_sectional_cohort_sql(params)
        if not cohort_sql:
            continue
        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
        patient_count = _to_int(metrics.get("patient_count"), default=-1) if isinstance(metrics, dict) else -1
        created_at = str(item.get("created_at") or "").strip() or _iso_now()
        normalized = _normalize_saved_cohort_item(
            {
                "id": str(item.get("id") or str(uuid.uuid4())),
                "type": "CROSS_SECTIONAL",
                "name": str(item.get("name") or "저장 코호트").strip(),
                "cohort_sql": cohort_sql,
                "count": patient_count if patient_count >= 0 else None,
                "sql_filter_summary": _cross_sectional_filter_summary(params),
                "human_summary": _cross_sectional_human_summary(params),
                "source": {"created_from": "CROSS_SECTIONAL_PAGE"},
                "params": params.model_dump(),
                "metrics": metrics if isinstance(metrics, dict) else None,
                "status": str(item.get("status") or "active"),
                "created_at": created_at,
                "updated_at": created_at,
            }
        )
        if normalized:
            mapped.append(normalized)
    return mapped


def _get_cohort_library(user: str | None = None) -> list[dict[str, Any]]:
    key = _saved_cohort_library_key(user)
    store = get_state_store()
    if not store.enabled:
        items = _FALLBACK_SAVED_COHORT_LIBRARY.get(key, [])
    else:
        payload = store.get(key) or {}
        items = payload.get("items", []) if isinstance(payload, dict) else []
    normalized_items: list[dict[str, Any]] = []
    for item in items if isinstance(items, list) else []:
        normalized = _normalize_saved_cohort_item(item)
        if normalized:
            normalized_items.append(normalized)
    if not normalized_items:
        return _cohort_library_from_legacy_saved(user)
    return normalized_items


def _set_cohort_library(items: list[dict[str, Any]], user: str | None = None) -> None:
    normalized_items = [item for item in (_normalize_saved_cohort_item(raw) for raw in items) if item]
    key = _saved_cohort_library_key(user)
    store = get_state_store()
    if not store.enabled:
        _FALLBACK_SAVED_COHORT_LIBRARY[key] = list(normalized_items)
        return
    ok = store.set(key, {"items": normalized_items})
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to persist cohort library")


def _cohort_sample_rows() -> int:
    # 0 means full population (no sampling). Use env override if you need faster approximate mode.
    raw = (os.getenv("COHORT_SAMPLE_ROWS") or "0").strip()
    try:
        value = int(raw)
    except Exception:
        return 0
    return max(0, value)


def _simulation_cache_ttl_sec() -> int:
    raw = (os.getenv("COHORT_SIM_CACHE_TTL_SEC") or "180").strip()
    try:
        value = int(raw)
    except Exception:
        return 180
    return max(0, value)


def _simulation_cache_bucket() -> int:
    ttl = _simulation_cache_ttl_sec()
    if ttl <= 0:
        # disable cache by forcing a unique bucket for each call
        return int(time.time_ns())
    return int(time.time() // ttl)


def _params_payload(params: CohortParams) -> str:
    return json.dumps(params.model_dump(), sort_keys=True, separators=(",", ":"))


def _er_admission_condition(alias: str) -> str:
    # Avoid overly broad substring matching (e.g., OTHER containing "ER").
    loc = f"UPPER(NVL({alias}.ADMISSION_LOCATION, ''))"
    return f"({loc} LIKE '%EMERGENCY%' OR REGEXP_LIKE({loc}, '(^|[^A-Z])(ER|ED)([^A-Z]|$)'))"


def _load_comorbidity_specs() -> list[dict[str, Any]]:
    if not _COHORT_COMORBIDITY_SPECS_PATH.exists():
        return []
    try:
        payload = json.loads(_COHORT_COMORBIDITY_SPECS_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []

    specs: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        group_key = str(item.get("group_key") or "").strip()
        group_label = str(item.get("group_label") or "").strip()
        flag_col = str(item.get("flag_col") or "").strip()
        if not group_key or not group_label or not flag_col:
            continue
        map_terms_raw = item.get("map_terms") or []
        map_terms = [str(term).strip() for term in map_terms_raw if str(term).strip()] if isinstance(map_terms_raw, list) else []
        fallback_raw = item.get("fallback_prefixes") or []
        fallback_prefixes = [str(prefix).strip().upper() for prefix in fallback_raw if str(prefix).strip()] if isinstance(fallback_raw, list) else []
        try:
            sort_order = int(item.get("sort_order") or len(specs) + 1)
        except Exception:
            sort_order = len(specs) + 1
        specs.append({
            "group_key": group_key,
            "group_label": group_label,
            "flag_col": flag_col,
            "sort_order": sort_order,
            "map_terms": map_terms,
            "fallback_prefixes": fallback_prefixes,
        })

    return specs


def _icd_prefix_condition(dx_code_expr: str, prefixes: list[str]) -> str:
    parts = [f"{dx_code_expr} LIKE '{prefix}%'" for prefix in prefixes if prefix]
    if not parts:
        return "1 = 0"
    return "(" + " OR ".join(parts) + ")"


def _comorbidity_specs_from_mapping(dx_code_expr: str) -> list[dict[str, Any]]:
    base_specs = _load_comorbidity_specs()
    if not base_specs:
        return []
    diagnosis_map = load_diagnosis_icd_map()
    specs: list[dict[str, Any]] = []
    for base in base_specs:
        mapped_prefixes = map_prefixes_for_terms(diagnosis_map, list(base.get("map_terms", [])))
        prefixes = mapped_prefixes or [str(item).strip().upper() for item in base.get("fallback_prefixes", []) if str(item).strip()]
        if not prefixes:
            continue
        specs.append({
            "group_key": base["group_key"],
            "group_label": base["group_label"],
            "flag_col": base["flag_col"],
            "sort_order": int(base["sort_order"]),
            "condition_sql": _icd_prefix_condition(dx_code_expr, prefixes),
        })
    return specs


def _cohort_cte(params: CohortParams) -> str:
    age = int(params.age_threshold)
    los = int(params.los_threshold)
    gender = (params.gender or "all").upper()
    entry_filter = (params.entry_filter or "all").lower()
    outcome_filter = (params.outcome_filter or "all").lower()
    sample_rows = _cohort_sample_rows()
    gender_clause = f"AND UPPER(TRIM(p.GENDER)) = '{gender}' " if gender in {"M", "F"} else ""
    icu_clause = "AND EXISTS (SELECT 1 FROM ICUSTAYS i WHERE i.HADM_ID = a.HADM_ID) " if params.icu_only else ""
    er_condition = _er_admission_condition("a")
    if entry_filter == "er":
        entry_clause = f"AND {er_condition} "
    elif entry_filter == "non_er":
        entry_clause = f"AND NOT {er_condition} "
    else:
        entry_clause = ""
    if outcome_filter == "expired":
        outcome_clause = "AND a.HOSPITAL_EXPIRE_FLAG = 1 "
    elif outcome_filter == "survived":
        outcome_clause = "AND NVL(a.HOSPITAL_EXPIRE_FLAG, 0) = 0 "
    else:
        outcome_clause = ""
    sample_rn_col = (
        ", ROW_NUMBER() OVER (ORDER BY a.ADMITTIME DESC, a.HADM_ID DESC) AS SAMPLE_RN "
        if sample_rows > 0
        else " "
    )
    sample_filter_clause = f"WHERE r.SAMPLE_RN <= {sample_rows} " if sample_rows > 0 else ""
    return (
        "WITH admissions_ranked AS ( "
        "SELECT a.HADM_ID, a.SUBJECT_ID, a.ADMITTIME, a.DISCHTIME, a.HOSPITAL_EXPIRE_FLAG, a.ADMISSION_LOCATION, "
        "LEAD(a.ADMITTIME) OVER (PARTITION BY a.SUBJECT_ID ORDER BY a.ADMITTIME, a.HADM_ID) AS NEXT_ADMITTIME "
        f"{sample_rn_col}"
        "FROM ADMISSIONS a "
        "WHERE a.ADMITTIME IS NOT NULL "
        "AND a.DISCHTIME IS NOT NULL "
        "), admissions_sample AS ( "
        "SELECT r.HADM_ID, r.SUBJECT_ID, r.ADMITTIME, r.DISCHTIME, r.HOSPITAL_EXPIRE_FLAG, r.ADMISSION_LOCATION, r.NEXT_ADMITTIME "
        "FROM admissions_ranked r "
        f"{sample_filter_clause}"
        "), cohort AS ( "
        "SELECT a.HADM_ID, a.SUBJECT_ID, a.ADMITTIME, a.DISCHTIME, a.HOSPITAL_EXPIRE_FLAG, a.ADMISSION_LOCATION, "
        "a.NEXT_ADMITTIME, UPPER(TRIM(p.GENDER)) AS GENDER, p.ANCHOR_AGE "
        "FROM admissions_sample a "
        "JOIN PATIENTS p ON p.SUBJECT_ID = a.SUBJECT_ID "
        "WHERE p.ANCHOR_AGE IS NOT NULL "
        f"AND p.ANCHOR_AGE >= {age} "
        f"AND (CAST(a.DISCHTIME AS DATE) - CAST(a.ADMITTIME AS DATE)) >= {los} "
        f"{gender_clause}"
        f"{icu_clause}"
        f"{entry_clause}"
        f"{outcome_clause}"
        ") "
    )


def _cohort_sql_bundle(params: CohortParams) -> dict[str, str]:
    cte = _cohort_cte(params)
    readmit_days = int(params.readmit_days)
    los_expr = "(CAST(c.DISCHTIME AS DATE) - CAST(c.ADMITTIME AS DATE))"
    readmit_30_case = (
        "CASE WHEN c.NEXT_ADMITTIME IS NOT NULL "
        "AND c.NEXT_ADMITTIME > c.DISCHTIME "
        f"AND c.NEXT_ADMITTIME <= c.DISCHTIME + {readmit_days} "
        "THEN 1 ELSE 0 END"
    )
    readmit_7_case = (
        "CASE WHEN c.NEXT_ADMITTIME IS NOT NULL "
        "AND c.NEXT_ADMITTIME > c.DISCHTIME "
        "AND c.NEXT_ADMITTIME <= c.DISCHTIME + 7 "
        "THEN 1 ELSE 0 END"
    )
    death_case = "CASE WHEN c.HOSPITAL_EXPIRE_FLAG = 1 THEN 1 ELSE 0 END"
    long_stay_case = f"CASE WHEN {los_expr} >= 14 THEN 1 ELSE 0 END"
    icu_case = "CASE WHEN icu.HADM_ID IS NOT NULL THEN 1 ELSE 0 END"
    er_case = f"CASE WHEN {_er_admission_condition('c')} THEN 1 ELSE 0 END"
    age_band_key = (
        "CASE "
        "WHEN c.ANCHOR_AGE < 40 THEN '18_39' "
        "WHEN c.ANCHOR_AGE < 50 THEN '40_49' "
        "WHEN c.ANCHOR_AGE < 60 THEN '50_59' "
        "WHEN c.ANCHOR_AGE < 70 THEN '60_69' "
        "WHEN c.ANCHOR_AGE < 80 THEN '70_79' "
        "ELSE '80_PLUS' END"
    )
    age_band_label = (
        "CASE "
        "WHEN c.ANCHOR_AGE < 40 THEN '18-39세' "
        "WHEN c.ANCHOR_AGE < 50 THEN '40-49세' "
        "WHEN c.ANCHOR_AGE < 60 THEN '50-59세' "
        "WHEN c.ANCHOR_AGE < 70 THEN '60-69세' "
        "WHEN c.ANCHOR_AGE < 80 THEN '70-79세' "
        "ELSE '80세 이상' END"
    )
    gender_key = (
        "CASE "
        "WHEN c.GENDER = 'M' THEN 'M' "
        "WHEN c.GENDER = 'F' THEN 'F' "
        "ELSE 'UNKNOWN' END"
    )
    gender_label = (
        "CASE "
        "WHEN c.GENDER = 'M' THEN '남성' "
        "WHEN c.GENDER = 'F' THEN '여성' "
        "ELSE '미상' END"
    )
    subgroup_metric_cols = (
        "COUNT(*) AS ADMISSION_CNT, "
        "COUNT(DISTINCT c.SUBJECT_ID) AS PATIENT_CNT, "
        f"ROUND(100 * AVG({readmit_30_case}), 2) AS READMIT_RATE_PCT, "
        f"ROUND(100 * AVG({death_case}), 2) AS MORTALITY_RATE_PCT, "
        f"ROUND(AVG({los_expr}), 2) AS AVG_LOS_DAYS "
    )
    age_sort_ord = (
        "CASE "
        "WHEN c.ANCHOR_AGE < 40 THEN 1 "
        "WHEN c.ANCHOR_AGE < 50 THEN 2 "
        "WHEN c.ANCHOR_AGE < 60 THEN 3 "
        "WHEN c.ANCHOR_AGE < 70 THEN 4 "
        "WHEN c.ANCHOR_AGE < 80 THEN 5 "
        "ELSE 6 END"
    )
    gender_sort_ord = (
        "CASE "
        "WHEN c.GENDER = 'M' THEN 1 "
        "WHEN c.GENDER = 'F' THEN 2 "
        "ELSE 3 END"
    )
    age_subgroup_sql = (
        cte
        + "SELECT "
        f"{age_band_key} AS GROUP_KEY, "
        f"{age_band_label} AS GROUP_LABEL, "
        f"{subgroup_metric_cols}"
        "FROM cohort c "
        f"GROUP BY {age_band_key}, {age_band_label} "
        "ORDER BY GROUP_KEY"
    )
    gender_subgroup_sql = (
        cte
        + "SELECT "
        f"{gender_key} AS GROUP_KEY, "
        f"{gender_label} AS GROUP_LABEL, "
        f"{subgroup_metric_cols}"
        "FROM cohort c "
        f"GROUP BY {gender_key}, {gender_label} "
        "ORDER BY CASE "
        "WHEN GROUP_KEY = 'M' THEN 1 "
        "WHEN GROUP_KEY = 'F' THEN 2 "
        "ELSE 3 END"
    )
    dx_code_expr = "UPPER(REPLACE(NVL(d.ICD_CODE, ''), '.', ''))"
    comorbidity_specs = _comorbidity_specs_from_mapping(dx_code_expr)
    dx_flags_cte = ""
    combined_subgroup_parts = [
        (
            "SELECT 'age' AS SECTION, 1 AS SECTION_ORD, "
            f"{age_band_key} AS GROUP_KEY, "
            f"{age_band_label} AS GROUP_LABEL, "
            f"{subgroup_metric_cols}"
            f"{age_sort_ord} AS SORT_ORD "
            "FROM cohort c "
            f"GROUP BY {age_band_key}, {age_band_label}, {age_sort_ord}"
        ),
        (
            "SELECT 'gender' AS SECTION, 2 AS SECTION_ORD, "
            f"{gender_key} AS GROUP_KEY, "
            f"{gender_label} AS GROUP_LABEL, "
            f"{subgroup_metric_cols}"
            f"{gender_sort_ord} AS SORT_ORD "
            "FROM cohort c "
            f"GROUP BY {gender_key}, {gender_label}, {gender_sort_ord}"
        ),
    ]
    if comorbidity_specs:
        flag_columns = ", ".join(
            f"MAX(CASE WHEN {spec['condition_sql']} THEN 1 ELSE 0 END) AS {spec['flag_col']}"
            for spec in comorbidity_specs
        )
        dx_flags_cte = (
            ", dx_flags AS ( "
            "SELECT d.HADM_ID, "
            f"{flag_columns} "
            "FROM DIAGNOSES_ICD d "
            "JOIN (SELECT DISTINCT HADM_ID FROM cohort) ch ON ch.HADM_ID = d.HADM_ID "
            "GROUP BY d.HADM_ID "
            ") "
        )

        def comorb_select(group_key: str, group_label: str, flag_col: str, sort_order: int) -> str:
            return (
                "SELECT "
                f"'{group_key}' AS GROUP_KEY, "
                f"'{group_label}' AS GROUP_LABEL, "
                "COUNT(*) AS ADMISSION_CNT, "
                "COUNT(DISTINCT c.SUBJECT_ID) AS PATIENT_CNT, "
                f"ROUND(100 * AVG({readmit_30_case}), 2) AS READMIT_RATE_PCT, "
                f"ROUND(100 * AVG({death_case}), 2) AS MORTALITY_RATE_PCT, "
                f"ROUND(AVG({los_expr}), 2) AS AVG_LOS_DAYS, "
                f"{sort_order} AS SORT_ORD "
                "FROM cohort c "
                "JOIN dx_flags f ON f.HADM_ID = c.HADM_ID "
                f"WHERE f.{flag_col} = 1"
            )

        def comorb_select_combined(group_key: str, group_label: str, flag_col: str, sort_order: int) -> str:
            return (
                "SELECT "
                "'comorbidity' AS SECTION, 3 AS SECTION_ORD, "
                f"'{group_key}' AS GROUP_KEY, "
                f"'{group_label}' AS GROUP_LABEL, "
                "COUNT(*) AS ADMISSION_CNT, "
                "COUNT(DISTINCT c.SUBJECT_ID) AS PATIENT_CNT, "
                f"ROUND(100 * AVG({readmit_30_case}), 2) AS READMIT_RATE_PCT, "
                f"ROUND(100 * AVG({death_case}), 2) AS MORTALITY_RATE_PCT, "
                f"ROUND(AVG({los_expr}), 2) AS AVG_LOS_DAYS, "
                f"{sort_order} AS SORT_ORD "
                "FROM cohort c "
                "JOIN dx_flags f ON f.HADM_ID = c.HADM_ID "
                f"WHERE f.{flag_col} = 1"
            )

        comorbidity_union_sql = " UNION ALL ".join(
            comorb_select(
                str(spec["group_key"]),
                str(spec["group_label"]),
                str(spec["flag_col"]),
                int(spec["sort_order"]),
            )
            for spec in comorbidity_specs
        )
        comorbidity_subgroup_sql = (
            cte
            + dx_flags_cte
            + "SELECT GROUP_KEY, GROUP_LABEL, ADMISSION_CNT, PATIENT_CNT, READMIT_RATE_PCT, MORTALITY_RATE_PCT, AVG_LOS_DAYS "
            "FROM ("
            + comorbidity_union_sql
            + ") "
            "ORDER BY SORT_ORD"
        )
        combined_subgroup_parts.extend(
            comorb_select_combined(
                str(spec["group_key"]),
                str(spec["group_label"]),
                str(spec["flag_col"]),
                int(spec["sort_order"]),
            )
            for spec in comorbidity_specs
        )
    else:
        comorbidity_subgroup_sql = (
            cte
            + "SELECT "
            "CAST(NULL AS VARCHAR2(64)) AS GROUP_KEY, "
            "CAST(NULL AS VARCHAR2(128)) AS GROUP_LABEL, "
            "CAST(NULL AS NUMBER) AS ADMISSION_CNT, "
            "CAST(NULL AS NUMBER) AS PATIENT_CNT, "
            "CAST(NULL AS NUMBER) AS READMIT_RATE_PCT, "
            "CAST(NULL AS NUMBER) AS MORTALITY_RATE_PCT, "
            "CAST(NULL AS NUMBER) AS AVG_LOS_DAYS "
            "FROM cohort c WHERE 1 = 0"
        )
    all_subgroups_sql = (
        cte
        + dx_flags_cte
        + "SELECT SECTION, GROUP_KEY, GROUP_LABEL, ADMISSION_CNT, PATIENT_CNT, READMIT_RATE_PCT, MORTALITY_RATE_PCT, AVG_LOS_DAYS "
        "FROM ("
        + " UNION ALL ".join(combined_subgroup_parts)
        + ") "
        "ORDER BY SECTION_ORD, SORT_ORD, GROUP_KEY"
    )
    metrics_sql = (
        cte
        + "SELECT "
        "COUNT(DISTINCT c.SUBJECT_ID) AS PATIENT_CNT, "
        f"ROUND(100 * AVG({readmit_30_case}), 2) AS READMIT_RATE_PCT, "
        f"ROUND(100 * AVG({death_case}), 2) AS MORTALITY_RATE_PCT, "
        f"ROUND(AVG({los_expr}), 2) AS AVG_LOS_DAYS, "
        f"ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {los_expr}), 2) AS MEDIAN_LOS_DAYS, "
        f"ROUND(100 * AVG({readmit_7_case}), 2) AS READMIT_7D_RATE_PCT, "
        f"ROUND(100 * AVG({long_stay_case}), 2) AS LONG_STAY_RATE_PCT, "
        f"ROUND(100 * AVG({icu_case}), 2) AS ICU_ADMISSION_RATE_PCT, "
        f"ROUND(100 * AVG({er_case}), 2) AS ER_ADMISSION_RATE_PCT, "
        "COUNT(*) AS ADMISSION_CNT, "
        f"SUM({readmit_30_case}) AS READMIT_30_CNT, "
        f"SUM({death_case}) AS DEATH_CNT, "
        f"ROUND(NVL(STDDEV({los_expr}), 0), 6) AS LOS_STDDEV_DAYS, "
        f"SUM({icu_case}) AS ICU_ADMISSION_CNT, "
        f"SUM({er_case}) AS ER_ADMISSION_CNT, "
        f"SUM({readmit_7_case}) AS READMIT_7_CNT, "
        f"SUM({long_stay_case}) AS LONG_STAY_CNT "
        "FROM cohort c "
        "LEFT JOIN (SELECT DISTINCT HADM_ID FROM ICUSTAYS) icu ON icu.HADM_ID = c.HADM_ID"
    )
    return {
        "cohort_cte": cte,
        "metrics_sql": metrics_sql,
        "age_subgroup_sql": age_subgroup_sql,
        "gender_subgroup_sql": gender_subgroup_sql,
        "comorbidity_subgroup_sql": comorbidity_subgroup_sql,
        "all_subgroups_sql": all_subgroups_sql,
        "patient_count_sql": cte + "SELECT COUNT(DISTINCT c.SUBJECT_ID) AS PATIENT_CNT FROM cohort c",
        "readmission_rate_sql": (
            cte
            + "SELECT ROUND(100 * AVG(CASE "
            "WHEN c.NEXT_ADMITTIME IS NOT NULL "
            "AND c.NEXT_ADMITTIME > c.DISCHTIME "
            f"AND c.NEXT_ADMITTIME <= c.DISCHTIME + {readmit_days} "
            "THEN 1 ELSE 0 END), 2) AS READMIT_RATE_PCT "
            "FROM cohort c"
        ),
        "mortality_rate_sql": (
            cte
            + "SELECT ROUND(100 * AVG(CASE WHEN c.HOSPITAL_EXPIRE_FLAG = 1 THEN 1 ELSE 0 END), 2) "
            "AS MORTALITY_RATE_PCT FROM cohort c"
        ),
        "avg_los_sql": (
            cte
            + "SELECT ROUND(AVG(CAST(c.DISCHTIME AS DATE) - CAST(c.ADMITTIME AS DATE)), 2) AS AVG_LOS_DAYS "
            "FROM cohort c"
        ),
        "median_los_sql": (
            cte
            + "SELECT ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP "
            "(ORDER BY (CAST(c.DISCHTIME AS DATE) - CAST(c.ADMITTIME AS DATE))), 2) AS MEDIAN_LOS_DAYS "
            "FROM cohort c"
        ),
        "readmission_7d_rate_sql": (
            cte
            + "SELECT ROUND(100 * AVG(CASE "
            "WHEN c.NEXT_ADMITTIME IS NOT NULL "
            "AND c.NEXT_ADMITTIME > c.DISCHTIME "
            "AND c.NEXT_ADMITTIME <= c.DISCHTIME + 7 "
            "THEN 1 ELSE 0 END), 2) AS READMIT_7D_RATE_PCT "
            "FROM cohort c"
        ),
        "long_stay_rate_sql": (
            cte
            + "SELECT ROUND(100 * AVG(CASE "
            "WHEN (CAST(c.DISCHTIME AS DATE) - CAST(c.ADMITTIME AS DATE)) >= 14 THEN 1 "
            "ELSE 0 END), 2) AS LONG_STAY_RATE_PCT "
            "FROM cohort c"
        ),
        "icu_admission_rate_sql": (
            cte
            + "SELECT ROUND(100 * AVG(CASE "
            "WHEN EXISTS (SELECT 1 FROM ICUSTAYS i WHERE i.HADM_ID = c.HADM_ID) THEN 1 "
            "ELSE 0 END), 2) AS ICU_ADMISSION_RATE_PCT "
            "FROM cohort c"
        ),
        "er_admission_rate_sql": (
            cte
            + "SELECT ROUND(100 * AVG(CASE "
            f"WHEN {_er_admission_condition('c')} "
            "THEN 1 ELSE 0 END), 2) AS ER_ADMISSION_RATE_PCT "
            "FROM cohort c"
        ),
        "life_table_sql": (
            cte
            + "SELECT "
            "FLOOR(CAST(c.DISCHTIME AS DATE) - CAST(c.ADMITTIME AS DATE)) AS LOS_DAY, "
            "SUM(CASE WHEN c.HOSPITAL_EXPIRE_FLAG = 1 THEN 1 ELSE 0 END) AS EVENT_CNT, "
            "SUM(CASE WHEN c.HOSPITAL_EXPIRE_FLAG = 1 THEN 0 ELSE 1 END) AS CENSOR_CNT "
            "FROM cohort c "
            "WHERE c.ADMITTIME IS NOT NULL AND c.DISCHTIME IS NOT NULL "
            "AND (CAST(c.DISCHTIME AS DATE) - CAST(c.ADMITTIME AS DATE)) >= 0 "
            "GROUP BY FLOOR(CAST(c.DISCHTIME AS DATE) - CAST(c.ADMITTIME AS DATE)) "
            "ORDER BY LOS_DAY"
        ),
}


def _simulate_metrics_and_stats(params: CohortParams) -> tuple[dict[str, float], dict[str, float]]:
    sqls = _cohort_sql_bundle(params)
    result = execute_sql(sqls["metrics_sql"])
    rows = result.get("rows") or []
    first_row = rows[0] if rows and isinstance(rows[0], (list, tuple)) else []
    if not first_row:
        metrics = {
            "patient_count": 0.0,
            "readmission_rate": 0.0,
            "mortality_rate": 0.0,
            "avg_los_days": 0.0,
            "median_los_days": 0.0,
            "readmission_7d_rate": 0.0,
            "long_stay_rate": 0.0,
            "icu_admission_rate": 0.0,
            "er_admission_rate": 0.0,
        }
        stats = {
            "n_admissions": 0.0,
            "readmit_30_cnt": 0.0,
            "death_cnt": 0.0,
            "los_stddev_days": 0.0,
            "icu_admission_cnt": 0.0,
            "er_admission_cnt": 0.0,
            "readmit_7_cnt": 0.0,
            "long_stay_cnt": 0.0,
        }
        return metrics, stats

    patient_count = max(0.0, _to_float(first_row[0], default=0.0))
    readmission_rate = max(0.0, min(100.0, _to_float(first_row[1], default=0.0)))
    mortality_rate = max(0.0, min(100.0, _to_float(first_row[2], default=0.0)))
    avg_los_days = max(0.0, _to_float(first_row[3], default=0.0))
    median_los_days = max(0.0, _to_float(first_row[4], default=0.0))
    readmission_7d_rate = max(0.0, min(100.0, _to_float(first_row[5], default=0.0)))
    long_stay_rate = max(0.0, min(100.0, _to_float(first_row[6], default=0.0)))
    icu_admission_rate = max(0.0, min(100.0, _to_float(first_row[7], default=0.0)))
    er_admission_rate = max(0.0, min(100.0, _to_float(first_row[8], default=0.0)))
    n_admissions = max(0, _to_int(first_row[9], default=0))
    readmit_30_cnt = max(0, _to_int(first_row[10], default=0))
    death_cnt = max(0, _to_int(first_row[11], default=0))
    los_stddev_days = max(0.0, _to_float(first_row[12], default=0.0))
    icu_admission_cnt = max(0, _to_int(first_row[13], default=0))
    er_admission_cnt = max(0, _to_int(first_row[14], default=0))
    readmit_7_cnt = max(0, _to_int(first_row[15], default=0))
    long_stay_cnt = max(0, _to_int(first_row[16], default=0))

    metrics = {
        "patient_count": float(round(patient_count)),
        "readmission_rate": float(round(readmission_rate, 2)),
        "mortality_rate": float(round(mortality_rate, 2)),
        "avg_los_days": float(round(avg_los_days, 2)),
        "median_los_days": float(round(median_los_days, 2)),
        "readmission_7d_rate": float(round(readmission_7d_rate, 2)),
        "long_stay_rate": float(round(long_stay_rate, 2)),
        "icu_admission_rate": float(round(icu_admission_rate, 2)),
        "er_admission_rate": float(round(er_admission_rate, 2)),
    }
    stats = {
        "n_admissions": float(n_admissions),
        "readmit_30_cnt": float(readmit_30_cnt),
        "death_cnt": float(death_cnt),
        "los_stddev_days": float(los_stddev_days),
        "icu_admission_cnt": float(icu_admission_cnt),
        "er_admission_cnt": float(er_admission_cnt),
        "readmit_7_cnt": float(readmit_7_cnt),
        "long_stay_cnt": float(long_stay_cnt),
    }
    return metrics, stats


def _simulate_metrics(params: CohortParams) -> dict[str, float]:
    metrics, _ = _simulate_metrics_and_stats(params)
    return metrics


def _parse_subgroup_row(row: Any, *, start_col: int = 0) -> dict[str, Any] | None:
    if not isinstance(row, (list, tuple)) or len(row) < (start_col + 7):
        return None
    return {
        "key": str(row[start_col] or ""),
        "label": str(row[start_col + 1] or ""),
        "admission_count": max(0, _to_int(row[start_col + 2], default=0)),
        "patient_count": max(0, _to_int(row[start_col + 3], default=0)),
        "readmission_rate": max(0.0, min(100.0, _to_float(row[start_col + 4], default=0.0))),
        "mortality_rate": max(0.0, min(100.0, _to_float(row[start_col + 5], default=0.0))),
        "avg_los_days": max(0.0, _to_float(row[start_col + 6], default=0.0)),
    }


def _parse_subgroup_rows(rows: list[Any], *, start_col: int = 0) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    for row in rows:
        item = _parse_subgroup_row(row, start_col=start_col)
        if item:
            parsed.append(item)
    return parsed


def _simulate_subgroups(params: CohortParams) -> dict[str, list[dict[str, Any]]]:
    sqls = _cohort_sql_bundle(params)
    combined_sql = str(sqls.get("all_subgroups_sql") or "").strip()
    if combined_sql:
        try:
            result = execute_sql(combined_sql)
            rows = result.get("rows") or []
            if isinstance(rows, list):
                sections: dict[str, list[dict[str, Any]]] = {
                    "age": [],
                    "gender": [],
                    "comorbidity": [],
                }
                for row in rows:
                    if not isinstance(row, (list, tuple)) or len(row) < 8:
                        continue
                    section = str(row[0] or "").strip().lower()
                    if section not in sections:
                        continue
                    parsed = _parse_subgroup_row(row, start_col=1)
                    if parsed:
                        sections[section].append(parsed)
                return sections
        except Exception as exc:
            logger.warning("combined subgroup SQL failed, fallback to per-section queries: %s", exc)

    def run(sql_key: str) -> list[dict[str, Any]]:
        result = execute_sql(sqls[sql_key])
        rows = result.get("rows") or []
        return _parse_subgroup_rows(rows if isinstance(rows, list) else [], start_col=0)

    return {
        "age": run("age_subgroup_sql"),
        "gender": run("gender_subgroup_sql"),
        "comorbidity": run("comorbidity_subgroup_sql"),
    }


@lru_cache(maxsize=_SIM_CACHE_MAXSIZE)
def _simulate_snapshot_cached(params_payload: str, cache_bucket: int) -> dict[str, Any]:
    # cache_bucket is part of cache key for TTL-like invalidation.
    _ = cache_bucket
    params = CohortParams(**json.loads(params_payload))
    metrics, stats = _simulate_metrics_and_stats(params)
    return {
        "metrics": metrics,
        "stats": stats,
        "subgroups": _simulate_subgroups(params),
        "life_table": _life_table(params),
    }


def _simulate_snapshot(params: CohortParams) -> dict[str, Any]:
    snapshot = _simulate_snapshot_cached(_params_payload(params), _simulation_cache_bucket())
    return copy.deepcopy(snapshot)


def _build_subgroup_comparison(
    current_subgroups: dict[str, list[dict[str, Any]]],
    simulated_subgroups: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    def empty_metrics() -> dict[str, float]:
        return {
            "admission_count": 0.0,
            "patient_count": 0.0,
            "readmission_rate": 0.0,
            "mortality_rate": 0.0,
            "avg_los_days": 0.0,
        }

    def normalize_metrics(item: dict[str, Any] | None) -> dict[str, float]:
        if not item:
            return empty_metrics()
        return {
            "admission_count": float(_to_int(item.get("admission_count"), default=0)),
            "patient_count": float(_to_int(item.get("patient_count"), default=0)),
            "readmission_rate": float(_to_float(item.get("readmission_rate"), default=0.0)),
            "mortality_rate": float(_to_float(item.get("mortality_rate"), default=0.0)),
            "avg_los_days": float(_to_float(item.get("avg_los_days"), default=0.0)),
        }

    def merge_section(
        current_rows: list[dict[str, Any]],
        simulated_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        current_by_key = {str(item.get("key")): item for item in current_rows}
        simulated_by_key = {str(item.get("key")): item for item in simulated_rows}
        ordered_keys: list[str] = [str(item.get("key")) for item in current_rows]
        for item in simulated_rows:
            key = str(item.get("key"))
            if key not in ordered_keys:
                ordered_keys.append(key)

        merged: list[dict[str, Any]] = []
        for key in ordered_keys:
            current_item = current_by_key.get(key)
            simulated_item = simulated_by_key.get(key)
            current_metrics = normalize_metrics(current_item)
            simulated_metrics = normalize_metrics(simulated_item)
            label = str(
                (current_item or {}).get("label")
                or (simulated_item or {}).get("label")
                or key
            )
            delta_metrics = {
                "admission_count": int(round(simulated_metrics["admission_count"] - current_metrics["admission_count"])),
                "patient_count": int(round(simulated_metrics["patient_count"] - current_metrics["patient_count"])),
                "readmission_rate": float(round(simulated_metrics["readmission_rate"] - current_metrics["readmission_rate"], 2)),
                "mortality_rate": float(round(simulated_metrics["mortality_rate"] - current_metrics["mortality_rate"], 2)),
                "avg_los_days": float(round(simulated_metrics["avg_los_days"] - current_metrics["avg_los_days"], 2)),
            }
            merged.append(
                {
                    "key": key,
                    "label": label,
                    "current": {
                        "admission_count": int(round(current_metrics["admission_count"])),
                        "patient_count": int(round(current_metrics["patient_count"])),
                        "readmission_rate": float(round(current_metrics["readmission_rate"], 2)),
                        "mortality_rate": float(round(current_metrics["mortality_rate"], 2)),
                        "avg_los_days": float(round(current_metrics["avg_los_days"], 2)),
                    },
                    "simulated": {
                        "admission_count": int(round(simulated_metrics["admission_count"])),
                        "patient_count": int(round(simulated_metrics["patient_count"])),
                        "readmission_rate": float(round(simulated_metrics["readmission_rate"], 2)),
                        "mortality_rate": float(round(simulated_metrics["mortality_rate"], 2)),
                        "avg_los_days": float(round(simulated_metrics["avg_los_days"], 2)),
                    },
                    "delta": delta_metrics,
                }
            )
        return merged

    return {
        "age": merge_section(current_subgroups.get("age", []), simulated_subgroups.get("age", [])),
        "gender": merge_section(current_subgroups.get("gender", []), simulated_subgroups.get("gender", [])),
        "comorbidity": merge_section(current_subgroups.get("comorbidity", []), simulated_subgroups.get("comorbidity", [])),
    }


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _two_sided_p_from_z(value: float) -> float:
    return max(0.0, min(1.0, 2.0 * (1.0 - _normal_cdf(abs(value)))))


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if q <= 0:
        return float(values[0])
    if q >= 1:
        return float(values[-1])
    pos = (len(values) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(values[lo])
    weight = pos - lo
    return float(values[lo] * (1.0 - weight) + values[hi] * weight)


def _cohen_h(p1: float, p2: float) -> float:
    p1 = min(1.0, max(0.0, p1))
    p2 = min(1.0, max(0.0, p2))
    return 2.0 * (math.asin(math.sqrt(p2)) - math.asin(math.sqrt(p1)))


def _cohen_d(mean1: float, mean2: float, sd1: float, sd2: float, n1: float, n2: float) -> float:
    if n1 <= 1 or n2 <= 1:
        return 0.0
    pooled_var = (((n1 - 1.0) * (sd1**2)) + ((n2 - 1.0) * (sd2**2))) / (n1 + n2 - 2.0)
    pooled_sd = math.sqrt(max(0.0, pooled_var))
    if pooled_sd <= 0:
        return 0.0
    return (mean2 - mean1) / pooled_sd


def _bootstrap_prop_diff(
    rng: random.Random,
    success_1: float,
    n_1: float,
    success_2: float,
    n_2: float,
    iterations: int,
) -> tuple[float, float]:
    if n_1 <= 0 or n_2 <= 0:
        return 0.0, 0.0
    diffs: list[float] = []
    a1 = max(1.0, success_1 + 1.0)
    b1 = max(1.0, (n_1 - success_1) + 1.0)
    a2 = max(1.0, success_2 + 1.0)
    b2 = max(1.0, (n_2 - success_2) + 1.0)
    for _ in range(iterations):
        p1 = rng.betavariate(a1, b1)
        p2 = rng.betavariate(a2, b2)
        diffs.append((p2 - p1) * 100.0)
    diffs.sort()
    return _percentile(diffs, 0.025), _percentile(diffs, 0.975)


def _bootstrap_mean_diff(
    rng: random.Random,
    mean_1: float,
    sd_1: float,
    n_1: float,
    mean_2: float,
    sd_2: float,
    n_2: float,
    iterations: int,
) -> tuple[float, float]:
    if n_1 <= 0 or n_2 <= 0:
        return 0.0, 0.0
    se1 = sd_1 / math.sqrt(max(1.0, n_1))
    se2 = sd_2 / math.sqrt(max(1.0, n_2))
    diffs: list[float] = []
    for _ in range(iterations):
        m1 = rng.gauss(mean_1, se1)
        m2 = rng.gauss(mean_2, se2)
        diffs.append(m2 - m1)
    diffs.sort()
    return _percentile(diffs, 0.025), _percentile(diffs, 0.975)


def _build_confidence_payload(
    current_metrics: dict[str, float],
    current_stats: dict[str, float],
    simulated_metrics: dict[str, float],
    simulated_stats: dict[str, float],
    baseline_params: CohortParams,
    simulated_params: CohortParams,
) -> dict[str, Any]:
    z_critical = 1.959963984540054
    alpha = 0.05
    iterations = 800
    seed_input = f"{baseline_params.model_dump_json()}::{simulated_params.model_dump_json()}"
    seed = int(hashlib.sha256(seed_input.encode("utf-8")).hexdigest()[:16], 16)
    rng = random.Random(seed)

    n1 = max(0.0, current_stats.get("n_admissions", 0.0))
    n2 = max(0.0, simulated_stats.get("n_admissions", 0.0))

    def build_prop_item(metric_key: str, label: str, count_key: str) -> dict[str, Any]:
        c1 = max(0.0, current_stats.get(count_key, 0.0))
        c2 = max(0.0, simulated_stats.get(count_key, 0.0))
        p1 = (c1 / n1) if n1 > 0 else 0.0
        p2 = (c2 / n2) if n2 > 0 else 0.0
        diff = (p2 - p1) * 100.0
        se = math.sqrt(max(0.0, (p1 * (1.0 - p1) / max(1.0, n1)) + (p2 * (1.0 - p2) / max(1.0, n2))))
        ci_low = diff - (z_critical * se * 100.0)
        ci_high = diff + (z_critical * se * 100.0)
        pooled = ((c1 + c2) / (n1 + n2)) if (n1 + n2) > 0 else 0.0
        se_pooled = math.sqrt(max(0.0, pooled * (1.0 - pooled) * ((1.0 / max(1.0, n1)) + (1.0 / max(1.0, n2)))))
        z = (p2 - p1) / se_pooled if se_pooled > 0 else 0.0
        p_value = _two_sided_p_from_z(z)
        effect_size = _cohen_h(p1, p2)
        boot_low, boot_high = _bootstrap_prop_diff(rng, c1, n1, c2, n2, iterations)
        return {
            "metric": metric_key,
            "label": label,
            "unit": "%",
            "current": float(round(current_metrics.get(metric_key, 0.0), 2)),
            "simulated": float(round(simulated_metrics.get(metric_key, 0.0), 2)),
            "difference": float(round(diff, 2)),
            "ci": [float(round(ci_low, 2)), float(round(ci_high, 2))],
            "p_value": float(round(p_value, 6)),
            "effect_size": float(round(effect_size, 4)),
            "effect_size_type": "cohen_h",
            "bootstrap_ci": [float(round(boot_low, 2)), float(round(boot_high, 2))],
            "significant": bool(p_value < alpha),
        }

    def build_mean_item(metric_key: str, label: str) -> dict[str, Any]:
        mean1 = float(current_metrics.get(metric_key, 0.0))
        mean2 = float(simulated_metrics.get(metric_key, 0.0))
        sd1 = max(0.0, float(current_stats.get("los_stddev_days", 0.0)))
        sd2 = max(0.0, float(simulated_stats.get("los_stddev_days", 0.0)))
        diff = mean2 - mean1
        se = math.sqrt(max(0.0, ((sd1**2) / max(1.0, n1)) + ((sd2**2) / max(1.0, n2))))
        ci_low = diff - (z_critical * se)
        ci_high = diff + (z_critical * se)
        z = diff / se if se > 0 else 0.0
        p_value = _two_sided_p_from_z(z)
        effect_size = _cohen_d(mean1, mean2, sd1, sd2, n1, n2)
        boot_low, boot_high = _bootstrap_mean_diff(rng, mean1, sd1, n1, mean2, sd2, n2, iterations)
        return {
            "metric": metric_key,
            "label": label,
            "unit": "days",
            "current": float(round(mean1, 2)),
            "simulated": float(round(mean2, 2)),
            "difference": float(round(diff, 2)),
            "ci": [float(round(ci_low, 2)), float(round(ci_high, 2))],
            "p_value": float(round(p_value, 6)),
            "effect_size": float(round(effect_size, 4)),
            "effect_size_type": "cohen_d",
            "bootstrap_ci": [float(round(boot_low, 2)), float(round(boot_high, 2))],
            "significant": bool(p_value < alpha),
        }

    metrics = [
        build_prop_item("readmission_rate", "재입원율(30일)", "readmit_30_cnt"),
        build_prop_item("readmission_7d_rate", "재입원율(7일)", "readmit_7_cnt"),
        build_prop_item("mortality_rate", "사망률", "death_cnt"),
        build_prop_item("long_stay_rate", "장기재원 비율(14일+)", "long_stay_cnt"),
        build_prop_item("icu_admission_rate", "ICU 입실 비율", "icu_admission_cnt"),
        build_prop_item("er_admission_rate", "응급실 입원 비율", "er_admission_cnt"),
        build_mean_item("avg_los_days", "평균 재원일수"),
    ]

    return {
        "method": "Wald CI + normal approximation p-value + effect size + parametric bootstrap",
        "alpha": alpha,
        "bootstrap_iterations": iterations,
        "n_current": int(round(n1)),
        "n_simulated": int(round(n2)),
        "metrics": metrics,
    }


def _round_metric_values(metrics: dict[str, Any]) -> dict[str, float]:
    rounded: dict[str, float] = {}
    for key in (
        "patient_count",
        "readmission_rate",
        "mortality_rate",
        "avg_los_days",
        "median_los_days",
        "readmission_7d_rate",
        "long_stay_rate",
        "icu_admission_rate",
        "er_admission_rate",
    ):
        value = _to_float(metrics.get(key), default=0.0)
        precision = 0 if key == "patient_count" else 2
        rounded[key] = float(round(value, precision))
    return rounded


def _build_metric_delta_payload(
    current_metrics: dict[str, Any],
    simulated_metrics: dict[str, Any],
) -> list[dict[str, Any]]:
    specs = [
        ("patient_count", "대상 환자 수", "명"),
        ("readmission_rate", "재입원율(30일)", "%p"),
        ("readmission_7d_rate", "재입원율(7일)", "%p"),
        ("mortality_rate", "사망률", "%p"),
        ("avg_los_days", "평균 재원일수", "일"),
        ("long_stay_rate", "장기재원 비율(14일+)", "%p"),
        ("icu_admission_rate", "ICU 입실 비율", "%p"),
        ("er_admission_rate", "응급실 입원 비율", "%p"),
    ]
    rows: list[dict[str, Any]] = []
    for key, label, unit in specs:
        current = _to_float(current_metrics.get(key), default=0.0)
        simulated = _to_float(simulated_metrics.get(key), default=0.0)
        delta = simulated - current
        rows.append({
            "metric": key,
            "label": label,
            "unit": unit,
            "current": float(round(current, 2)),
            "simulated": float(round(simulated, 2)),
            "delta": float(round(delta, 2)),
            "abs_delta": float(round(abs(delta), 2)),
        })
    return rows


def _build_significance_brief(confidence: dict[str, Any]) -> list[dict[str, Any]]:
    metrics = confidence.get("metrics", []) if isinstance(confidence, dict) else []
    if not isinstance(metrics, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in metrics:
        if not isinstance(item, dict):
            continue
        if not bool(item.get("significant")):
            continue
        rows.append({
            "metric": str(item.get("metric") or ""),
            "label": str(item.get("label") or ""),
            "difference": float(round(_to_float(item.get("difference"), default=0.0), 2)),
            "ci": [
                float(round(_to_float((item.get("ci") or [0, 0])[0], default=0.0), 2)),
                float(round(_to_float((item.get("ci") or [0, 0])[1], default=0.0), 2)),
            ],
            "p_value": float(round(_to_float(item.get("p_value"), default=1.0), 6)),
            "effect_size": float(round(_to_float(item.get("effect_size"), default=0.0), 4)),
            "effect_size_type": str(item.get("effect_size_type") or ""),
        })
    rows.sort(key=lambda item: abs(_to_float(item.get("difference"), default=0.0)), reverse=True)
    return rows[:5]


def _build_subgroup_shift_brief(subgroups: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    section_label = {"age": "나이대", "gender": "성별", "comorbidity": "기저질환"}
    delta_specs = [
        ("readmission_rate", "재입원율", "%p"),
        ("mortality_rate", "사망률", "%p"),
        ("avg_los_days", "평균 재원일수", "일"),
    ]
    for section_key in ("age", "gender", "comorbidity"):
        section_rows = subgroups.get(section_key, []) if isinstance(subgroups, dict) else []
        if not isinstance(section_rows, list):
            continue
        for item in section_rows:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label") or item.get("key") or "").strip()
            delta = item.get("delta", {})
            if not label or not isinstance(delta, dict):
                continue
            for metric_key, metric_label, unit in delta_specs:
                value = _to_float(delta.get(metric_key), default=0.0)
                if abs(value) <= 0.0:
                    continue
                rows.append({
                    "section": section_label.get(section_key, section_key),
                    "group": label,
                    "metric": metric_key,
                    "label": metric_label,
                    "unit": unit,
                    "delta": float(round(value, 2)),
                    "abs_delta": float(round(abs(value), 2)),
                })
    rows.sort(key=lambda item: _to_float(item.get("abs_delta"), default=0.0), reverse=True)
    return rows[:8]


def _build_survival_brief(survival: list[dict[str, Any]]) -> list[dict[str, float]]:
    if not isinstance(survival, list) or not survival:
        return []
    points = {0, 30, 90, 180}
    rows: list[dict[str, float]] = []
    for item in survival:
        if not isinstance(item, dict):
            continue
        day = int(round(_to_float(item.get("time"), default=0.0)))
        if day not in points:
            continue
        current = _to_float(item.get("current"), default=0.0)
        simulated = _to_float(item.get("simulated"), default=0.0)
        rows.append({
            "day": float(day),
            "current": float(round(current, 2)),
            "simulated": float(round(simulated, 2)),
            "delta": float(round(simulated - current, 2)),
        })
    rows.sort(key=lambda item: _to_float(item.get("day"), default=0.0))
    return rows


def _build_cohort_simulation_insight_payload(
    *,
    baseline_params: CohortParams,
    simulated_params: CohortParams,
    current_metrics: dict[str, Any],
    simulated_metrics: dict[str, Any],
    confidence: dict[str, Any],
    subgroups: dict[str, Any],
    survival: list[dict[str, Any]],
) -> dict[str, Any]:
    metric_deltas = _build_metric_delta_payload(current_metrics, simulated_metrics)
    metric_deltas.sort(key=lambda item: _to_float(item.get("abs_delta"), default=0.0), reverse=True)
    return {
        "baseline_params": baseline_params.model_dump(),
        "simulated_params": simulated_params.model_dump(),
        "current_metrics": _round_metric_values(current_metrics),
        "simulated_metrics": _round_metric_values(simulated_metrics),
        "top_metric_changes": metric_deltas[:6],
        "significant_changes": _build_significance_brief(confidence),
        "top_subgroup_shifts": _build_subgroup_shift_brief(subgroups),
        "survival_snapshot": _build_survival_brief(survival),
    }


def _fallback_simulation_insight(
    *,
    current_metrics: dict[str, Any],
    simulated_metrics: dict[str, Any],
    baseline_params: CohortParams,
    simulated_params: CohortParams,
    confidence: dict[str, Any],
) -> str:
    readmit_diff = _to_float(simulated_metrics.get("readmission_rate")) - _to_float(current_metrics.get("readmission_rate"))
    mortality_diff = _to_float(simulated_metrics.get("mortality_rate")) - _to_float(current_metrics.get("mortality_rate"))
    los_diff = _to_float(simulated_metrics.get("avg_los_days")) - _to_float(current_metrics.get("avg_los_days"))
    icu_diff = _to_float(simulated_metrics.get("icu_admission_rate")) - _to_float(current_metrics.get("icu_admission_rate"))
    er_diff = _to_float(simulated_metrics.get("er_admission_rate")) - _to_float(current_metrics.get("er_admission_rate"))
    n_significant = len(_build_significance_brief(confidence))

    baseline_note = ""
    if baseline_params.model_dump() != simulated_params.model_dump():
        baseline_note = (
            f"기준군({baseline_params.age_threshold}세 이상, 재입원 {baseline_params.readmit_days}일) 대비 "
        )

    return (
        f"{baseline_note}시뮬레이션 결과 재입원율은 {readmit_diff:+.2f}%p, 사망률은 {mortality_diff:+.2f}%p, "
        f"평균 재원일수는 {los_diff:+.2f}일 변화했습니다. "
        f"ICU 입실 비율은 {icu_diff:+.2f}%p, 응급실 입원 비율은 {er_diff:+.2f}%p 변동했습니다. "
        f"통계적으로 유의한 지표는 {n_significant}개로 확인되며, 해석 시 분모 변화와 선택 편향 가능성을 함께 점검해야 합니다."
    )


def _llm_simulation_insight(
    *,
    baseline_params: CohortParams,
    simulated_params: CohortParams,
    current_metrics: dict[str, Any],
    simulated_metrics: dict[str, Any],
    confidence: dict[str, Any],
    subgroups: dict[str, Any],
    survival: list[dict[str, Any]],
) -> str:
    settings = get_settings()
    if not str(settings.openai_api_key or "").strip():
        raise RuntimeError("OPENAI_API_KEY is not configured")

    payload = _build_cohort_simulation_insight_payload(
        baseline_params=baseline_params,
        simulated_params=simulated_params,
        current_metrics=current_metrics,
        simulated_metrics=simulated_metrics,
        confidence=confidence,
        subgroups=subgroups,
        survival=survival,
    )
    messages = [
        {
            "role": "system",
            "content": (
                "너는 임상 코호트 What-if 시뮬레이션 해석 도우미다. "
                "출력은 JSON만 허용하며 키는 insight 하나만 사용한다. "
                "insight는 한국어 3~5문장으로 작성하고 '-습니다/입니다' 존댓말로 끝낸다. "
                "숫자 근거를 2개 이상 포함하고, 개선/악화 방향을 명확히 비교한다. "
                "과장하거나 인과를 단정하지 말고, 마지막 문장에 해석 주의사항을 넣는다. "
                "마크다운/불릿/줄바꿈 없이 한 단락으로 작성한다."
            ),
        },
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]

    client = LLMClient()
    response = client.chat(
        messages=messages,
        model=settings.expert_model,
        max_tokens=max(220, min(420, int(getattr(settings, "llm_max_output_tokens_expert", settings.llm_max_output_tokens)))),
        expect_json=True,
    )
    parsed = extract_json_object(str(response.get("content") or ""))
    insight = " ".join(str(parsed.get("insight") or "").strip().split())
    if not insight:
        raise RuntimeError("cohort insight is empty")
    return insight


def _generate_simulation_insight(
    *,
    baseline_params: CohortParams,
    simulated_params: CohortParams,
    current_metrics: dict[str, Any],
    simulated_metrics: dict[str, Any],
    confidence: dict[str, Any],
    subgroups: dict[str, Any],
    survival: list[dict[str, Any]],
) -> tuple[str, str]:
    fallback = _fallback_simulation_insight(
        current_metrics=current_metrics,
        simulated_metrics=simulated_metrics,
        baseline_params=baseline_params,
        simulated_params=simulated_params,
        confidence=confidence,
    )
    try:
        insight = _llm_simulation_insight(
            baseline_params=baseline_params,
            simulated_params=simulated_params,
            current_metrics=current_metrics,
            simulated_metrics=simulated_metrics,
            confidence=confidence,
            subgroups=subgroups,
            survival=survival,
        )
        return insight, "llm"
    except Exception as exc:
        logger.warning("cohort insight LLM generation failed: %s", exc)
        return fallback, "fallback"


def _life_table(params: CohortParams) -> list[tuple[float, float, float]]:
    sql = _cohort_sql_bundle(params)["life_table_sql"]
    result = execute_sql(sql)
    rows = result.get("rows") or []
    table: list[tuple[float, float, float]] = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 3:
            continue
        day = _to_float(row[0], default=0.0)
        event_cnt = max(0.0, _to_float(row[1], default=0.0))
        censor_cnt = max(0.0, _to_float(row[2], default=0.0))
        table.append((day, event_cnt, censor_cnt))
    table.sort(key=lambda item: item[0])
    return table


def _km_curve_from_life_table(
    life_table: list[tuple[float, float, float]],
    time_points: list[int],
) -> list[float]:
    total = sum(event_cnt + censor_cnt for _, event_cnt, censor_cnt in life_table)
    if total <= 0:
        return [0.0 for _ in time_points]

    n_risk = total
    survival = 1.0
    idx = 0
    values: list[float] = []

    for t in time_points:
        while idx < len(life_table) and life_table[idx][0] <= t:
            _, event_cnt, censor_cnt = life_table[idx]
            if n_risk > 0 and event_cnt > 0:
                step = max(0.0, 1.0 - (event_cnt / n_risk))
                survival *= step
            n_risk = max(0.0, n_risk - event_cnt - censor_cnt)
            idx += 1
        values.append(round(max(0.0, min(100.0, survival * 100.0)), 1))
    return values


def _build_survival_payload_from_life_tables(
    current_life_table: list[tuple[float, float, float]],
    simulated_life_table: list[tuple[float, float, float]],
) -> list[dict[str, float]]:
    points = _SURVIVAL_TIME_POINTS
    current_values = _km_curve_from_life_table(current_life_table, points)
    simulated_values = _km_curve_from_life_table(simulated_life_table, points)
    return [
        {
            "time": float(day),
            "current": float(current_values[idx]),
            "simulated": float(simulated_values[idx]),
        }
        for idx, day in enumerate(points)
    ]


def _build_survival_payload(
    current_params: CohortParams,
    simulated_params: CohortParams,
) -> list[dict[str, float]]:
    if current_params.model_dump() == simulated_params.model_dump():
        table = _life_table(current_params)
        return _build_survival_payload_from_life_tables(table, table)
    return _build_survival_payload_from_life_tables(
        _life_table(current_params),
        _life_table(simulated_params),
    )


def _saved_cohorts_key(user: str | None) -> str:
    return scoped_state_key(_SAVED_COHORTS_KEY, user)


def _get_saved_cohorts(user: str | None = None) -> list[dict[str, Any]]:
    key = _saved_cohorts_key(user)
    store = get_state_store()
    if not store.enabled:
        return list(_FALLBACK_SAVED_COHORTS.get(key, []))
    payload = store.get(key) or {}
    cohorts = payload.get("cohorts", []) if isinstance(payload, dict) else []
    return cohorts if isinstance(cohorts, list) else []


def _set_saved_cohorts(cohorts: list[dict[str, Any]], user: str | None = None) -> None:
    key = _saved_cohorts_key(user)
    store = get_state_store()
    if not store.enabled:
        _FALLBACK_SAVED_COHORTS[key] = list(cohorts)
        return
    ok = store.set(key, {"cohorts": cohorts})
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to persist cohorts")


@router.post("/simulate")
def cohort_simulate(req: SimulationRequest):
    with use_request_user(req.user):
        simulated_params = req.params
        simulated_snapshot = _simulate_snapshot(simulated_params)
        simulated_metrics = simulated_snapshot.get("metrics", {})
        simulated_stats = simulated_snapshot.get("stats", {})
        simulated_subgroups = simulated_snapshot.get("subgroups", {})
        simulated_life_table = simulated_snapshot.get("life_table", [])

        if req.include_baseline:
            baseline_params = CohortParams(**DEFAULT_PARAMS)
            if baseline_params.model_dump() == simulated_params.model_dump():
                current_snapshot = simulated_snapshot
            else:
                current_snapshot = _simulate_snapshot(baseline_params)
        else:
            baseline_params = simulated_params
            current_snapshot = simulated_snapshot

        current_metrics = current_snapshot.get("metrics", {})
        current_stats = current_snapshot.get("stats", {})
        current_subgroups = current_snapshot.get("subgroups", {})
        current_life_table = current_snapshot.get("life_table", [])

        survival = _build_survival_payload_from_life_tables(
            current_life_table if isinstance(current_life_table, list) else [],
            simulated_life_table if isinstance(simulated_life_table, list) else [],
        )
        confidence = _build_confidence_payload(
            current_metrics=current_metrics,
            current_stats=current_stats,
            simulated_metrics=simulated_metrics,
            simulated_stats=simulated_stats,
            baseline_params=baseline_params,
            simulated_params=simulated_params,
        )
        subgroups = _build_subgroup_comparison(current_subgroups, simulated_subgroups)
        insight, insight_source = _generate_simulation_insight(
            baseline_params=baseline_params,
            simulated_params=simulated_params,
            current_metrics=current_metrics,
            simulated_metrics=simulated_metrics,
            confidence=confidence,
            subgroups=subgroups,
            survival=survival,
        )
        return {
            "params": simulated_params.model_dump(),
            "baseline_params": baseline_params.model_dump(),
            "current": current_metrics,
            "simulated": simulated_metrics,
            "survival": survival,
            "confidence": confidence,
            "subgroups": subgroups,
            "insight": insight,
            "insight_source": insight_source,
        }


@router.post("/sql")
def cohort_sql(req: CohortSqlRequest):
    params = req.params
    return {
        "params": params.model_dump(),
        "sample_rows": _cohort_sample_rows(),
        "sql": _cohort_sql_bundle(params),
    }


@router.post("/pdf/generate-sql")
def generate_smart_sql(req: SmartSqlRequest):
    """
    Use run_oneshot to generate SQL from PDF-derived summary/criteria.
    """
    req_user = req.user or get_request_user_id() or None
    try:
        question = (
            f"논문 제목/파일: {req.filename or '알 수 없음'}\n"
            f"연구 요약: {req.summary}\n"
            f"선정 및 제외 기준: {req.criteria}\n\n"
            "위 연구 디자인을 SQL 쿼리로 변환해줘. "
            "MIMIC-IV 스키마를 사용하고, 단계별로 환자가 필터링되는 Funnel 형태의 CTE 구조를 만들어줘."
        )

        with use_request_user(req_user):
            payload = run_oneshot(
                question,
                translate=False,
                rag_multi=True,
                enable_clarification=False,
            )

        final_sql = ""
        if isinstance(payload, dict):
            if isinstance(payload.get("final"), dict):
                final_sql = str(payload["final"].get("final_sql") or "").strip()
            elif isinstance(payload.get("draft"), dict):
                final_sql = str(payload["draft"].get("final_sql") or "").strip()

        if not final_sql:
            raise HTTPException(status_code=500, detail="SQL generation failed")

        with use_request_user(req_user):
            db_result = execute_sql(final_sql)

        return {
            "generated_sql": {
                "cohort_sql": final_sql,
                "count_sql": f"SELECT COUNT(*) FROM ({final_sql.replace('FETCH FIRST 100 ROWS ONLY', '')})",
            },
            "db_result": {
                "columns": db_result.get("columns", []),
                "rows": db_result.get("rows", [])[:50],
                "row_count": db_result.get("row_count", 0),
                "total_count": db_result.get("total_count"),
                "error": db_result.get("error"),
            },
            "user": req_user,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/pdf/confirm")
def confirm_pdf_cohort(req: ConfirmPdfCohortRequest):
    """
    Persist final PDF cohort extraction result by pdf_hash after SQL validation.
    """
    confirmed_store = AppStateStore(collection_name="pdf_confirmed_cohorts")
    if not confirmed_store.enabled:
        raise HTTPException(status_code=500, detail="State store is not enabled")

    req_user = req.user or get_request_user_id() or None

    generated_sql = req.data.get("generated_sql", {}) if isinstance(req.data, dict) else {}
    cohort_sql = str((generated_sql or {}).get("cohort_sql") or "").strip()
    if not cohort_sql:
        raise HTTPException(status_code=400, detail="Generated cohort SQL is missing")

    validation_res: dict[str, Any] = {}
    validation_error: str | None = None
    with use_request_user(req_user):
        try:
            validation_res = execute_sql(cohort_sql)
        except HTTPException as exc:
            validation_error = str(exc.detail)
        except Exception as exc:
            validation_error = str(exc)

    if validation_error and req.status == "confirmed":
        raise HTTPException(
            status_code=422,
            detail=f"Cannot confirm cohort due to SQL error: {validation_error}",
        )

    now_iso = datetime.now().isoformat()
    saved_status = req.status if not validation_error else "draft"

    merged_data = dict(req.data)
    db_result = merged_data.get("db_result")
    if not isinstance(db_result, dict):
        db_result = {}
    if validation_error:
        db_result["error"] = validation_error
    else:
        db_result["columns"] = validation_res.get("columns", [])
        db_result["rows"] = validation_res.get("rows", [])[:100]
        db_result["row_count"] = validation_res.get("row_count", 0)
        db_result["total_count"] = validation_res.get("total_count")
        db_result["error"] = None
    merged_data["db_result"] = db_result

    payload = {
        **merged_data,
        "pdf_hash": req.pdf_hash,
        "user_id": req_user,
        "status": saved_status,
        "confirmed_at": now_iso if saved_status == "confirmed" else None,
        "updated_at": now_iso,
    }

    save_key = scoped_state_key(req.pdf_hash, req_user)
    ok = confirmed_store.set(save_key, payload)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to save cohort data")

    logger.info("Saved PDF cohort (%s) status=%s user=%s", req.pdf_hash, saved_status, req_user or "-")
    return {
        "status": "success",
        "message": "Cohort confirmed and saved" if saved_status == "confirmed" else "Cohort saved as draft",
        "pdf_hash": req.pdf_hash,
        "user": req_user,
        "saved_status": saved_status,
    }


@router.get("/library")
def list_cohort_library(
    user: str | None = None,
    type: str | None = None,
    q: str | None = None,
    limit: int = 100,
    cursor: str | None = None,
):
    req_user = user or get_request_user_id() or None
    items = _get_cohort_library(req_user)
    cohort_type = str(type or "").strip().upper()
    if cohort_type in {"CROSS_SECTIONAL", "PDF_DERIVED"}:
        items = [item for item in items if str(item.get("type") or "").upper() == cohort_type]
    keyword = str(q or "").strip().lower()
    if keyword:
        filtered: list[dict[str, Any]] = []
        for item in items:
            source = item.get("source") if isinstance(item.get("source"), dict) else {}
            corpus = " ".join(
                [
                    str(item.get("name") or ""),
                    str(item.get("description") or ""),
                    str(item.get("human_summary") or ""),
                    str(item.get("sql_filter_summary") or ""),
                    str(source.get("pdf_name") or ""),
                ]
            ).lower()
            if keyword in corpus:
                filtered.append(item)
        items = filtered
    items.sort(key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""), reverse=True)
    safe_limit = max(1, min(200, int(limit or 100)))
    offset = 0
    if cursor:
        try:
            offset = max(0, int(cursor))
        except Exception:
            offset = 0
    sliced = items[offset : offset + safe_limit]
    next_cursor = str(offset + safe_limit) if (offset + safe_limit) < len(items) else None
    return {"items": sliced, "next_cursor": next_cursor}


@router.post("/library")
def create_cohort_library_item(req: CohortLibraryCreateRequest):
    req_user = req.user or get_request_user_id() or None
    cohort_sql = _strip_terminal_semicolon(req.cohort_sql)
    _validate_readonly_sql(cohort_sql)
    now_iso = _iso_now()
    count_value = req.count
    if count_value is None:
        try:
            count_value = _best_effort_count_from_sql(cohort_sql, req_user)
        except Exception:
            count_value = None

    normalized = _normalize_saved_cohort_item(
        {
            "id": str(uuid.uuid4()),
            "type": req.type,
            "name": req.name.strip(),
            "description": req.description.strip() if req.description else None,
            "cohort_sql": cohort_sql,
            "count": int(count_value) if count_value is not None else None,
            "sql_filter_summary": req.sql_filter_summary.strip() if req.sql_filter_summary else None,
            "human_summary": req.human_summary.strip() if req.human_summary else None,
            "source": req.source.model_dump(),
            "pdf_details": req.pdf_details.model_dump() if req.pdf_details else None,
            "params": req.params if isinstance(req.params, dict) else None,
            "metrics": req.metrics if isinstance(req.metrics, dict) else None,
            "status": req.status,
            "created_at": now_iso,
            "updated_at": now_iso,
        }
    )
    if not normalized:
        raise HTTPException(status_code=400, detail="Invalid cohort payload")
    items = _get_cohort_library(req_user)
    items.append(normalized)
    _set_cohort_library(items, req_user)
    return normalized


@router.get("/library/{cohort_id}")
def get_cohort_library_item(cohort_id: str, user: str | None = None):
    req_user = user or get_request_user_id() or None
    items = _get_cohort_library(req_user)
    target = next((item for item in items if str(item.get("id")) == cohort_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="Cohort not found")
    return target


@router.patch("/library/{cohort_id}")
def patch_cohort_library_item(cohort_id: str, req: CohortLibraryPatchRequest):
    req_user = req.user or get_request_user_id() or None
    items = _get_cohort_library(req_user)
    idx = next((index for index, item in enumerate(items) if str(item.get("id")) == cohort_id), -1)
    if idx < 0:
        raise HTTPException(status_code=404, detail="Cohort not found")
    current = dict(items[idx])
    if req.name is not None:
        current["name"] = req.name.strip()
    if req.description is not None:
        current["description"] = req.description.strip() or None
    current["updated_at"] = _iso_now()
    normalized = _normalize_saved_cohort_item(current)
    if not normalized:
        raise HTTPException(status_code=400, detail="Invalid cohort payload")
    items[idx] = normalized
    _set_cohort_library(items, req_user)
    return normalized


@router.delete("/library/{cohort_id}")
def delete_cohort_library_item(cohort_id: str, user: str | None = None):
    req_user = user or get_request_user_id() or None
    items = _get_cohort_library(req_user)
    next_items = [item for item in items if str(item.get("id")) != cohort_id]
    if len(next_items) == len(items):
        raise HTTPException(status_code=404, detail="Cohort not found")
    _set_cohort_library(next_items, req_user)

    legacy = _get_saved_cohorts(req_user)
    next_legacy = [item for item in legacy if str(item.get("id")) != cohort_id]
    if len(next_legacy) != len(legacy):
        _set_saved_cohorts(next_legacy, req_user)
    return {"ok": True, "count": len(next_items)}


@router.get("/saved")
def list_saved_cohorts(user: str | None = None):
    req_user = user or get_request_user_id() or None
    cohorts = [item for item in _get_cohort_library(req_user) if str(item.get("type") or "").upper() == "CROSS_SECTIONAL"]
    cohorts.sort(key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""), reverse=True)
    legacy_like = [
        {
            "id": item.get("id"),
            "name": item.get("name"),
            "created_at": item.get("created_at"),
            "status": item.get("status") or "active",
            "params": item.get("params") if isinstance(item.get("params"), dict) else None,
            "metrics": item.get("metrics") if isinstance(item.get("metrics"), dict) else None,
            "type": item.get("type"),
            "count": item.get("count"),
            "cohort_sql": item.get("cohort_sql"),
            "updated_at": item.get("updated_at"),
            "sql_filter_summary": item.get("sql_filter_summary"),
            "human_summary": item.get("human_summary"),
            "source": item.get("source"),
            "description": item.get("description"),
        }
        for item in cohorts
    ]
    if legacy_like:
        return {"cohorts": legacy_like}
    # Backward-compatible fallback when cohort library is empty.
    fallback = _get_saved_cohorts(req_user)
    fallback.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    return {"cohorts": fallback}


@router.post("/saved")
def save_cohort(req: SaveCohortRequest):
    params = req.params
    req_user = req.user or get_request_user_id() or None
    with use_request_user(req_user):
        metrics = _simulate_metrics(params)
    now_iso = _iso_now()
    cohort_id = str(uuid.uuid4())
    cohort_sql = _cross_sectional_cohort_sql(params)
    cohort_count = _to_int(metrics.get("patient_count"), default=-1)

    cohort_library_item = _normalize_saved_cohort_item(
        {
            "id": cohort_id,
            "type": "CROSS_SECTIONAL",
            "name": req.name.strip(),
            "description": None,
            "cohort_sql": cohort_sql,
            "count": cohort_count if cohort_count >= 0 else None,
            "sql_filter_summary": _cross_sectional_filter_summary(params),
            "human_summary": _cross_sectional_human_summary(params),
            "source": {"created_from": "CROSS_SECTIONAL_PAGE"},
            "params": params.model_dump(),
            "metrics": metrics,
            "status": req.status,
            "created_at": now_iso,
            "updated_at": now_iso,
        }
    )
    if not cohort_library_item:
        raise HTTPException(status_code=400, detail="Failed to build cohort payload")
    library_items = _get_cohort_library(req_user)
    library_items.append(cohort_library_item)
    _set_cohort_library(library_items, req_user)

    legacy_created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cohort = {
        "id": cohort_id,
        "name": req.name.strip(),
        "created_at": legacy_created_at,
        "status": req.status,
        "params": params.model_dump(),
        "metrics": metrics,
    }
    cohorts = _get_saved_cohorts(req_user)
    cohorts.append(cohort)
    _set_saved_cohorts(cohorts, req_user)
    return {"ok": True, "cohort": cohort, "item": cohort_library_item}


@router.delete("/saved/{cohort_id}")
def delete_saved_cohort(cohort_id: str, user: str | None = None):
    req_user = user or get_request_user_id() or None
    cohorts = _get_saved_cohorts(req_user)
    next_cohorts = [item for item in cohorts if str(item.get("id")) != cohort_id]
    if len(next_cohorts) != len(cohorts):
        _set_saved_cohorts(next_cohorts, req_user)

    items = _get_cohort_library(req_user)
    next_items = [item for item in items if str(item.get("id")) != cohort_id]
    if len(next_items) == len(items) and len(next_cohorts) == len(cohorts):
        raise HTTPException(status_code=404, detail="Cohort not found")
    if len(next_items) != len(items):
        _set_cohort_library(next_items, req_user)
    return {"ok": True, "count": len(next_items)}
