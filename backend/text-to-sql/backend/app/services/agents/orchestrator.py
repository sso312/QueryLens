from __future__ import annotations

from typing import Any
from dataclasses import dataclass
from datetime import datetime
import math
import json
import re
from pathlib import Path

from fastapi import HTTPException

from app.core.config import get_settings
from app.services.agents.clarifier import evaluate_question_clarity
from app.services.agents.sql_engineer import generate_sql
from app.services.agents.sql_expert import review_sql
from app.services.agents.intent_guard import enforce_intent_alignment
from app.services.agents.sql_planner import plan_query_intent
from app.services.agents.sql_postprocess import postprocess_sql, recommend_postprocess_profile
from app.services.agents.translator import contains_korean, translate_to_english
from app.services.policy.gate import precheck_sql
from app.services.runtime.context_builder import build_context_payload, build_context_payload_multi
from app.services.runtime.context_budget import trim_context_to_budget
from app.services.runtime.risk_classifier import classify

_SQL_EXAMPLES_CACHE: dict[str, Any] | None = None
_SQL_EXAMPLES_CACHE_PATH: str | None = None
_SQL_EXACT_KEEP_RE = re.compile(r"[^0-9a-z가-힣]+")
_FOLLOWUP_CUE_RE = re.compile(
    r"(그\s*조건|그\s*결과|해당\s*조건|이전\s*질문|앞선\s*질문|같은\s*조건|방금|"
    r"\b(then|previous|above|same condition|based on that|what about that)\b)",
    re.IGNORECASE,
)


_DEFERRED_SCOPE_TABLES = {"dual"}


def _load_demo_cache(path: str) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _normalize_question(text: str) -> str:
    cleaned = text.lower()
    cleaned = re.sub(r"[^a-z0-9\s]", " ", cleaned)
    cleaned = re.sub(r"[가-힣]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _normalize_exact_match_question(text: str) -> str:
    cleaned = str(text or "").strip().lower()
    cleaned = _SQL_EXACT_KEEP_RE.sub(" ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _load_sql_examples_cache(path: str) -> dict[str, Any]:
    global _SQL_EXAMPLES_CACHE, _SQL_EXAMPLES_CACHE_PATH
    settings = get_settings()
    files: list[Path] = [Path(path)]
    if bool(getattr(settings, "sql_examples_include_augmented", False)):
        augmented_path = Path(str(getattr(settings, "sql_examples_augmented_path", "var/metadata/sql_examples_augmented.jsonl")))
        if str(augmented_path) not in {str(item) for item in files}:
            files.append(augmented_path)
    cache_key = "|".join(str(item) for item in files)
    if _SQL_EXAMPLES_CACHE is not None and _SQL_EXAMPLES_CACHE_PATH == cache_key:
        return _SQL_EXAMPLES_CACHE

    by_raw: dict[str, dict[str, str]] = {}
    by_norm: dict[str, list[dict[str, str]]] = {}
    for file_path in files:
        if not file_path.exists():
            continue
        for line in file_path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text:
                continue
            try:
                item = json.loads(text)
            except json.JSONDecodeError:
                continue
            if not isinstance(item, dict):
                continue
            q = str(item.get("question") or "").strip()
            s = str(item.get("sql") or "").strip()
            if not q or not s:
                continue
            if q in by_raw:
                continue
            row = {"question": q, "sql": s}
            by_raw[q] = row
            norm = _normalize_exact_match_question(q)
            if norm:
                by_norm.setdefault(norm, []).append(row)

    _SQL_EXAMPLES_CACHE = {"by_raw": by_raw, "by_norm": by_norm}
    _SQL_EXAMPLES_CACHE_PATH = cache_key
    return _SQL_EXAMPLES_CACHE


def _lookup_sql_examples_exact(*questions: str | None) -> dict[str, str] | None:
    settings = get_settings()
    if not bool(getattr(settings, "sql_examples_exact_match_enabled", True)):
        return None
    cache = _load_sql_examples_cache(str(getattr(settings, "sql_examples_path", "var/metadata/sql_examples.jsonl")))
    by_raw = cache.get("by_raw", {})
    by_norm = cache.get("by_norm", {})
    if not isinstance(by_raw, dict) or not isinstance(by_norm, dict):
        return None

    for question in questions:
        text = str(question or "").strip()
        if not text:
            continue
        exact = by_raw.get(text)
        if isinstance(exact, dict):
            return exact

    for question in questions:
        text = str(question or "").strip()
        if not text:
            continue
        norm = _normalize_exact_match_question(text)
        if not norm:
            continue
        candidates = by_norm.get(norm) or []
        if not candidates:
            continue
        if len(candidates) == 1:
            return candidates[0]
        lowered = text.lower()
        exact_matches = [
            candidate
            for candidate in candidates
            if str(candidate.get("question") or "").strip().lower() == lowered
        ]
        if len(exact_matches) == 1:
            return exact_matches[0]
        # Ambiguous normalized collisions are skipped to avoid mis-anchoring SQL.
        continue
    return None


def _lookup_demo_cache(cache: dict[str, Any], question: str) -> dict[str, Any] | None:
    if question in cache:
        return {
            "mode": "demo",
            "question": question,
            "result": cache[question],
        }
    alias_map = cache.get("_aliases", {}) if isinstance(cache, dict) else {}
    if isinstance(alias_map, dict):
        aliased_key = alias_map.get(question)
        if aliased_key and aliased_key in cache:
            return {
                "mode": "demo",
                "question": question,
                "result": cache[aliased_key],
                "matched": aliased_key,
            }
    normalized = _normalize_question(question)
    if normalized:
        index = {_normalize_question(k): k for k in cache.keys() if k != "_aliases"}
        matched_key = index.get(normalized)
        if matched_key:
            return {
                "mode": "demo",
                "question": question,
                "result": cache[matched_key],
                "matched": matched_key,
            }
    return None


def _add_llm_cost(usage: dict[str, Any], stage: str) -> None:
    # Cost tracking is intentionally detached from the LLM execution path.
    return None


def _is_deferred_table_scope_error(exc: HTTPException) -> bool:
    detail = str(getattr(exc, "detail", "") or "").strip()
    if not detail:
        return False
    lowered = detail.lower()
    if not lowered.startswith("table not allowed:"):
        return False
    raw = detail.split(":", 1)[1] if ":" in detail else ""
    blocked = [part.strip().lower() for part in raw.split(",") if part.strip()]
    return bool(blocked) and all(name in _DEFERRED_SCOPE_TABLES for name in blocked)


def _precheck_oneshot_sql(sql: str, question: str) -> dict[str, Any]:
    try:
        return precheck_sql(sql, question)
    except HTTPException as exc:
        if not _is_deferred_table_scope_error(exc):
            raise
        return {
            "passed": False,
            "deferred": True,
            "detail": str(exc.detail),
            "checks": [
                {
                    "name": "Table scope",
                    "passed": False,
                    "message": "Disallowed: DUAL (deferred to run-time repair)",
                }
            ],
        }


def _normalize_string_list(value: Any, *, limit: int) -> list[str]:
    if not isinstance(value, list):
        return []
    items: list[str] = []
    for item in value:
        text = str(item).strip()
        if not text:
            continue
        if text in items:
            continue
        items.append(text)
        if len(items) >= limit:
            break
    return items


def _inject_exact_match_example_hint(
    context: dict[str, Any],
    *,
    question: str,
    sql: str,
) -> dict[str, Any]:
    if not isinstance(context, dict):
        return context
    matched_question = str(question or "").strip()
    matched_sql = str(sql or "").strip()
    if not matched_question or not matched_sql:
        return context

    existing_examples = context.get("examples")
    examples = list(existing_examples) if isinstance(existing_examples, list) else []
    hint_signature = (
        _normalize_exact_match_question(matched_question),
        re.sub(r"\s+", " ", matched_sql).strip().lower(),
    )

    deduped: list[dict[str, Any]] = []
    for item in examples:
        if not isinstance(item, dict):
            continue
        item_text = str(item.get("text") or "")
        item_q = str((item.get("metadata") or {}).get("question") or "").strip()
        parsed_q = item_q
        parsed_sql = ""
        if not parsed_q and "Question:" in item_text and "SQL:" in item_text:
            m = re.search(r"Question:\s*(.*?)\s*SQL:\s*(.*)$", item_text, re.DOTALL)
            if m:
                parsed_q = m.group(1).strip()
                parsed_sql = m.group(2).strip()
        signature = (
            _normalize_exact_match_question(parsed_q),
            re.sub(r"\s+", " ", parsed_sql).strip().lower(),
        )
        if signature == hint_signature:
            continue
        deduped.append(item)

    hint = {
        "id": "example::exact_match_hint",
        "text": f"Question: {matched_question}\nSQL: {matched_sql}",
        "metadata": {
            "type": "example",
            "source": "sql_examples_exact_match_hint",
            "question": matched_question,
        },
        "score": 1.0,
    }
    context["examples"] = [hint, *deduped]
    return context


@dataclass
class _ContextPayloadPack:
    schemas: list[dict[str, Any]]
    examples: list[dict[str, Any]]
    templates: list[dict[str, Any]]
    glossary: list[dict[str, Any]]


def _trim_context_payload_to_budget(context: dict[str, Any], budget: int) -> dict[str, Any]:
    if not isinstance(context, dict):
        return context
    packed = _ContextPayloadPack(
        schemas=list(context.get("schemas") or []),
        examples=list(context.get("examples") or []),
        templates=list(context.get("templates") or []),
        glossary=list(context.get("glossary") or []),
    )
    trimmed = trim_context_to_budget(packed, budget)
    return {
        "schemas": list(getattr(trimmed, "schemas", [])),
        "examples": list(getattr(trimmed, "examples", [])),
        "templates": list(getattr(trimmed, "templates", [])),
        "glossary": list(getattr(trimmed, "glossary", [])),
    }


def _risk_input_text(question: str, question_en: str | None) -> str:
    base = str(question or "").strip()
    translated = str(question_en or "").strip()
    if not translated or translated == base:
        return base
    return f"{base}\n{translated}"


_QUESTION_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+|[가-힣]+")
_PLANNER_COMPLEX_SIGNAL_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        r"(연도별|월별|주별|일별|분기별|추이|시계열|사분위|백분위|비교|대비|전후|차이|에 따른|별로|그룹별|하위군)",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(vs|versus|compared?|comparison|trend|over\s+time|yearly|monthly|weekly|daily|quartile|q[1-4]|decile|percentile|stratif|subgroup)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(count|avg|average|sum|min|max|median)\b.*\bby\b|\bby\b.*\b(count|avg|average|sum|min|max|median)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\btop\s+\d+\b|상위\s*\d+|탑\s*\d+", re.IGNORECASE),
    re.compile(
        r"(수술\s*후|입원\s*후|\d+\s*(일|주|개월|월|년)\s*이내|after\s+\d+\s*(day|week|month|year)|within\s+\d+\s*(day|week|month|year)|between\s+\S+\s+and\s+\S+)",
        re.IGNORECASE,
    ),
)
_AGE_SEMANTIC_INTENT_RE = re.compile(
    r"(연령대|나이대|연령|나이|나잇대|세\b|aged?\b|age\s*(group|band|range)?\b)",
    re.IGNORECASE,
)
_AGE_GROUPING_INTENT_RE = re.compile(
    r"(연령대|나이대|연령별|나이별|연령\s*구간|나이\s*구간|age\s*(group|band|bucket|range)|by\s+age|age[-\s]*stratif)",
    re.IGNORECASE,
)
_YEAR_SEMANTIC_INTENT_RE = re.compile(
    r"(연도|년도|연도별|년별|year|yearly|annual|anchor[_\s]*year|anchor[_\s]*year[_\s]*group)",
    re.IGNORECASE,
)
_AGE_SEMANTIC_HINT = (
    "Age semantics requested: use PATIENTS.ANCHOR_AGE or explicit age bands; "
    "do not substitute ANCHOR_YEAR_GROUP unless year intent is explicit."
)


def _count_question_tokens(*texts: str | None) -> int:
    total = 0
    for text in texts:
        if not text:
            continue
        total += len(_QUESTION_TOKEN_RE.findall(text))
    return total


def _count_planner_complex_signals(*texts: str | None) -> int:
    merged = " ".join(str(text).strip() for text in texts if str(text or "").strip())
    if not merged:
        return 0
    return sum(1 for pattern in _PLANNER_COMPLEX_SIGNAL_PATTERNS if pattern.search(merged))


def _prefer_anchor_age_semantics(*texts: str | None) -> bool:
    merged = " ".join(str(text or "").strip() for text in texts if str(text or "").strip())
    if not merged:
        return False
    has_age_intent = bool(_AGE_SEMANTIC_INTENT_RE.search(merged))
    has_year_intent = bool(_YEAR_SEMANTIC_INTENT_RE.search(merged))
    return has_age_intent and not has_year_intent


def _has_age_grouping_intent(*texts: str | None) -> bool:
    merged = " ".join(str(text or "").strip() for text in texts if str(text or "").strip())
    if not merged:
        return False
    return bool(_AGE_GROUPING_INTENT_RE.search(merged))


def _apply_anchor_age_semantic_hint(
    *,
    question: str,
    question_en: str | None,
    planner_intent: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, bool]:
    if not _prefer_anchor_age_semantics(question, question_en):
        return planner_intent, False

    # Ensure SQL generator receives a concrete semantic constraint even when planner is skipped.
    intent = dict(planner_intent) if isinstance(planner_intent, dict) else {}
    filters_raw = intent.get("filters")
    filters: list[str] = list(filters_raw) if isinstance(filters_raw, list) else []
    if _AGE_SEMANTIC_HINT not in filters:
        filters.append(_AGE_SEMANTIC_HINT)
    intent["filters"] = filters

    # Do not force age-group grain for pure age-filter requests
    # (e.g., "65세 이상 환자 수"). Only set grain when grouping intent is explicit.
    if _has_age_grouping_intent(question, question_en):
        grain = str(intent.get("grain") or "").strip()
        if not grain or _YEAR_SEMANTIC_INTENT_RE.search(grain):
            intent["grain"] = "age_group"
    summary = str(intent.get("intent_summary") or "").strip()
    if _AGE_SEMANTIC_HINT not in summary:
        intent["intent_summary"] = f"{summary} {_AGE_SEMANTIC_HINT}".strip()
    return intent, True


def _decide_planner_usage(
    settings: Any,
    *,
    question: str,
    question_en: str | None,
    risk_info: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    mode = str(getattr(settings, "planner_activation_mode", "complex_only") or "complex_only").strip().lower()
    if not bool(getattr(settings, "planner_enabled", False)):
        return False, {"enabled": False, "mode": mode, "reason": "planner_disabled"}
    if mode in {"off", "false", "never", "disabled"}:
        return False, {"enabled": False, "mode": mode, "reason": "activation_mode_off"}
    if mode in {"always", "on", "all"}:
        return True, {"enabled": True, "mode": mode, "reason": "activation_mode_always"}

    # default: complex_only
    # Run planner only when complexity signals are present.
    # Keep signal telemetry even when planner is skipped.
    mode = "complex_only"
    token_count = _count_question_tokens(question, question_en)
    signal_hits = _count_planner_complex_signals(question, question_en)
    complexity_score = int(risk_info.get("complexity") or 0)
    complexity_threshold = max(0, int(getattr(settings, "planner_complexity_threshold", 1)))
    min_question_tokens = max(1, int(getattr(settings, "planner_min_question_tokens", 16)))
    effective_complexity_threshold = max(complexity_threshold, 3)

    has_complex_signal = signal_hits > 0
    has_risk_complexity = complexity_score >= effective_complexity_threshold
    has_long_question = token_count >= min_question_tokens
    gate_count = int(has_complex_signal) + int(has_risk_complexity) + int(has_long_question)

    reasons: list[str] = []
    if has_complex_signal:
        reasons.append("complex_signal")
    if has_risk_complexity:
        reasons.append("risk_complexity")
    if has_long_question:
        reasons.append("long_question")

    required_gate_count = max(1, min(3, int(getattr(settings, "planner_required_gate_count", 2))))
    should_run_planner = gate_count >= required_gate_count
    return should_run_planner, {
        "enabled": should_run_planner,
        "mode": mode,
        "reason": ",".join(reasons) if reasons else "complexity_gate_not_met",
        "token_count": token_count,
        "signal_hits": signal_hits,
        "risk_complexity": complexity_score,
        "complexity_threshold": complexity_threshold,
        "effective_complexity_threshold": effective_complexity_threshold,
        "min_question_tokens": min_question_tokens,
        "gate_count": gate_count,
        "required_gate_count": required_gate_count,
    }


def _should_apply_expert_review(settings: Any, risk_info: dict[str, Any]) -> bool:
    mode = str(getattr(settings, "expert_trigger_mode", "score") or "score").strip().lower()
    if mode in {"off", "false", "disabled"}:
        return False
    if mode in {"always", "on", "all"}:
        return True

    risk_score = int(risk_info.get("risk") or 0)
    complexity_score = int(risk_info.get("complexity") or 0)
    threshold = max(0, int(getattr(settings, "expert_score_threshold", 0)))
    if risk_score >= threshold:
        return True
    # Analytical prompts can be low-risk but still need an expert pass.
    return complexity_score >= max(2, threshold - 2)

_ASCII_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_/-]*")
_MULTI_SPACE_RE = re.compile(r"\s+")
_CLARIFICATION_SLOT_ORDER = ("period", "cohort", "comparison", "metric")
_CURRENT_CALENDAR_YEAR_KO = f"{datetime.now().year}년 전체"
_CURRENT_CALENDAR_YEAR_EN = f"Calendar year {datetime.now().year}"
_SLOT_LABELS_KO = {
    "period": "기간",
    "cohort": "대상 환자",
    "comparison": "비교 기준",
    "metric": "지표",
}
_SLOT_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "period": (
        re.compile(r"(최근|지난|작년|올해|전년|전년도|이번)\s*\d*\s*(일|주|개월|달|월|년)?"),
        re.compile(r"(월별|연도별|주별|일별|기간|time|date|month|year|week|day)", re.IGNORECASE),
        re.compile(r"(between|from|to)\s+\S+", re.IGNORECASE),
    ),
    "cohort": (
        re.compile(r"(환자|코호트|대상|집단|남성|여성|성별|연령|세\s*이상|세\s*이하|진단|질환)"),
        re.compile(r"(icu|입원|외래|subject|cohort|group|population|diagnos|disease)", re.IGNORECASE),
    ),
    "comparison": (
        re.compile(r"(비교|대비|전후|차이|증감|군간|대조군)"),
        re.compile(r"(vs|versus|comparison|compared|before|after)", re.IGNORECASE),
    ),
    "metric": (
        re.compile(r"(사망률|사망|생존율|생존|재입원율|재입원|비율|건수|평균|중앙|중위|재원일수)"),
        re.compile(r"(rate|ratio|count|mean|median|mortality|survival|readmission|length\s+of\s+stay)", re.IGNORECASE),
    ),
}
_SLOT_OPTION_SAMPLES_KO: dict[str, tuple[str, ...]] = {
    "period": ("최근 30일", _CURRENT_CALENDAR_YEAR_KO, "입원 후 30일"),
    "cohort": ("전체 환자", "65세 이상 환자", "여성 환자"),
    "comparison": ("비교 없음", "남성 대 여성 비교", "연도별 비교"),
    "metric": ("사망률", "사망 건수", "생존율"),
}
_PERIOD_ONLY_REASON_KO = "질문에 기간 정보가 없어 데이터 범위를 먼저 정해야 합니다."
_PERIOD_ONLY_REASON_EN = "A time range is required before generating SQL."
_PERIOD_ONLY_QUESTION_KO = "어떤 기간으로 분석할까요?"
_PERIOD_ONLY_QUESTION_EN = "What time period should be used?"
_SLOT_LABEL_ALIASES: dict[str, tuple[str, ...]] = {
    "period": ("기간", "시간", "date", "time", "날짜"),
    "cohort": ("대상환자", "대상", "환자군", "코호트", "cohort", "population", "group"),
    "comparison": ("비교기준", "비교", "대조군", "비교군", "comparison"),
    "metric": ("지표", "메트릭", "metric", "결과지표", "평가지표"),
}
_METRIC_HINTS = (
    "사망률",
    "사망 건수",
    "사망",
    "생존율",
    "생존",
    "재입원율",
    "재입원",
    "재원일수",
    "비율",
    "건수",
    "평균",
    "중앙",
    "중위",
)
_TIME_GRAIN_PATTERN = re.compile(
    r"(연도별|월별|주별|일별|분기별|추이|시계열|by\s+year|by\s+month|by\s+week|by\s+day|yearly|monthly|weekly|daily|trend|over\s+time)",
    re.IGNORECASE,
)
_DEFINITION_AMBIGUITY_SIGNAL_KEYWORDS = (
    "정의",
    "기준",
    "판정",
    "분류",
    "criterion",
    "criteria",
    "definition",
    "define",
    "rule",
)
_DEFINITION_AMBIGUITY_RULES: tuple[dict[str, Any], ...] = (
    {
        "id": "hypertension_definition",
        "match_terms": ("고혈압", "hypertension", "htn"),
        "criteria_terms": (
            "i10",
            "i11",
            "i12",
            "i13",
            "i15",
            "icd",
            "진단코드",
            "진단 코드",
            "코드기반",
            "코드 기반",
            "항고혈압",
            "복용",
            "병력",
            "comorbidity",
            "history",
            "위기 제외",
            "hypertensive crisis",
        ),
        "reason_ko": "의학적 정의 기준이 여러 가지라 먼저 기준을 정해야 합니다.",
        "question_ko": "‘고혈압’을 어떤 기준으로 볼까요?",
        "options_ko": (
            "진단 코드 기반 (I10-I15)",
            "항고혈압제 복용 기준",
            "입실 전 병력(comorbidity)",
            "고혈압 위기 제외",
        ),
        "reason_en": "Multiple medical definitions are possible, so a definition criterion is required first.",
        "question_en": "How should hypertension be defined?",
        "options_en": (
            "Diagnosis-code based (I10-I15)",
            "Antihypertensive medication use",
            "Pre-admission comorbidity history",
            "Exclude hypertensive crisis",
        ),
    },
)


def _strip_english_tokens_for_korean(text: str) -> str:
    # 한국어 문장을 유지하면서 영문 토큰을 제거한다.
    cleaned = _ASCII_WORD_RE.sub("", text)
    cleaned = re.sub(r"\s*[:：]\s*", ": ", cleaned)
    cleaned = re.sub(r"\s*[/|]\s*", " / ", cleaned)
    cleaned = _MULTI_SPACE_RE.sub(" ", cleaned).strip(" ,;")
    return cleaned.strip()


def _normalize_conversation(conversation: list[dict[str, Any]] | None) -> list[dict[str, str]]:
    if not conversation:
        return []
    normalized: list[dict[str, str]] = []
    for turn in conversation[-20:]:
        role = str(turn.get("role") or "").strip().lower()
        content = str(turn.get("content") or "").strip()
        if role not in {"user", "assistant"} or not content:
            continue
        normalized.append({"role": role, "content": content[:2000]})
    return normalized


def _looks_like_followup_question(question: str) -> bool:
    text = _MULTI_SPACE_RE.sub(" ", str(question or "").strip())
    if not text:
        return False
    if _FOLLOWUP_CUE_RE.search(text):
        return True
    lowered = text.lower()
    if len(lowered) <= 24 and lowered in {
        "그럼",
        "then",
        "and then",
        "what about this",
        "what about that",
    }:
        return True
    if len(lowered) <= 48 and re.match(r"^(그럼|그 조건|해당 조건|then|what about)\b", lowered):
        return True
    return False


def _previous_user_question(
    question: str,
    conversation: list[dict[str, Any]] | None,
) -> str:
    turns = _normalize_conversation(conversation)
    if not turns:
        return ""
    current = _MULTI_SPACE_RE.sub(" ", str(question or "").strip())
    seen_current = False
    for turn in reversed(turns):
        if turn.get("role") != "user":
            continue
        text = _MULTI_SPACE_RE.sub(" ", str(turn.get("content") or "").strip())
        if not text:
            continue
        if not seen_current and text == current:
            seen_current = True
            continue
        if text != current:
            return text
    return ""


def _inject_followup_context(
    question: str,
    conversation: list[dict[str, Any]] | None,
) -> str:
    q = str(question or "").strip()
    if not q:
        return q
    if not _looks_like_followup_question(q):
        return q
    previous = _previous_user_question(q, conversation)
    if not previous:
        return q
    tag = "[후속 질문]" if contains_korean(q) else "[follow-up]"
    merged = f"{previous}\n{tag} {q}".strip()
    return merged if merged else q


def _is_clarification_prompt_text(text: str) -> bool:
    normalized = _MULTI_SPACE_RE.sub(" ", text).strip().lower()
    if not normalized:
        return False
    hints = (
        "추가로 아래 항목",
        "답변 예시",
        "질문 범위를 조금 더 좁혀",
        "질문 범위를 조금 더",
        "기간을 알려",
        "기간으로 분석",
        "clarify",
        "clarification",
        "what time period should be used",
    )
    return any(hint in normalized for hint in hints)


def _slice_active_clarification_turns(turns: list[dict[str, str]]) -> list[dict[str, str]]:
    if not turns:
        return []

    clarify_indices = [
        idx
        for idx, turn in enumerate(turns)
        if turn["role"] == "assistant" and _is_clarification_prompt_text(turn["content"])
    ]

    start_idx = 0
    if clarify_indices:
        chain_start = clarify_indices[-1]
        while True:
            prev_idx = next((idx for idx in reversed(clarify_indices) if idx < chain_start), None)
            if prev_idx is None:
                break
            has_non_clarify_assistant = any(
                turns[pos]["role"] == "assistant" and not _is_clarification_prompt_text(turns[pos]["content"])
                for pos in range(prev_idx + 1, chain_start)
            )
            if has_non_clarify_assistant:
                break
            chain_start = prev_idx

        base_idx = next((idx for idx in range(chain_start - 1, -1, -1) if turns[idx]["role"] == "user"), None)
        start_idx = base_idx if base_idx is not None else chain_start
    else:
        latest_user_idx = next((idx for idx in range(len(turns) - 1, -1, -1) if turns[idx]["role"] == "user"), None)
        start_idx = latest_user_idx if latest_user_idx is not None else 0

    return turns[start_idx:]


def _extract_requested_slots_from_assistant(text: str) -> set[str]:
    if not text:
        return set()

    requested: set[str] = set()
    in_requested_section = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "추가로 아래 항목" in line:
            in_requested_section = True
            continue
        if not in_requested_section:
            continue
        if line.startswith("답변 예시") or line.startswith("선택 예시") or line.startswith("이유"):
            break
        bullet = re.match(r"^[-•]\s*(.+)$", line)
        if not bullet:
            continue
        slot = _slot_from_label(bullet.group(1))
        if slot:
            requested.add(slot)

    if requested:
        return requested
    return _extract_slots_from_text(text)


def _extract_slots_from_text(text: str) -> set[str]:
    if not text:
        return set()
    found: set[str] = set()
    for slot, patterns in _SLOT_PATTERNS.items():
        for pattern in patterns:
            if pattern.search(text):
                found.add(slot)
                break
    return found


def _has_time_grain_intent(text: str) -> bool:
    if not text:
        return False
    return bool(_TIME_GRAIN_PATTERN.search(text))


def _contains_any_term(text: str, terms: tuple[str, ...]) -> bool:
    lowered = text.lower()
    compact = re.sub(r"\s+", "", lowered)
    for term in terms:
        token = str(term).strip().lower()
        if not token:
            continue
        if token in lowered or token.replace(" ", "") in compact:
            return True
    return False


def _detect_definition_ambiguity_rule(question: str) -> dict[str, Any] | None:
    if not question.strip():
        return None
    for rule in _DEFINITION_AMBIGUITY_RULES:
        match_terms = tuple(rule.get("match_terms") or ())
        criteria_terms = tuple(rule.get("criteria_terms") or ())
        if not _contains_any_term(question, match_terms):
            continue
        if _contains_any_term(question, criteria_terms):
            continue
        return rule
    return None


def _is_definition_clarification_signal(
    *,
    reason: str,
    clarification_question: str,
    options: list[str],
    example_inputs: list[str],
) -> bool:
    merged = " ".join([reason, clarification_question, *options, *example_inputs]).strip()
    if not merged:
        return False
    return _contains_any_term(merged, _DEFINITION_AMBIGUITY_SIGNAL_KEYWORDS)


def _build_default_scope(
    *,
    base_question: str,
    raw_answers: dict[str, str],
) -> dict[str, Any]:
    period_answer = raw_answers.get("period", "")
    cohort_answer = raw_answers.get("cohort", "")

    has_period_answer = bool(period_answer and _is_specific_slot_answer("period", period_answer))
    has_cohort_answer = bool(cohort_answer and _is_specific_slot_answer("cohort", cohort_answer))
    has_time_grain = _has_time_grain_intent(base_question)

    defaults: dict[str, Any] = {"applied": False}
    is_korean = contains_korean(base_question)

    if not has_period_answer and not has_time_grain:
        defaults["period"] = "전체 기간" if is_korean else "full period"
    if not has_cohort_answer:
        defaults["cohort"] = "전체 환자" if is_korean else "all patients"

    if "period" in defaults or "cohort" in defaults:
        defaults["applied"] = True
        base = base_question.strip()
        if is_korean:
            defaults["message"] = (
                f"전체 데이터에 대한 '{base}' 질문의 결과를 반환하였습니다."
                if base
                else "전체 데이터에 대한 질문의 결과를 반환하였습니다."
            )
        else:
            defaults["message"] = (
                f"Returned results for '{base}' on the full dataset."
                if base
                else "Returned results on the full dataset."
            )
    return defaults


def _inject_default_scope_into_question(question: str, default_scope: dict[str, Any]) -> str:
    if not bool(default_scope.get("applied", False)):
        return question
    period = str(default_scope.get("period") or "").strip()
    cohort = str(default_scope.get("cohort") or "").strip()
    if not period and not cohort:
        return question

    is_korean = contains_korean(question)
    parts: list[str] = []
    if period:
        parts.append(f"{'기간' if is_korean else 'period'}: {period}")
    if cohort:
        parts.append(f"{'대상 환자' if is_korean else 'cohort'}: {cohort}")

    if not parts:
        return question

    suffix = " / ".join(parts)
    base = question.strip()
    if suffix in base:
        return base
    return f"{base} ({suffix})" if base else suffix


def _truncate_slot_answer(text: str, *, limit: int = 80) -> str:
    normalized = _MULTI_SPACE_RE.sub(" ", text).strip()
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[: limit - 1].rstrip()}…"


def _normalize_slot_label(label: str) -> str:
    return re.sub(r"\s+", "", label).strip().lower()


def _slot_from_label(label: str) -> str | None:
    normalized = _normalize_slot_label(label)
    if not normalized:
        return None
    for slot, aliases in _SLOT_LABEL_ALIASES.items():
        for alias in aliases:
            alias_normalized = _normalize_slot_label(alias)
            if normalized == alias_normalized or normalized.startswith(alias_normalized):
                return slot
    return None


def _push_slot_answer(slot_answers: dict[str, list[str]], slot: str, value: str) -> None:
    normalized = _truncate_slot_answer(value)
    if not normalized:
        return
    if normalized not in slot_answers[slot]:
        slot_answers[slot].append(normalized)


def _extract_labeled_slot_values(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    chunks = [chunk.strip() for chunk in re.split(r"[\n/]+", text) if chunk.strip()]
    for chunk in chunks:
        match = re.match(r"^([^:：]{1,20})\s*[:：]\s*(.+)$", chunk)
        if not match:
            continue
        slot = _slot_from_label(match.group(1))
        if not slot:
            continue
        value = match.group(2).strip()
        if value:
            values[slot] = value
    return values


def _extract_slot_value_from_free_text(slot: str, text: str) -> str:
    compact = _MULTI_SPACE_RE.sub(" ", text).strip(" .")
    lowered = compact.lower()
    if not compact:
        return ""

    if slot == "period":
        if compact in {"전체", "전체 기간", "전체기간"}:
            return "전체"
        match = re.search(
            r"((최근|지난|작년|올해|전년|전년도|이번)\s*\d*\s*(일|주|개월|달|월|년)?)",
            compact,
        )
        if match:
            return match.group(1).strip()
        return ""

    if slot == "comparison":
        if any(keyword in lowered for keyword in ("비교 없음", "없음", "없다", "no comparison", "none")):
            return "비교 없음"
        vs_match = re.search(
            r"([가-힣A-Za-z0-9_]+)\s*(?:vs|VS|대)\s*([가-힣A-Za-z0-9_]+)",
            compact,
        )
        if vs_match:
            left = vs_match.group(1).strip()
            right = vs_match.group(2).strip()
            if left and right:
                return f"{left} vs {right}"
        compare_match = re.search(r"([가-힣A-Za-z0-9\s]{2,20}비교)", compact)
        if compare_match:
            return compare_match.group(1).strip()
        return ""

    if slot == "metric":
        for hint in _METRIC_HINTS:
            if hint in compact:
                if "icu" in lowered and not hint.lower().startswith("icu"):
                    return f"ICU {hint}"
                return hint
        return ""

    if slot == "cohort":
        if compact in {"전체 환자", "전체"}:
            return "전체 환자"
        age_match = re.search(r"(\d+\s*세\s*(이상|이하))", compact)
        if age_match:
            return f"{age_match.group(1).strip()} 환자"
        if "icu" in lowered or "중환자" in compact:
            return "ICU 환자"
        if "입원" in compact:
            return "입원 환자"
        if "외래" in compact:
            return "외래 환자"
        sex_patient_match = re.search(r"(남성|여성)\s*환자", compact)
        if sex_patient_match:
            return sex_patient_match.group(0).strip()
        simple_patient_match = re.search(r"([가-힣A-Za-z0-9\s]{1,18}환자)", compact)
        if simple_patient_match and "vs" not in lowered and "비교" not in compact:
            candidate = simple_patient_match.group(1).strip()
            if candidate:
                return candidate
        return ""

    return ""


def _is_specific_slot_answer(slot: str, value: str) -> bool:
    normalized = value.strip().lower()
    if not normalized:
        return False
    if slot == "comparison":
        if any(keyword in normalized for keyword in ("비교 없음", "없음", "없다", "no comparison", "none")):
            return True
        return slot in _extract_slots_from_text(value)
    if slot == "metric":
        return slot in _extract_slots_from_text(value)
    if slot == "period":
        if normalized in {"전체", "전체기간", "all", "all period"}:
            return True
        return slot in _extract_slots_from_text(value)
    if slot == "cohort":
        if any(hint in normalized for hint in _METRIC_HINTS):
            return False
        if " vs " in normalized or "비교" in normalized:
            return False
        if normalized in {"전체", "전체 환자", "all", "all patients"}:
            return True
        if any(
            keyword in normalized
            for keyword in (
                "남성",
                "여성",
                "연령",
                "세",
                "이상",
                "이하",
                "진단",
                "질환",
                "icu",
                "입원",
                "외래",
                "중환자",
                "male",
                "female",
                "age",
                "diagnos",
                "disease",
            )
        ):
            return True
        return "환자" in normalized and len(normalized) <= 12
    return bool(normalized)


def _collect_clarification_memory(
    question: str,
    conversation: list[dict[str, Any]] | None,
) -> tuple[str, set[str], dict[str, str]]:
    turns = _normalize_conversation(conversation)
    if question.strip():
        if not turns or turns[-1]["role"] != "user" or turns[-1]["content"] != question.strip():
            turns.append({"role": "user", "content": question.strip()})

    scoped_turns = _slice_active_clarification_turns(turns)

    base_question = question.strip()
    for turn in scoped_turns:
        if turn["role"] == "user":
            base_question = turn["content"]
            break

    asked_slots: set[str] = set()
    slot_answers: dict[str, list[str]] = {slot: [] for slot in _CLARIFICATION_SLOT_ORDER}
    pending_slots: set[str] = set()

    for turn in scoped_turns:
        text = turn["content"].strip()
        if not text:
            continue
        if turn["role"] == "assistant":
            pending_slots = _extract_requested_slots_from_assistant(text)
            asked_slots.update(pending_slots)
            continue

        assigned_slots: set[str] = set()
        labeled_values = _extract_labeled_slot_values(text)
        for slot, value in labeled_values.items():
            _push_slot_answer(slot_answers, slot, value)
            assigned_slots.add(slot)

        detected_slots = _extract_slots_from_text(text)
        for slot in detected_slots:
            if slot in assigned_slots:
                continue
            candidate = _extract_slot_value_from_free_text(slot, text)
            if candidate:
                _push_slot_answer(slot_answers, slot, candidate)
                assigned_slots.add(slot)

        if pending_slots:
            pending_list = [slot for slot in _CLARIFICATION_SLOT_ORDER if slot in pending_slots]
            if len(pending_list) == 1:
                slot = pending_list[0]
                if slot not in assigned_slots:
                    candidate = _extract_slot_value_from_free_text(slot, text)
                    if not candidate and not detected_slots and not labeled_values:
                        candidate = text
                    if candidate:
                        _push_slot_answer(slot_answers, slot, candidate)
            else:
                for slot in pending_list:
                    if slot in assigned_slots:
                        continue
                    candidate = _extract_slot_value_from_free_text(slot, text)
                    if candidate and _is_specific_slot_answer(slot, candidate):
                        _push_slot_answer(slot_answers, slot, candidate)
        pending_slots = set()

    latest_answers: dict[str, str] = {}
    for slot in _CLARIFICATION_SLOT_ORDER:
        values = slot_answers.get(slot) or []
        if not values:
            continue
        latest_answers[slot] = _truncate_slot_answer(values[-1])
    return base_question, asked_slots, latest_answers


def _infer_required_slots(
    *,
    question: str,
    reason: str,
    clarification_question: str,
    options: list[str],
    example_inputs: list[str],
    asked_slots: set[str],
) -> list[str]:
    required = set(asked_slots)
    for text in [question, reason, clarification_question, *options, *example_inputs]:
        required.update(_extract_slots_from_text(text))
    if not required:
        required.update({"cohort", "metric"})
    return [slot for slot in _CLARIFICATION_SLOT_ORDER if slot in required]


def _build_korean_examples(
    *,
    missing_slots: list[str],
) -> list[str]:
    if not missing_slots:
        return []
    if len(missing_slots) == 1:
        slot = missing_slots[0]
        sample_values = _SLOT_OPTION_SAMPLES_KO.get(slot) or ()
        return [item for item in sample_values[:3] if item]
    examples: list[str] = []
    for idx in range(2):
        parts: list[str] = []
        for slot in missing_slots:
            label = _SLOT_LABELS_KO.get(slot, slot)
            sample_values = _SLOT_OPTION_SAMPLES_KO.get(slot) or ()
            if sample_values:
                parts.append(f"{label}: {sample_values[idx % len(sample_values)]}")
        if parts:
            examples.append(" / ".join(parts))
    return examples[:3]


def _build_korean_consolidated_clarification(
    *,
    question: str,
    reason: str,
    clarification_question: str,
    options: list[str],
    example_inputs: list[str],
    conversation: list[dict[str, Any]] | None,
) -> tuple[str, list[str], list[str], dict[str, str], list[str], list[str], str]:
    base_question, asked_slots, raw_answers = _collect_clarification_memory(question, conversation)
    required_slots = _infer_required_slots(
        question=base_question,
        reason=reason,
        clarification_question=clarification_question,
        options=options,
        example_inputs=example_inputs,
        asked_slots=asked_slots,
    )
    if not required_slots:
        required_slots = ["period", "cohort", "metric"]

    known_answers: dict[str, str] = {}
    for slot in required_slots:
        value = raw_answers.get(slot)
        if value and _is_specific_slot_answer(slot, value):
            known_answers[slot] = value
    # 연도별/월별/추이 같은 질문은 기간을 전체로 기본 해석해 추가 질문을 생략한다.
    if "period" in required_slots and "period" not in known_answers and _has_time_grain_intent(base_question):
        known_answers["period"] = "전체"

    missing_slots = [slot for slot in required_slots if slot not in known_answers]

    lines: list[str] = []
    if known_answers:
        lines.append("현재까지 답변 정리:")
        for slot in required_slots:
            value = known_answers.get(slot)
            if not value:
                continue
            lines.append(f"- {_SLOT_LABELS_KO[slot]}: {value}")
    lines.append("추가로 아래 항목을 한 번에 알려주세요.")
    for slot in missing_slots:
        lines.append(f"- {_SLOT_LABELS_KO[slot]}")

    option_candidates: list[str] = []
    for slot in missing_slots:
        option_candidates.extend(_SLOT_OPTION_SAMPLES_KO.get(slot, ()))
    dedup_options: list[str] = []
    for item in option_candidates:
        if item not in dedup_options:
            dedup_options.append(item)
    examples = _build_korean_examples(
        missing_slots=missing_slots,
    )
    if not examples:
        if len(missing_slots) == 1:
            examples = [item for item in dedup_options[:3] if item]
        else:
            examples = [item for item in example_inputs if item][:3]

    return (
        "\n".join(lines),
        dedup_options[:5],
        examples[:3],
        known_answers,
        required_slots,
        missing_slots,
        base_question,
    )


def _compose_refined_question(
    *,
    base_question: str,
    required_slots: list[str],
    known_answers: dict[str, str],
) -> str:
    details = [
        f"{_SLOT_LABELS_KO[slot]}: {known_answers[slot]}"
        for slot in required_slots
        if slot in known_answers and known_answers[slot]
    ]
    if not details:
        return base_question.strip()
    suffix = " / ".join(details)
    prefix = base_question.strip()
    if not prefix:
        return suffix
    if suffix in prefix:
        return prefix
    return f"{prefix} ({suffix})"


def _default_korean_clarification(question: str) -> tuple[str, list[str], list[str]]:
    q = question.lower()
    if "약" in question or "med" in q or "drug" in q or "medication" in q:
        reason = "요청 범위가 넓어 약물군 또는 진료 영역을 먼저 좁혀야 합니다."
        options = ["심혈관 약물", "정신과 약물", "항생제", "모든 약물"]
        examples = [
            "심혈관 약물에 대한 정보가 필요해요",
            "정신과 약물로 좁혀주세요",
            "항생제만 보여주세요",
        ]
        return reason, options, examples
    reason = "질문 범위를 조금 더 좁혀야 정확한 SQL을 만들 수 있습니다."
    options = ["기간을 지정", "대상 집단을 지정", "지표를 지정"]
    examples = [
        "최근 1년 데이터로 보여주세요",
        "65세 이상 환자로 제한해 주세요",
        "재입원율 기준으로만 알려주세요",
    ]
    return reason, options, examples


def _normalize_clarifier_payload(
    payload: dict[str, Any],
    question: str,
    *,
    conversation: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    base_question, _, raw_answers = _collect_clarification_memory(question, conversation)
    resolved_question = base_question or question
    settings = get_settings()
    if bool(getattr(settings, "default_scope_autofill_enabled", False)):
        default_scope = _build_default_scope(base_question=resolved_question, raw_answers=raw_answers)
    else:
        default_scope = {"applied": False}
    ambiguity_rule = _detect_definition_ambiguity_rule(resolved_question)

    need_clarification = bool(payload.get("need_clarification"))
    refined_question = str(payload.get("refined_question") or "").strip()
    reason = str(payload.get("reason") or "").strip()
    clarification_question = str(payload.get("clarification_question") or "").strip()
    if need_clarification and not clarification_question:
        clarification_question = "질문 범위를 조금 더 좁혀주세요."

    options = _normalize_string_list(payload.get("options"), limit=5)
    example_inputs = _normalize_string_list(payload.get("example_inputs"), limit=3)
    known_answers: dict[str, str] = {}

    if ambiguity_rule is not None:
        if contains_korean(question):
            options_ko = [str(item).strip() for item in ambiguity_rule.get("options_ko", ()) if str(item).strip()]
            return {
                "need_clarification": True,
                "reason": str(ambiguity_rule.get("reason_ko") or "의학적 정의 기준이 모호합니다."),
                "clarification_question": str(ambiguity_rule.get("question_ko") or "어떤 정의 기준으로 볼까요?"),
                "options": options_ko[:5],
                "example_inputs": options_ko[:3],
                "known_answers": known_answers,
                "refined_question": "",
                "default_scope": {"applied": False},
                "usage": payload.get("usage", {}),
            }
        options_en = [str(item).strip() for item in ambiguity_rule.get("options_en", ()) if str(item).strip()]
        return {
            "need_clarification": True,
            "reason": str(
                ambiguity_rule.get("reason_en")
                or "Clinical definition ambiguity detected."
            ),
            "clarification_question": str(
                ambiguity_rule.get("question_en") or "Which definition criterion should be used?"
            ),
            "options": options_en[:5],
            "example_inputs": options_en[:3],
            "known_answers": known_answers,
            "refined_question": "",
            "default_scope": {"applied": False},
            "usage": payload.get("usage", {}),
        }

    if contains_korean(question):
        reason = _strip_english_tokens_for_korean(reason)
        clarification_question = _strip_english_tokens_for_korean(clarification_question)
        options = [_strip_english_tokens_for_korean(item) for item in options]
        options = [item for item in options if item]
        example_inputs = [_strip_english_tokens_for_korean(item) for item in example_inputs]
        example_inputs = [item for item in example_inputs if item]

    definition_signal = _is_definition_clarification_signal(
        reason=reason,
        clarification_question=clarification_question,
        options=options,
        example_inputs=example_inputs,
    )
    if need_clarification and not definition_signal:
        # Accept non-definition clarifications if payload is still actionable.
        has_actionable_prompt = bool(
            clarification_question.strip()
            or options
            or example_inputs
        )
        if not has_actionable_prompt:
            need_clarification = False
    if need_clarification:
        if not reason:
            reason = (
                "의학적 정의 기준이 모호합니다."
                if contains_korean(question)
                else "Clinical definition is ambiguous."
            )
        if not clarification_question:
            clarification_question = (
                "어떤 정의 기준으로 볼까요?"
                if contains_korean(question)
                else "Which definition criterion should be used?"
            )
        if not options:
            options = (
                ["진단 코드 기준", "약물 기준", "병력 기준"]
                if contains_korean(question)
                else ["Diagnosis-code based", "Medication based", "History based"]
            )
        if not example_inputs:
            example_inputs = options[:3]
        default_scope = {"applied": False}
    else:
        reason = ""
        clarification_question = ""
        options = []
        example_inputs = []
        if not refined_question:
            refined_question = resolved_question

    return {
        "need_clarification": need_clarification,
        "reason": reason,
        "clarification_question": clarification_question,
        "options": options,
        "example_inputs": example_inputs,
        "known_answers": known_answers,
        "refined_question": refined_question,
        "default_scope": default_scope,
        "usage": payload.get("usage", {}),
    }


def run_oneshot(
    question: str,
    *,
    skip_policy: bool = False,
    translate: bool | None = None,
    rag_multi: bool | None = None,
    conversation: list[dict[str, Any]] | None = None,
    enable_clarification: bool = False,
) -> dict[str, Any]:
    settings = get_settings()
    original_question = question
    question = _inject_followup_context(question, conversation)
    translated_question = None
    scope_assumptions: dict[str, Any] = {"applied": False}
    use_translate = settings.translate_ko_to_en if translate is None else translate
    use_rag_multi = settings.rag_multi_query if rag_multi is None else rag_multi
    oneshot_postprocess_enabled = bool(getattr(settings, "oneshot_postprocess_enabled", False))
    oneshot_intent_guard_enabled = bool(getattr(settings, "oneshot_intent_guard_enabled", False))
    oneshot_intent_realign_enabled = bool(getattr(settings, "oneshot_intent_realign_enabled", False))

    if settings.demo_mode or settings.demo_cache_always:
        cache = _load_demo_cache(settings.demo_cache_path)
        cached = _lookup_demo_cache(cache, question)
        if cached:
            return cached

    if enable_clarification:
        clarity: dict[str, Any] = {
            "need_clarification": False,
            "reason": "",
            "clarification_question": "",
            "options": [],
            "example_inputs": [],
            "refined_question": "",
            "usage": {},
        }
        try:
            clarity_raw = evaluate_question_clarity(original_question, conversation=conversation)
            clarity = _normalize_clarifier_payload(
                clarity_raw,
                original_question,
                conversation=conversation,
            )
            _add_llm_cost(clarity.get("usage", {}), "clarify")
        except Exception:
            clarity = {**clarity, "need_clarification": False}
        if clarity["need_clarification"]:
            return {
                "mode": "clarify",
                "question": original_question,
                "clarification": {
                    "reason": clarity.get("reason"),
                    "question": clarity.get("clarification_question"),
                    "options": clarity.get("options", []),
                    "example_inputs": clarity.get("example_inputs", []),
                    "known_answers": clarity.get("known_answers", {}),
                },
            }
        scope_candidate = clarity.get("default_scope")
        if isinstance(scope_candidate, dict):
            scope_assumptions = scope_candidate

        question_changed = False
        refined = str(clarity.get("refined_question") or "").strip()
        if refined:
            original_question = refined
            if question != refined:
                question_changed = True
            question = refined
        if bool(scope_assumptions.get("applied", False)) and bool(
            getattr(settings, "default_scope_autofill_enabled", False)
        ):
            scoped_question = _inject_default_scope_into_question(question, scope_assumptions)
            if scoped_question != question:
                question_changed = True
            question = scoped_question
        if question_changed and (settings.demo_mode or settings.demo_cache_always):
            cache = _load_demo_cache(settings.demo_cache_path)
            cached = _lookup_demo_cache(cache, question)
            if cached:
                return cached

    if use_translate and contains_korean(question):
        try:
            translated_question, usage = translate_to_english(question)
            _add_llm_cost(usage, "translate")
            if not translated_question:
                translated_question = None
        except Exception:
            translated_question = None

    if (settings.demo_mode or settings.demo_cache_always) and translated_question:
        cache = _load_demo_cache(settings.demo_cache_path)
        cached = _lookup_demo_cache(cache, translated_question) or _lookup_demo_cache(cache, question)
        if cached:
            cached["question"] = original_question
            cached["question_en"] = translated_question
            return cached

    exact_match_mode = str(getattr(settings, "sql_examples_exact_match_mode", "off") or "off").strip().lower()
    if exact_match_mode not in {"off", "hint", "short_circuit"}:
        exact_match_mode = "off"
    matched_example_hint: dict[str, str] | None = None
    matched_example = (
        _lookup_sql_examples_exact(question, translated_question)
        if exact_match_mode != "off"
        else None
    )
    if isinstance(matched_example, dict):
        matched_sql = str(matched_example.get("sql") or "").strip()
        matched_question = str(matched_example.get("question") or "").strip()
        if matched_sql and exact_match_mode == "short_circuit":
            risk_info = classify(_risk_input_text(question, translated_question))
            final_payload: dict[str, Any] = {
                "final_sql": matched_sql,
                "used_tables": [],
                "risk_score": int(risk_info.get("risk") or 0),
                "source": "sql_examples_exact_match",
                "matched_question": matched_question,
            }
            if not skip_policy and oneshot_postprocess_enabled:
                profile, profile_reasons = recommend_postprocess_profile(
                    question,
                    matched_sql,
                    default_profile="relaxed",
                )
                matched_sql, rules = postprocess_sql(question, matched_sql, profile=profile)
                final_payload["final_sql"] = matched_sql
                if rules:
                    final_payload["postprocess"] = rules
                if profile_reasons:
                    final_payload["postprocess_profile"] = profile
                    final_payload["postprocess_profile_reasons"] = profile_reasons
            unresolved_issues: list[str] = []
            if oneshot_intent_guard_enabled:
                aligned_sql, alignment_rules, unresolved_issues = enforce_intent_alignment(
                    question,
                    matched_sql,
                    planner_intent=None,
                )
                if aligned_sql.strip() != matched_sql.strip():
                    matched_sql = aligned_sql
                    final_payload["final_sql"] = matched_sql
                    if alignment_rules:
                        existing = final_payload.get("postprocess")
                        base_rules = list(existing) if isinstance(existing, list) else []
                        for rule in alignment_rules:
                            if rule not in base_rules:
                                base_rules.append(rule)
                        final_payload["postprocess"] = base_rules
            if unresolved_issues:
                final_payload["intent_alignment_issues"] = unresolved_issues
            policy_result = None
            if not skip_policy and matched_sql:
                policy_result = _precheck_oneshot_sql(matched_sql, question)
            return {
                "mode": "advanced",
                "question": original_question,
                "question_en": translated_question if translated_question else None,
                "assumptions": scope_assumptions if scope_assumptions.get("applied") else None,
                "planner": None,
                "planner_decision": {
                    "enabled": False,
                    "mode": "off",
                    "reason": "sql_examples_exact_match",
                },
                "risk": risk_info,
                "policy": policy_result,
                "context": {"schemas": [], "examples": [], "templates": [], "glossary": []},
                "draft": dict(final_payload),
                "final": final_payload,
            }
        if matched_sql and matched_question:
            matched_example_hint = {"question": matched_question, "sql": matched_sql}

    risk_info = classify(_risk_input_text(question, translated_question))
    if translated_question and use_rag_multi:
        context = build_context_payload_multi([question, translated_question])
    elif translated_question:
        # Korean queries lose lexical signal when retrieval uses translated text only.
        # Keep bilingual retrieval even when multi-query is disabled.
        if contains_korean(question):
            context = build_context_payload_multi([question, translated_question])
        else:
            context = build_context_payload(translated_question)
    else:
        context = build_context_payload(question)
    if matched_example_hint:
        context = _inject_exact_match_example_hint(
            context,
            question=matched_example_hint.get("question", ""),
            sql=matched_example_hint.get("sql", ""),
        )
        context = _trim_context_payload_to_budget(context, settings.context_token_budget)

    planner_payload: dict[str, Any] | None = None
    planner_intent: dict[str, Any] | None = None
    use_planner, planner_decision = _decide_planner_usage(
        settings,
        question=question,
        question_en=translated_question,
        risk_info=risk_info,
    )
    if use_planner:
        try:
            planned = plan_query_intent(question, context, question_en=translated_question)
            _add_llm_cost(planned.get("usage", {}), "plan")
            intent = planned.get("intent")
            if isinstance(intent, dict):
                planner_payload = planned
                planner_intent = intent
        except Exception:
            planner_payload = None
            planner_intent = None
            planner_decision = {**planner_decision, "fallback": "planner_failed"}

    planner_intent, age_semantic_hint_applied = _apply_anchor_age_semantic_hint(
        question=question,
        question_en=translated_question,
        planner_intent=planner_intent,
    )
    if age_semantic_hint_applied:
        planner_decision = {**planner_decision, "age_semantic_hint": "anchor_age_preferred"}

    attempt = 0
    last_error: Exception | None = None
    while attempt <= settings.max_retry_attempts:
        attempt += 1
        try:
            engineer = generate_sql(
                question,
                context,
                question_en=translated_question,
                planner_intent=planner_intent,
            )
            # LLM 경고 문구는 사용하지 않도록 제거
            engineer.pop("warnings", None)
            final_payload = engineer

            if _should_apply_expert_review(settings, risk_info):
                expert = review_sql(
                    question,
                    context,
                    engineer,
                    question_en=translated_question,
                    planner_intent=planner_intent,
                )
                final_payload = expert
                expert_applied = True
                # LLM 경고 문구는 사용하지 않도록 제거
                final_payload.pop("warnings", None)
            else:
                expert_applied = False

            usage = final_payload.get("usage", {})
            _add_llm_cost(usage, "oneshot")

            final_sql = final_payload.get("final_sql") or ""
            unresolved_issues: list[str] = []
            if final_sql:
                if oneshot_postprocess_enabled:
                    profile, profile_reasons = recommend_postprocess_profile(
                        question,
                        final_sql,
                        default_profile="relaxed",
                    )
                    final_sql, rules = postprocess_sql(question, final_sql, profile=profile)
                    if rules:
                        final_payload["final_sql"] = final_sql
                        final_payload["postprocess"] = rules
                    if profile_reasons:
                        final_payload["postprocess_profile"] = profile
                        final_payload["postprocess_profile_reasons"] = profile_reasons

                if oneshot_intent_guard_enabled:
                    aligned_sql, alignment_rules, unresolved_issues = enforce_intent_alignment(
                        question,
                        final_payload.get("final_sql") or final_sql,
                        planner_intent=planner_intent,
                    )
                    if aligned_sql.strip() != str(final_payload.get("final_sql") or "").strip():
                        final_payload["final_sql"] = aligned_sql
                        final_sql = aligned_sql
                        if alignment_rules:
                            existing = final_payload.get("postprocess")
                            base_rules = list(existing) if isinstance(existing, list) else []
                            for rule in alignment_rules:
                                if rule not in base_rules:
                                    base_rules.append(rule)
                            final_payload["postprocess"] = base_rules
                # Planner intent and SQL are inconsistent: run expert once as a targeted realignment pass.
                age_semantic_mismatch = "age_intent_mapped_to_anchor_year_group" in unresolved_issues
                if (
                    unresolved_issues
                    and planner_intent
                    and oneshot_intent_realign_enabled
                    and (not expert_applied or age_semantic_mismatch)
                ):
                    try:
                        intent_realign = review_sql(
                            question,
                            context,
                            {**final_payload, "final_sql": final_payload.get("final_sql") or final_sql},
                            question_en=translated_question,
                            planner_intent=planner_intent,
                        )
                        intent_realign.pop("warnings", None)
                        _add_llm_cost(intent_realign.get("usage", {}), "oneshot_intent_realign")
                        realigned_sql = str(intent_realign.get("final_sql") or "").strip()
                        if realigned_sql:
                            realign_profile, _ = recommend_postprocess_profile(
                                question,
                                realigned_sql,
                                default_profile="auto",
                            )
                            realigned_sql, realign_post_rules = postprocess_sql(
                                question,
                                realigned_sql,
                                profile=realign_profile,
                            )
                            realigned_sql, realign_align_rules, unresolved_after_realign = enforce_intent_alignment(
                                question,
                                realigned_sql,
                                planner_intent=planner_intent,
                            )
                            before_issue_set = set(unresolved_issues)
                            after_issue_set = set(unresolved_after_realign)
                            no_regression = after_issue_set.issubset(before_issue_set)
                            age_issue_resolved = (
                                age_semantic_mismatch
                                and "age_intent_mapped_to_anchor_year_group" not in unresolved_after_realign
                            )
                            if no_regression and (len(unresolved_after_realign) < len(unresolved_issues) or age_issue_resolved):
                                final_payload = intent_realign
                                final_payload["final_sql"] = realigned_sql
                                final_sql = realigned_sql
                                merged_rules: list[str] = []
                                existing_rules = final_payload.get("postprocess")
                                if isinstance(existing_rules, list):
                                    merged_rules.extend(existing_rules)
                                for rule in [*realign_post_rules, *realign_align_rules]:
                                    if rule and rule not in merged_rules:
                                        merged_rules.append(rule)
                                if merged_rules:
                                    final_payload["postprocess"] = merged_rules
                                unresolved_issues = unresolved_after_realign
                                final_payload["intent_alignment_repair"] = "expert_realign"
                    except Exception:
                        pass
                if unresolved_issues:
                    final_payload["intent_alignment_issues"] = unresolved_issues

            policy_result = None
            if not skip_policy and final_sql:
                policy_result = _precheck_oneshot_sql(final_sql, question)
            return {
                "mode": "advanced",
                "question": original_question,
                "question_en": translated_question if translated_question else None,
                "assumptions": scope_assumptions if scope_assumptions.get("applied") else None,
                "planner": planner_payload,
                "planner_decision": planner_decision,
                "risk": risk_info,
                "policy": policy_result,
                "context": context,
                "draft": engineer,
                "final": final_payload,
            }
        except Exception as exc:  # pragma: no cover - depends on LLM
            last_error = exc
            if attempt > settings.max_retry_attempts:
                raise
    raise last_error if last_error else RuntimeError("Unknown error")
