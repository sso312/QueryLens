"""질문 의도/분석 타입 추출.

- LLM을 사용해 의도/축/집계/차트 타입을 추정한다.
- 실패 시 간단한 규칙 기반으로 fallback 한다.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from pathlib import Path

from openai import OpenAI
from pydantic import BaseModel
from dotenv import load_dotenv

from src.config.llm_config import OPENAI_MODEL
from src.utils.logging import log_event

# 실행 위치와 무관하게 프로젝트 .env를 로드
_DOTENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_DOTENV_PATH)

_AGE_QUERY_TOKENS = ("연령", "나이", "age")
_GENDER_QUERY_TOKENS = ("성별", "gender", "sex")
_SURVIVAL_QUERY_TOKENS = ("생존", "사망", "mortality", "survival", "death", "alive", "dead", "expire")
_AGE_COLUMN_TOKENS = ("age_group", "age band", "age_band", "age", "연령", "나이")
_GENDER_COLUMN_TOKENS = ("gender", "sex", "성별")
_SURVIVAL_COLUMN_TOKENS = ("survival", "alive", "dead", "mortality", "death", "expire", "status", "outcome", "사망", "생존")


# 입력: user_query, df_schema
# 출력: Dict[str, Any]
# 흐름 - 질문에 컬럼명이 포함되어 있으면 우선 사용


def _find_column_in_query(user_query: str, columns: List[str]) -> str | None:
    q = user_query.lower()
    for col in columns:
        if col.lower() in q:
            return col
    return None


def _mentioned_columns_in_query(user_query: str, columns: List[str]) -> List[str]:
    q = user_query.lower()
    hits: List[tuple[int, str]] = []
    for col in columns:
        idx = q.find(col.lower())
        if idx >= 0:
            hits.append((idx, col))
    hits.sort(key=lambda item: item[0])
    return [col for _, col in hits]


def _pick_semantic_column(columns: List[str], tokens: tuple[str, ...], *, exclude: Optional[List[str]] = None) -> Optional[str]:
    blocked = {str(col).lower() for col in (exclude or [])}
    for col in columns:
        lower = col.lower()
        if lower in blocked:
            continue
        if any(token in lower for token in tokens):
            return col
    return None


def _extract_multisplit_slots(
    user_query: str,
    columns: List[str],
    seed_axis: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    q = (user_query or "").lower()
    wants_age = any(token in q for token in _AGE_QUERY_TOKENS)
    wants_gender = any(token in q for token in _GENDER_QUERY_TOKENS)
    wants_survival = any(token in q for token in _SURVIVAL_QUERY_TOKENS)
    if sum([wants_age, wants_gender, wants_survival]) < 2:
        return {"multisplit_axis": None, "multisplit_group": None, "multisplit_secondary_group": None}

    age_col = _pick_semantic_column(columns, _AGE_COLUMN_TOKENS)
    gender_col = _pick_semantic_column(columns, _GENDER_COLUMN_TOKENS, exclude=[age_col] if age_col else None)
    survival_col = _pick_semantic_column(
        columns,
        _SURVIVAL_COLUMN_TOKENS,
        exclude=[age_col, gender_col] if age_col or gender_col else None,
    )

    axis = age_col or seed_axis
    group = gender_col if wants_gender else None
    secondary_group = survival_col if wants_survival else None
    if axis == group:
        group = None
    if axis == secondary_group or group == secondary_group:
        secondary_group = None
    return {
        "multisplit_axis": axis,
        "multisplit_group": group,
        "multisplit_secondary_group": secondary_group,
    }


def _numeric_columns_from_schema(
    dtypes: Dict[str, str],
    numeric_columns: Optional[List[str]],
) -> List[str]:
    if numeric_columns:
        return list(numeric_columns)
    inferred: List[str] = []
    for col, dtype in dtypes.items():
        if any(token in str(dtype).lower() for token in ("int", "float", "number", "decimal")):
            inferred.append(col)
    return inferred


def _pick_primary_outcome_fallback(
    user_query: str,
    columns: List[str],
    dtypes: Dict[str, str],
    numeric_columns: Optional[List[str]],
    time_columns: Optional[List[str]],
) -> str | None:
    mentioned = _mentioned_columns_in_query(user_query, columns)
    numeric = _numeric_columns_from_schema(dtypes, numeric_columns)
    numeric_set = {c.lower() for c in numeric}
    time_set = {c.lower() for c in (time_columns or [])}

    # 1) 질문에서 직접 언급된 수치형 컬럼 우선
    for col in mentioned:
        if col.lower() in numeric_set:
            return col

    # 2) 없으면 스키마 기반 수치형 기본값
    picked_numeric = _pick_numeric_column(dtypes, numeric_columns)
    if picked_numeric:
        return picked_numeric

    # 3) 수치형이 전혀 없으면 시간축 컬럼은 피하고 언급 컬럼 선택
    for col in mentioned:
        if col.lower() not in time_set:
            return col

    # 4) 마지막 fallback
    return mentioned[0] if mentioned else None


def _pick_group_var_fallback(
    user_query: str,
    analysis_intent: str,
    columns: List[str],
    categorical_columns: Optional[List[str]],
    time_columns: Optional[List[str]],
    primary_outcome: Optional[str],
    time_var: Optional[str],
) -> str | None:
    q = user_query.lower()
    if not any(k in q for k in ("별", "by ")):
        return None

    time_set = {c.lower() for c in (time_columns or [])}
    primary_lower = (primary_outcome or "").lower()
    time_var_lower = (time_var or "").lower()

    mentioned_candidates = _mentioned_columns_in_query(user_query, columns)

    for col in mentioned_candidates:
        lower = col.lower()
        if lower == primary_lower:
            continue
        # trend의 "by month"는 그룹이 아니라 시간축으로 해석한다.
        if analysis_intent == "trend" and (lower == time_var_lower or lower in time_set):
            continue
        return col

    # trend에서는 질문에 명시되지 않은 그룹을 자동 주입하지 않는다.
    if analysis_intent == "trend":
        return None

    for col in (categorical_columns or []):
        lower = col.lower()
        if lower == primary_lower:
            continue
        if lower in time_set:
            continue
        return col
    return None

# 입력: dtypes, numeric_columns
# 출력: str | None
# 1순위: numeric_columns 리스트에서 선택
# 2순위: dtypes에서 숫자형 컬럼을 찾아 선택
# 없으면 None 반환
# y축에 사용


def _pick_numeric_column(
    dtypes: Dict[str, str],
    numeric_columns: Optional[List[str]] = None,
) -> str | None:
    # 숫자형 컬럼을 하나 고른다(없으면 None)
    if numeric_columns:
        return numeric_columns[0]
    for col, dtype in dtypes.items():
        if any(token in dtype for token in ("int", "float", "number", "decimal")):
            return col
    return None

# 입력: columns, time_columns
# 출력: str | None
# time 컬럼이 있으면 우선 선택
# 없으면 이름 힌트 기반으로 선택
# x축에 사용


def _pick_time_column(
    columns: List[str],
    time_columns: Optional[List[str]] = None,
) -> str | None:
    # 시간 관련 컬럼명 힌트를 기반으로 선택
    if time_columns:
        return time_columns[0]
    hints = ("date", "time", "day", "month", "year")
    for col in columns:
        if any(h in col.lower() for h in hints):
            return col
    return None

# 입력: categorical_columns
# 출력: str | None
# 범주형 컬럼이 있으면 우선 선택
# 없으면 None 반환


def _pick_categorical_column(categorical_columns: Optional[List[str]]) -> str | None:
    if categorical_columns:
        return categorical_columns[0]
    return None

# 입력: user_query
# 출력: str
# 간단한 키워드 매칭으로 의도 추정


def _infer_intent(user_query: str) -> str:
    q = user_query.lower()
    if any(k in q for k in ("추세", "trend", "변화", "over time", "시간", "월별", "연도별", "일별")):
        return "trend"
    if any(k in q for k in ("scatter", "산점도", "bubble", "버블", "line+scatter", "line scatter")):
        return "correlation"
    if any(k in q for k in ("분포", "distribution", "hist", "히스토")):
        return "distribution"
    if any(k in q for k in ("비교", "compare", "difference", "vs")):
        return "comparison"
    if any(k in q for k in ("비율", "proportion", "ratio", "share")):
        return "proportion"
    if any(k in q for k in ("상관", "correlation", "관계")):
        return "correlation"
    return "overview"


def _infer_chart_preference(user_query: str) -> str | None:
    q = (user_query or "").lower()
    if any(token in q for token in ("histogram", "hist", "히스토그램", "히스토")):
        return "hist"
    if any(token in q for token in ("violin", "바이올린")):
        return "violin"
    if any(token in q for token in ("area", "area chart", "면적", "영역")):
        return "area"
    if any(
        token in q
        for token in (
            "dynamic scatter",
            "animated scatter",
            "animation scatter",
            "동적 산점도",
            "애니메이션 산점도",
            "버블",
            "bubble",
        )
    ):
        return "dynamic_scatter"
    if any(
        token in q
        for token in (
            "line+scatter",
            "line scatter",
            "line and scatter",
            "lines+markers",
            "라인 스캐터",
            "선과 점",
            "선+점",
        )
    ):
        return "line_scatter"
    if any(token in q for token in ("scatter plot", "scatter", "산점도")):
        return "scatter"
    if any(
        token in q
        for token in (
            "line plot",
            "line chart",
            "line",
            "라인 플롯",
            "라인 차트",
            "선 그래프",
        )
    ):
        return "line"
    return None


def _infer_intent_from_glossary(user_query: str, retrieved_context: str | None) -> str | None:
    if not retrieved_context:
        return None
    q = (user_query or "").lower()
    # Expect glossary entries like:
    # Term: change
    # Intent: trend
    import re

    for term, intent in re.findall(r"Term:\s*(.+?)\nIntent:\s*(\w+)", retrieved_context):
        if term.lower() in q:
            return intent
    return None
# LLM 응답 모델
# 분석 의도, 축, 그룹, 집계, 추천 차트 타입
# 모두 Optional


class IntentResult(BaseModel):
    # 분석 목적
    analysis_intent: str
    # x/y 축
    x: Optional[str] = None
    y: Optional[str] = None
    # 그룹 기준
    group_by: Optional[str] = None
    # 집계 방식
    agg: Optional[str] = None
    # 추천 차트
    recommended_chart: Optional[str] = None


# intent 정규화
# time_trend 같은 값을 trend로 정규화
# 입력: intent 문자열
def _normalize_intent(intent: str) -> str:
    # time_trend 같은 값을 trend로 정규화
    if intent == "time_trend":
        return "trend"
    return intent

# LLM을 사용해 의도/축/집계/차트 타입 추출
# 실패 시 예외 발생
# 입력: user_query, df_schema
# 출력: IntentResult 객체


def _llm_extract_intent(
    user_query: str,
    df_schema: Dict[str, Any],
    retrieved_context: str | None = None,
) -> IntentResult:
    import os
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY가 설정되어 있지 않습니다.")

    client = OpenAI(api_key=api_key)

    system_prompt = (
        "너는 데이터 분석 어시스턴트다. 사용자 질문과 데이터 스키마를 보고 "
        "분석 의도(추세/분포/비율/비교/상관관계/요약), 축, 그룹, 집계, 차트 타입을 "
        "결정해 JSON으로만 답한다."
    )

    context_block = ""
    if retrieved_context:
        context_block = f"\n\n참고 컨텍스트:\n{retrieved_context}\n"

    user_prompt = (
        "사용자 질문과 데이터 스키마는 아래와 같다.\n"
        f"- 질문: {user_query}\n"
        f"- 스키마: {df_schema}\n"
        f"{context_block}\n"
        "다음 필드를 가진 JSON으로만 답해라:\n"
        "{analysis_intent, x, y, group_by, agg, recommended_chart}\n"
        "analysis_intent 후보: trend, distribution, proportion, comparison, correlation, summary\n"
    )

    response = client.responses.parse(
        model=OPENAI_MODEL or "gpt-4o-mini",
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        text_format=IntentResult,
    )

    return response.output_parsed

# 주요 의도/변수 추출 함수
# 입력: user_query, df_schema
# 출력: Dict[str, Any]


def extract_intent(
    user_query: str,
    df_schema: Dict[str, Any],
    retrieved_context: str | None = None,
) -> Dict[str, Any]:
    """질문 의도와 핵심 변수를 추출한다."""
    columns = df_schema.get("columns", [])
    dtypes = df_schema.get("dtypes", {})
    column_roles = df_schema.get(
        "column_roles", {}) if isinstance(df_schema, dict) else {}
    numeric_columns = column_roles.get("numeric", [])
    time_columns = column_roles.get("time", [])
    categorical_columns = column_roles.get("categorical", [])

    glossary_intent = _infer_intent_from_glossary(user_query, retrieved_context)
    chart_preference = _infer_chart_preference(user_query)

    try:
        llm_result = _llm_extract_intent(user_query, df_schema, retrieved_context)
        analysis_intent = _normalize_intent(llm_result.analysis_intent)
        if glossary_intent and analysis_intent in ("summary", "overview"):
            analysis_intent = glossary_intent
        if chart_preference == "line_scatter" and analysis_intent in ("summary", "overview"):
            analysis_intent = "trend"
        if chart_preference in ("scatter", "dynamic_scatter") and analysis_intent in ("summary", "overview"):
            analysis_intent = "correlation"
        primary_outcome = (
            llm_result.y
            or llm_result.x
            or _pick_numeric_column(dtypes, numeric_columns)
        )
        time_var = llm_result.x if analysis_intent == "trend" else None
        group_var = llm_result.group_by
        if group_var and primary_outcome and group_var.lower() == primary_outcome.lower():
            group_var = None
        if analysis_intent == "trend" and group_var and time_var and group_var.lower() == time_var.lower():
            group_var = None
        multisplit_slots = _extract_multisplit_slots(user_query, columns, seed_axis=group_var)
        recommended_chart = (chart_preference or llm_result.recommended_chart or "").strip() or None

        log_event("intent.llm.success",
                  llm_result.model_dump(exclude_none=True))

        return {
            "analysis_intent": analysis_intent,
            "primary_outcome": primary_outcome,
            "time_var": time_var,
            "group_var": group_var,
            "agg": llm_result.agg,
            "recommended_chart": recommended_chart,
            **multisplit_slots,
            "extra_analyses": [],
            "user_query": user_query,
        }
    except Exception as exc:
        # LLM 실패 시 규칙 기반 fallback
        log_event("intent.llm.error", {"error": str(exc)})

        analysis_intent = glossary_intent or _infer_intent(user_query)
        if chart_preference == "line_scatter" and analysis_intent in ("overview", "summary"):
            analysis_intent = "trend"
        if chart_preference in ("scatter", "dynamic_scatter") and analysis_intent in ("overview", "summary"):
            analysis_intent = "correlation"
        primary_outcome = _pick_primary_outcome_fallback(
            user_query=user_query,
            columns=columns,
            dtypes=dtypes,
            numeric_columns=numeric_columns,
            time_columns=time_columns,
        )
        time_var = (
            _pick_time_column(
                columns, time_columns) if analysis_intent == "trend" else None
        )

        group_var = _pick_group_var_fallback(
            user_query=user_query,
            analysis_intent=analysis_intent,
            columns=columns,
            categorical_columns=categorical_columns,
            time_columns=time_columns,
            primary_outcome=primary_outcome,
            time_var=time_var,
        )
        multisplit_slots = _extract_multisplit_slots(user_query, columns, seed_axis=group_var)

        return {
            "analysis_intent": analysis_intent,
            "primary_outcome": primary_outcome,
            "time_var": time_var,
            "group_var": group_var,
            "recommended_chart": chart_preference,
            **multisplit_slots,
            "extra_analyses": [],
            "user_query": user_query,
        }
