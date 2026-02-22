"""분석·차트 플랜 생성 룰 엔진.

- intent_info와 df를 기반으로 여러 개의 분석 플랜을 만든다.
- 각 플랜은 chart_spec 초안과 간단한 설명을 포함한다.
- 전제: ADMISSIONS/ICUSTAYS가 중심 테이블이며, 이벤트는 HADM_ID/STAY_ID에 종속된다.
- PATIENTS는 속성 테이블이며 분석 기준 테이블이 아니다.
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

import pandas as pd
from pandas.api import types as pdt

from src.agent import rule_engine_postprocess
from src.utils.logging import log_event

# 도메인 금지/허용 토큰 상수 (임상 의미 보존)
# 전제: 중심 테이블은 ADMISSIONS / ICUSTAYS, 이벤트는 HADM_ID/STAY_ID 종속
_FORBIDDEN_TRAJECTORY = ("subject_id", "patient_id")
_ALLOWED_TRAJECTORY = ("stay_id", "hadm_id")
_IDENTIFIER_COLS = ("subject_id", "hadm_id", "stay_id", "patient_id")
_FORBIDDEN_GROUP_COLS = (
    "subject_id",
    "hadm_id",
    "stay_id",
    "seq_num",
    "transfer_id",
    "orderid",
    "linkorderid",
    "order_provider_id",
    "caregiver_id",
    "pharmacy_id",
    "icd_code",
    "itemid",
    "emar_id",
    "poe_id",
)
# low-cardinality 허용 그룹(컬럼 실명 기준)
_ALLOWED_GROUP_COLS = (
    "gender",
    "anchor_year_group",
    "admission_type",
    "insurance",
    "language",
    "race",
    "marital_status",
    "first_careunit",
    "last_careunit",
    "curr_service",
    "careunit",
)
_CLINICAL_HINTS = [
    "subject_id",
    "hadm_id",
    "stay_id",
    "icd",
    "drg",
    "diagnosis",
    "admission",
    "discharge",
    "mortality",
    "los",
    "length_of_stay",
    "careunit",
    "icu",
    "ward",
    "charttime",
    "lab",
    "vital",
]
_TIME_CANDIDATES = (
    "charttime",
    "admittime",
    "dischtime",
    "intime",
    "outtime",
    "starttime",
    "endtime",
    "storetime",
    "storedate",
    "edregtime",
    "edouttime",
    "ordertime",
    "transfertime",
    "chartdate",
)
_PREFERRED_NUMERIC_Y = (
    "valuenum",
    "value",
    "amount",
    "rate",
    "los",
    "diagnosis_count",
    "count",
    "anchor_age",
    "doses_per_24_hrs",
)
_CONFUSION_ACTUAL_TOKENS = (
    "actual",
    "true",
    "ground_truth",
    "label",
    "target",
    "정답",
    "실제",
    "실측",
)
_CONFUSION_PRED_TOKENS = (
    "pred",
    "prediction",
    "yhat",
    "inferred",
    "estimate",
    "예측",
    "추정",
)
_AGE_QUERY_TOKENS = ("연령", "나이", "age")
_GENDER_QUERY_TOKENS = ("성별", "gender", "sex")
_SURVIVAL_QUERY_TOKENS = ("생존", "사망", "mortality", "survival", "death", "alive", "dead", "expire")
_BAR_QUERY_TOKENS = ("bar", "막대", "바 차트", "막대그래프")
_AGE_COLUMN_TOKENS = ("age_group", "age band", "age_band", "age", "연령", "나이")
_GENDER_COLUMN_TOKENS = ("gender", "sex", "성별")
_SURVIVAL_COLUMN_TOKENS = ("survival", "alive", "dead", "mortality", "death", "expire", "status", "outcome", "사망", "생존")
_DEFAULT_MAX_CATEGORIES = 10
_MAX_CATEGORY_CHART_TYPES = {
    "bar",
    "bar_basic",
    "bar_grouped",
    "bar_stacked",
    "bar_hgroup",
    "bar_hstack",
    "bar_percent",
    "bar_hpercent",
    "lollipop",
}


def _extract_chart_spec_from_context(
    retrieved_context: Optional[str],
    df: pd.DataFrame,
) -> Optional[Dict[str, Any]]:
    if not retrieved_context:
        return None
    # 간단한 패턴: "chart_spec: { ... }"
    match = re.search(r"chart_spec:\s*(\{.*?\})", retrieved_context)
    if not match:
        return None
    try:
        spec = json.loads(match.group(1))
    except Exception:
        return None

    chart_type = spec.get("chart_type")
    x = spec.get("x")
    y = spec.get("y")
    group = spec.get("group")

    # 최소 유효성: 컬럼이 실제로 존재하는지 확인
    cols = set(df.columns)
    for col in (x, y, group):
        if col and col not in cols:
            return None

    return {
        "chart_spec": {k: v for k, v in spec.items() if v is not None},
        "reason": "RAG 예시 기반 추천 플랜입니다.",
    }


def _infer_chart_from_columns(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Infer a chart spec using only result columns."""
    cols = list(df.columns)
    lower = {c.lower(): c for c in cols}

    # Alias-based hints (from SQL templates)
    if "x_time" in lower and "y_value" in lower:
        return {
            "chart_spec": {"chart_type": "line", "x": lower["x_time"], "y": lower["y_value"]},
            "reason": "결과 별칭 기준으로 시계열 집계 차트가 적합합니다.",
        }
    if "x_group" in lower and "y_value" in lower:
        return {
            "chart_spec": {"chart_type": "bar", "x": lower["x_group"], "y": lower["y_value"]},
            "reason": "결과 별칭 기준으로 그룹 집계 차트가 적합합니다.",
        }
    # Benchmark-like schema -> horizontal stacked bar
    if "metric_name" in lower and "run_name" in lower and "metric_value" in lower:
        return {
            "chart_spec": {
                "chart_type": "bar_hstack",
                "x": lower["metric_name"],
                "y": lower["metric_value"],
                "group": lower["run_name"],
                "bar_mode": "stack",
                "orientation": "h",
                "title": "Summary Metrics",
                "x_title": "metric_value",
                "y_title": "metric_name",
            },
            "reason": "메트릭/런 스키마라 수평 누적 막대가 비교에 적합합니다.",
        }

    cm_axes = _pick_confusion_matrix_axes(df)
    if cm_axes:
        has_actual_named = any(
            any(token in col.lower() for token in _CONFUSION_ACTUAL_TOKENS)
            for col in cols
        )
        has_pred_named = any(
            any(token in col.lower() for token in _CONFUSION_PRED_TOKENS)
            for col in cols
        )
        if has_actual_named and has_pred_named:
            spec: Dict[str, Any] = {
                "chart_type": "confusion_matrix",
                "x": cm_axes["x"],
                "y": cm_axes["y"],
            }
            if cm_axes.get("value"):
                spec["group"] = cm_axes["value"]
                spec["agg"] = "sum"
            return {
                "chart_spec": spec,
                "reason": "실제값-예측값 축이 감지되어 혼동행렬이 적합합니다.",
            }

    # Time-series heuristics
    time_cols = [c for c in cols if any(t in c.lower() for t in ("time", "date", "day", "month", "year"))]
    # numeric은 식별자/코드 컬럼을 제외한 실제 측정치 우선
    numeric_cols = [
        c
        for c in cols
        if pdt.is_numeric_dtype(df[c]) and not _is_identifier_col(c) and "code" not in c.lower()
    ]
    numeric_cols.sort(
        key=lambda c: next(
            (idx for idx, token in enumerate(_PREFERRED_NUMERIC_Y) if token in c.lower()),
            999,
        )
    )
    time_cols.sort(
        key=lambda c: next(
            (idx for idx, token in enumerate(_TIME_CANDIDATES) if token == c.lower()),
            999,
        )
    )
    categorical_cols = [
        c for c in cols
        if pdt.is_string_dtype(df[c]) or pdt.is_categorical_dtype(df[c])
    ]
    categorical_cols = [
        c for c in categorical_cols
        if not _is_identifier_col(c)
    ]

    def _nunique(col: str) -> int:
        try:
            return int(df[col].nunique(dropna=True))
        except Exception:
            return 999999

    def _pick_hierarchy(cands: List[str]) -> tuple[Optional[str], Optional[str]]:
        valid = [c for c in cands if 1 < _nunique(c) <= 30]
        if len(valid) < 2:
            return None, None
        valid.sort(key=lambda c: _nunique(c))
        # parent는 저카디널리티, child는 상대적으로 상세한 범주
        return valid[0], valid[1]

    if time_cols and numeric_cols:
        return {
            "chart_spec": {"chart_type": "line", "x": time_cols[0], "y": numeric_cols[0]},
            "reason": "시간형 컬럼과 수치형 컬럼이 있어 추세 차트가 적합합니다.",
        }

    # Categorical hierarchy + numeric -> nested pie (sunburst)
    parent, child = _pick_hierarchy(categorical_cols)
    if parent and child and numeric_cols:
        return {
            "chart_spec": {
                "chart_type": "nested_pie",
                "x": parent,
                "group": child,
                "y": numeric_cols[0],
            },
            "reason": "상하위 범주 비율을 동시에 보여주기 위해 중첩 파이가 적합합니다.",
        }

    # Wide multi-numeric table -> grouped bar (simple-to-detailed bar entry)
    series_hint_tokens = (
        "women",
        "men",
        "gap",
        "score",
        "rate",
        "ratio",
        "precision",
        "recall",
        "f1",
        "accuracy",
        "auc",
    )
    multi_series_numeric = [
        c for c in numeric_cols
        if any(tok in c.lower() for tok in series_hint_tokens)
    ]
    if categorical_cols and len(multi_series_numeric) >= 2:
        x_col = categorical_cols[0]
        series_cols = multi_series_numeric[:6]
        return {
            "chart_spec": {
                "chart_type": "bar_grouped",
                "x": x_col,
                "series_cols": series_cols,
                "bar_mode": "group",
            },
            "reason": "다중 수치 시리즈라 그룹 막대로 항목별 비교가 적합합니다.",
        }

    # Categorical + numeric + categorical-group -> pyramid
    if categorical_cols and numeric_cols:
        x_candidates = [c for c in categorical_cols if 2 <= _nunique(c) <= 40]
        g_candidates = [c for c in categorical_cols if 2 <= _nunique(c) <= 6]
        x_col = x_candidates[0] if x_candidates else None
        g_col = next((c for c in g_candidates if c != x_col), None)
        if x_col and g_col:
            return {
                "chart_spec": {
                    "chart_type": "pyramid",
                    "x": x_col,
                    "y": numeric_cols[0],
                    "group": g_col,
                },
                "reason": "두 그룹을 좌우로 비교하기에 피라미드 막대가 적합합니다.",
            }

    # Legacy: Categorical + two numeric -> pyramid
    if categorical_cols and len(numeric_cols) >= 2:
        return {
            "chart_spec": {
                "chart_type": "pyramid",
                "x": categorical_cols[0],
                "y": numeric_cols[0],
                "group": numeric_cols[1],
            },
            "reason": "범주형 기준 좌우 비교가 가능해 피라미드 막대 차트가 적합합니다.",
        }

    # Two numeric columns -> scatter
    if len(numeric_cols) >= 2:
        return {
            "chart_spec": {"chart_type": "scatter", "x": numeric_cols[0], "y": numeric_cols[1]},
            "reason": "수치형 컬럼이 2개 이상이라 상관관계 산점도가 적합합니다.",
        }

    # Categorical + numeric -> pie (low-cardinality) else bar
    if categorical_cols and numeric_cols:
        pie_x = next((c for c in categorical_cols if 1 < _nunique(c) <= 12), None)
        if pie_x:
            return {
                "chart_spec": {"chart_type": "pie", "x": pie_x, "y": numeric_cols[0], "agg": "sum"},
                "reason": "저카디널리티 범주 비중 비교에 파이 차트가 적합합니다.",
            }
        return {
            "chart_spec": {"chart_type": "bar", "x": categorical_cols[0], "y": numeric_cols[0]},
            "reason": "범주형-수치형 조합으로 비교 막대 차트가 적합합니다.",
        }

    # Single numeric -> histogram
    if len(numeric_cols) == 1:
        return {
            "chart_spec": {"chart_type": "hist", "x": numeric_cols[0]},
            "reason": "단일 수치형 컬럼 분포 확인을 위해 히스토그램이 적합합니다.",
        }

    return None

# 입력: df, col, max_groups
# 출력: bool
# 그룹 후보의 카디널리티를 제한


def _is_low_cardinality(df: pd.DataFrame, col: str, max_groups: int) -> bool:
    try:
        return int(df[col].nunique(dropna=True)) <= max_groups
    except Exception:
        return False

# 입력: df
# 출력: str | None
# 임상 데이터에서 안전한 그룹 컬럼을 선택(화이트리스트 + 카디널리티 제한)


def _pick_safe_group(df: pd.DataFrame) -> str | None:
    allow_tokens = _ALLOWED_GROUP_COLS
    deny_tokens = (
        "name",
        "patient",
        "subject_id",
        "hadm_id",
        "stay_id",
        "icd",
        "drg",
        "diagnosis",
        "mrn",
        "ssn",
        "itemid",
        "emar_id",
        "poe_id",
    )
    max_groups = 30

    for col in df.columns:
        lower = col.lower()
        if any(d in lower for d in deny_tokens):
            continue
        if not any(a in lower for a in allow_tokens):
            continue
        if not (pdt.is_string_dtype(df[col]) or pdt.is_categorical_dtype(df[col])):
            continue
        if _is_low_cardinality(df, col, max_groups):
            return col
    return None


def _pick_secondary_group(
    df: pd.DataFrame,
    exclude: Optional[str] = None,
) -> str | None:
    for col in df.columns:
        if exclude and col == exclude:
            continue
        if _is_identifier_col(col):
            continue
        if not (pdt.is_string_dtype(df[col]) or pdt.is_categorical_dtype(df[col])):
            continue
        if _is_low_cardinality(df, col, 12):
            return col
    return None


def _pick_semantic_group(
    df: pd.DataFrame,
    tokens: tuple[str, ...],
    *,
    exclude: Optional[List[str]] = None,
    max_groups: int = 30,
) -> Optional[str]:
    blocked = {str(col).lower() for col in (exclude or [])}
    for col in df.columns:
        lower = str(col).lower()
        if lower in blocked:
            continue
        if _is_identifier_col(col):
            continue
        if not (pdt.is_string_dtype(df[col]) or pdt.is_categorical_dtype(df[col])):
            continue
        if not any(token in lower for token in tokens):
            continue
        if _is_low_cardinality(df, col, max_groups):
            return col
    return None


def _infer_multisplit_bar_slots(
    user_query: Optional[str],
    df: pd.DataFrame,
    *,
    seed_axis: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    q = str(user_query or "").lower()
    wants_age = any(token in q for token in _AGE_QUERY_TOKENS)
    wants_gender = any(token in q for token in _GENDER_QUERY_TOKENS)
    wants_survival = any(token in q for token in _SURVIVAL_QUERY_TOKENS)
    wants_bar = any(token in q for token in _BAR_QUERY_TOKENS)
    # 최소 두 개 이상 슬롯이 동시에 언급되면 복합 분할 의도로 간주한다.
    if sum([wants_age, wants_gender, wants_survival]) < 2:
        return {"axis": None, "group": None, "secondary_group": None}

    age_col = _pick_semantic_group(df, _AGE_COLUMN_TOKENS, max_groups=40)
    gender_col = _pick_semantic_group(df, _GENDER_COLUMN_TOKENS, exclude=[age_col] if age_col else None, max_groups=12)
    survival_col = _pick_semantic_group(
        df,
        _SURVIVAL_COLUMN_TOKENS,
        exclude=[age_col, gender_col] if age_col or gender_col else None,
        max_groups=8,
    )

    axis = age_col or seed_axis
    group = gender_col if wants_gender else None
    secondary_group = survival_col if wants_survival else None

    if axis == group:
        group = None
    if axis == secondary_group or group == secondary_group:
        secondary_group = None
    # 바 차트 명시가 없더라도 연령/성별/생존 조합은 막대형 비교가 기본값으로 유효하다.
    if not wants_bar and not (group and secondary_group):
        return {"axis": None, "group": None, "secondary_group": None}
    return {
        "axis": axis,
        "group": group,
        "secondary_group": secondary_group,
    }

# 입력: df
# 출력: str | None
# 임상 추세에서 환자 단위 trajectory를 위한 그룹 컬럼 선택


def _pick_patient_group(df: pd.DataFrame) -> str | None:
    preferred = _ALLOWED_TRAJECTORY
    for col in df.columns:
        lower = col.lower()
        if any(p == lower for p in preferred) or any(p in lower for p in preferred):
            return col
    return None

# 입력: df, hints
# 출력: bool
# 컬럼명에 의료/임상 관련 힌트가 있는지 확인


def _has_column_hint(df: pd.DataFrame, hints: List[str]) -> bool:
    cols = [c.lower() for c in df.columns]
    return any(any(h in c for h in hints) for c in cols)

# 입력: df
# 출력: str | None
# 임상 데이터에서 의미 있는 그룹 컬럼 후보를 고른다


def _pick_clinical_group(df: pd.DataFrame) -> str | None:
    preferred = (
        "careunit",
        "icu",
        "ward",
        "admission_type",
        "admission",
        "discharge",
        "service",
        "diagnosis",
        "icd",
        "drg",
    )
    for col in df.columns:
        lower = col.lower()
        if any(p in lower for p in preferred):
            if _is_low_cardinality(df, col, 30):
                return col
    return None

# 입력: user_query
# 출력: context_flags
# ICU/입실 후 문맥을 보수적으로 판단 (의미 보존 우선)


def _infer_context_flags(user_query: Optional[str], available_columns: List[str]) -> Dict[str, bool]:
    q = (user_query or "").lower()
    cols = {c.lower() for c in available_columns}
    icu_kw = any(k in q for k in ("icu", "중환자실", "입실", "입실 후", "입실후"))
    admit_kw = any(k in q for k in ("입원 후", "admission after", "admit after"))
    if not admit_kw:
        admit_kw = any(k in q for k in ("admission", "admit")) and "admission_type" not in q
    if "입원유형" in q:
        admit_kw = False
    # "후" 단독 키워드는 오탐이 많아 숫자+시간 단위 패턴만 허용한다.
    post_days = bool(
        re.search(r"\bafter\s+\d+\s*(day|days|hour|hours|d|h)\b", q)
        or re.search(r"\b\d+\s*(day|days|hour|hours|d|h)\s+after\b", q)
        or re.search(r"\b후\s*\d+\s*(일|시간)\b", q)
        or re.search(r"\b\d+\s*(일|시간)\s*후\b", q)
        or re.search(r"\bn일\s*후\b", q)
    )

    # ICU 맥락: 키워드 또는 stay_id+intime이 있는 경우 보수적으로 판단
    icu_context = icu_kw or ("stay_id" in cols and "intime" in cols)
    admit_context = admit_kw or ("hadm_id" in cols and "admittime" in cols)

    return {
        "icu_context": icu_context,
        "admit_context": admit_context and not icu_context,
        "post_days": post_days,
    }


def _infer_bar_style(user_query: Optional[str]) -> Dict[str, bool]:
    q = (user_query or "").lower()
    bar_requested = any(token in q for token in ("bar", "막대", "막대형", "바 차트"))
    stacked = any(token in q for token in ("stack", "stacked", "누적", "스택"))
    horizontal = any(token in q for token in ("horizontal", "가로", "수평"))
    percent = any(token in q for token in ("100%", "percent", "비율", "비중", "구성비"))
    grouped = any(token in q for token in ("grouped", "group", "그룹", "군집", "클러스터", "비교"))
    detailed = any(token in q for token in ("상세", "구체", "detailed", "세부"))
    return {
        "requested": bar_requested,
        "stacked": stacked,
        "horizontal": horizontal,
        "percent": percent,
        "grouped": grouped,
        "detailed": detailed,
    }


def _infer_chart_preference(user_query: Optional[str]) -> Optional[str]:
    q = (user_query or "").lower()
    if any(
        token in q
        for token in (
            "confusion matrix",
            "confusion_matrix",
            "혼동행렬",
            "혼동 행렬",
            "오분류 행렬",
            "cm matrix",
        )
    ):
        return "confusion_matrix"
    if any(token in q for token in ("lollipop", "로리팝", "롤리팝")):
        return "lollipop"
    if any(token in q for token in ("heatmap", "히트맵", "heat map")):
        return "heatmap"
    if any(token in q for token in ("treemap", "트리맵", "tree map")):
        return "treemap"
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
        for token in ("line plot", "line chart", "line", "라인 플롯", "라인 차트", "선 그래프")
    ):
        return "line"
    return None


def _pick_animation_frame_col(
    df: pd.DataFrame,
    exclude: Optional[List[str]] = None,
) -> Optional[str]:
    blocked = {str(col).lower() for col in (exclude or [])}
    time_hint = ("date", "time", "month", "year", "day")
    for col in df.columns:
        lower = str(col).lower()
        if lower in blocked:
            continue
        if not any(token in lower for token in time_hint):
            continue
        try:
            uniq = int(df[col].nunique(dropna=True))
        except Exception:
            uniq = 0
        if uniq >= 2 and uniq <= 200:
            return col
    for col in df.columns:
        lower = str(col).lower()
        if lower in blocked:
            continue
        if not pdt.is_numeric_dtype(df[col]):
            continue
        if _is_identifier_col(col):
            continue
        try:
            uniq = int(df[col].nunique(dropna=True))
        except Exception:
            uniq = 0
        if uniq >= 4 and uniq <= 80:
            return col
    return None


def _pick_size_col(
    df: pd.DataFrame,
    exclude: Optional[List[str]] = None,
) -> Optional[str]:
    blocked = {str(col).lower() for col in (exclude or [])}
    for col in df.columns:
        lower = str(col).lower()
        if lower in blocked:
            continue
        if _is_identifier_col(col):
            continue
        if not pdt.is_numeric_dtype(df[col]):
            continue
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if series.empty:
            continue
        if float(series.max()) <= 0:
            continue
        try:
            uniq = int(series.nunique(dropna=True))
        except Exception:
            uniq = 0
        if uniq >= 4:
            return col
    return None


def _pick_confusion_matrix_axes(
    df: pd.DataFrame,
    *,
    seed_x: Optional[str] = None,
    seed_y: Optional[str] = None,
    seed_value: Optional[str] = None,
) -> Optional[Dict[str, Optional[str]]]:
    cols = list(df.columns)

    def _nunique(col: str) -> int:
        try:
            return int(df[col].nunique(dropna=True))
        except Exception:
            return 0

    categorical_cols: List[str] = []
    for col in cols:
        if _is_identifier_col(col):
            continue
        uniq = _nunique(col)
        if uniq < 2 or uniq > 60:
            continue
        if pdt.is_string_dtype(df[col]) or pdt.is_categorical_dtype(df[col]):
            categorical_cols.append(col)
            continue
        if pdt.is_numeric_dtype(df[col]) and uniq <= 20:
            # label-encoded class column 허용
            categorical_cols.append(col)

    if len(categorical_cols) < 2:
        return None

    numeric_cols = [
        col
        for col in cols
        if pdt.is_numeric_dtype(df[col]) and not _is_identifier_col(col)
    ]

    actual_col = seed_y if isinstance(seed_y, str) and seed_y in categorical_cols else None
    pred_col = seed_x if isinstance(seed_x, str) and seed_x in categorical_cols else None

    if not actual_col:
        for token in _CONFUSION_ACTUAL_TOKENS:
            actual_col = next((col for col in categorical_cols if token in col.lower()), None)
            if actual_col:
                break
    if not pred_col:
        for token in _CONFUSION_PRED_TOKENS:
            pred_col = next(
                (col for col in categorical_cols if col != actual_col and token in col.lower()),
                None,
            )
            if pred_col:
                break

    if not actual_col:
        actual_col = categorical_cols[0]
    if not pred_col:
        pred_col = next((col for col in categorical_cols if col != actual_col), None)
    if not actual_col or not pred_col:
        return None

    value_col: Optional[str] = None
    if (
        isinstance(seed_value, str)
        and seed_value in numeric_cols
        and seed_value not in {actual_col, pred_col}
    ):
        value_col = seed_value
    if value_col is None:
        for token in ("cnt", "count", "freq", "n", "num", "support", "value", "cases"):
            value_col = next(
                (
                    col
                    for col in numeric_cols
                    if col not in {actual_col, pred_col} and token in col.lower()
                ),
                None,
            )
            if value_col:
                break

    return {"x": pred_col, "y": actual_col, "value": value_col}


def _numeric_candidates(
    df: pd.DataFrame,
    exclude: Optional[List[str]] = None,
) -> List[str]:
    blocked = {str(col).lower() for col in (exclude or [])}
    cands: List[str] = []
    for col in df.columns:
        lower = str(col).lower()
        if lower in blocked:
            continue
        if _is_identifier_col(col):
            continue
        if pdt.is_numeric_dtype(df[col]):
            cands.append(col)
    return cands


def _pick_numeric_by_tokens(cands: List[str], tokens: List[str]) -> Optional[str]:
    for token in tokens:
        for col in cands:
            if token in col.lower():
                return col
    return None

# 입력: cols, candidates
# 출력: str | None
# 컬럼 실명 기준으로 가장 먼저 매칭되는 컬럼 선택


def _first_matching_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand in lower_map:
            return lower_map[cand]
    return None


def _first_time_col(cols: List[str]) -> Optional[str]:
    exact = _first_matching_col(cols, list(_TIME_CANDIDATES))
    if exact:
        return exact
    for col in cols:
        lower = col.lower()
        if any(token in lower for token in ("time", "date", "day", "month", "year")):
            return col
    return None

# 입력: cols
# 출력: str | None
# 경과시간 파생 컬럼 후보를 찾는다


def _find_elapsed_column(cols: List[str], context: str) -> Optional[str]:
    lower = [c.lower() for c in cols]
    if context == "icu":
        candidates = [
            "elapsed_icu_days",
            "icu_elapsed_days",
            "days_since_intime",
            "hours_since_intime",
            "icu_day",
        ]
    else:
        candidates = [
            "elapsed_admit_days",
            "admit_elapsed_days",
            "days_since_admittime",
            "hours_since_admittime",
            "admit_day",
        ]
    for cand in candidates:
        if cand in lower:
            return cols[lower.index(cand)]
    return None

# 입력: col
# 출력: bool
# 식별자 계열 컬럼인지 판단


def _is_identifier_col(col: str) -> bool:
    lower = col.lower()
    return any(tok == lower or tok in lower for tok in _IDENTIFIER_COLS)


def _dedupe_plans(plans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return rule_engine_postprocess.dedupe_plans(plans)


def _record_failure(
    failure_reasons: Optional[List[str]],
    reason: str,
) -> None:
    if failure_reasons is None:
        return
    normalized = (reason or "").strip()
    if normalized and normalized not in failure_reasons:
        failure_reasons.append(normalized)


def _bar_preferred_chart_type(style: Dict[str, bool]) -> Optional[str]:
    return rule_engine_postprocess.bar_preferred_chart_type(style)


def _prioritize_bar_plans(
    plans: List[Dict[str, Any]],
    style: Dict[str, bool],
) -> List[Dict[str, Any]]:
    return rule_engine_postprocess.prioritize_bar_plans(plans, style)


def _prioritize_requested_chart(
    plans: List[Dict[str, Any]],
    preferred_chart: Optional[str],
) -> List[Dict[str, Any]]:
    return rule_engine_postprocess.prioritize_requested_chart(plans, preferred_chart)


def _pick_hist_x(
    df: pd.DataFrame,
    primary: Optional[str],
    seed_spec: Dict[str, Any],
) -> Optional[str]:
    if primary and primary in df.columns and pdt.is_numeric_dtype(df[primary]) and not _is_identifier_col(primary):
        return primary
    seed_y = seed_spec.get("y")
    if isinstance(seed_y, str) and seed_y in df.columns and pdt.is_numeric_dtype(df[seed_y]) and not _is_identifier_col(seed_y):
        return seed_y
    seed_x = seed_spec.get("x")
    if isinstance(seed_x, str) and seed_x in df.columns and pdt.is_numeric_dtype(df[seed_x]) and not _is_identifier_col(seed_x):
        return seed_x
    for col in df.columns:
        if _is_identifier_col(col):
            continue
        if pdt.is_numeric_dtype(df[col]):
            return col
    return None


def _pick_hist_group(
    df: pd.DataFrame,
    group_var: Optional[str],
    seed_spec: Dict[str, Any],
) -> Optional[str]:
    if group_var and group_var in df.columns and not _is_identifier_col(group_var):
        if pdt.is_string_dtype(df[group_var]) or pdt.is_categorical_dtype(df[group_var]):
            if _is_low_cardinality(df, group_var, 30):
                return group_var
    seed_group = seed_spec.get("group")
    if isinstance(seed_group, str) and seed_group in df.columns and not _is_identifier_col(seed_group):
        if pdt.is_string_dtype(df[seed_group]) or pdt.is_categorical_dtype(df[seed_group]):
            if _is_low_cardinality(df, seed_group, 30):
                return seed_group
    return None


def _ensure_hist_plan(
    plans: List[Dict[str, Any]],
    preferred_chart: Optional[str],
    primary: Optional[str],
    group_var: Optional[str],
    column_only_plan: Optional[Dict[str, Any]],
    df: pd.DataFrame,
) -> List[Dict[str, Any]]:
    preferred = str(preferred_chart or "").strip().lower()
    if preferred != "hist":
        return plans
    if any(str((p.get("chart_spec") or {}).get("chart_type") or "").lower() == "hist" for p in plans):
        return plans

    seed = column_only_plan or (plans[0] if plans else {})
    seed_spec = (seed or {}).get("chart_spec") or {}
    x = _pick_hist_x(df, primary, seed_spec)
    if not x:
        return plans
    g = _pick_hist_group(df, group_var, seed_spec)

    spec: Dict[str, Any] = {"chart_type": "hist", "x": x}
    if g:
        spec["group"] = g

    new_plan = {
        "chart_spec": spec,
        "reason": "히스토그램 요청이 감지되어 분포형(hist) 차트를 우선 생성했습니다.",
    }
    return [new_plan, *plans]


def _ensure_confusion_matrix_plan(
    plans: List[Dict[str, Any]],
    preferred_chart: Optional[str],
    df: pd.DataFrame,
    column_only_plan: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    preferred = str(preferred_chart or "").strip().lower()
    if preferred != "confusion_matrix":
        return plans
    if any(str((p.get("chart_spec") or {}).get("chart_type") or "").lower() == "confusion_matrix" for p in plans):
        return plans

    seed_spec = (column_only_plan or (plans[0] if plans else {}) or {}).get("chart_spec") or {}
    cm_axes = _pick_confusion_matrix_axes(
        df,
        seed_x=seed_spec.get("x"),
        seed_y=seed_spec.get("y"),
        seed_value=seed_spec.get("group"),
    )
    if not cm_axes:
        return plans

    spec: Dict[str, Any] = {
        "chart_type": "confusion_matrix",
        "x": cm_axes["x"],
        "y": cm_axes["y"],
    }
    if cm_axes.get("value"):
        spec["group"] = cm_axes["value"]
        spec["agg"] = "sum"

    new_plan = {
        "chart_spec": spec,
        "reason": "혼동행렬 요청이 감지되어 실제값-예측값 매트릭스를 우선 생성했습니다.",
    }
    return [new_plan, *plans]


def _ensure_bar_plan(
    plans: List[Dict[str, Any]],
    style: Dict[str, bool],
    primary: Optional[str],
    group_var: Optional[str],
    column_only_plan: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not style.get("requested"):
        return plans
    if any(str((p.get("chart_spec") or {}).get("chart_type") or "").lower().startswith("bar") for p in plans):
        return plans

    seed = column_only_plan or (plans[0] if plans else {})
    seed_spec = (seed or {}).get("chart_spec") or {}
    x = seed_spec.get("x") or group_var
    y = seed_spec.get("y") or primary
    g = seed_spec.get("group") or None
    if not x or not y:
        return plans

    preferred = _bar_preferred_chart_type(style) or "bar_basic"
    needs_group = preferred in {
        "bar_grouped",
        "bar_stacked",
        "bar_hstack",
        "bar_percent",
        "bar_hpercent",
    }
    if needs_group and not g:
        if preferred in {"bar_hstack", "bar_hpercent"}:
            preferred = "bar_hgroup"
        else:
            preferred = "bar_basic"

    spec: Dict[str, Any] = {
        "chart_type": preferred,
        "x": x,
        "y": y,
    }
    if g and preferred in {
        "bar_grouped",
        "bar_stacked",
        "bar_hstack",
        "bar_percent",
        "bar_hpercent",
    }:
        spec["group"] = g
    if preferred in {"bar_hgroup", "bar_hstack", "bar_hpercent"}:
        spec["orientation"] = "h"
    if preferred in {"bar_stacked", "bar_hstack", "bar_percent", "bar_hpercent"}:
        spec["bar_mode"] = "stack"
    elif preferred in {"bar_grouped", "bar_hgroup"}:
        spec["bar_mode"] = "group"

    new_plan = {
        "chart_spec": spec,
        "reason": "막대 차트 요청이 감지되어 BAR 유형을 우선 생성했습니다.",
    }
    return [new_plan, *plans]


def _apply_default_max_categories(plans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for plan in plans:
        spec = plan.get("chart_spec") or {}
        chart_type = str(spec.get("chart_type") or "").lower()
        if chart_type not in _MAX_CATEGORY_CHART_TYPES:
            continue
        if spec.get("max_categories") is None:
            spec["max_categories"] = _DEFAULT_MAX_CATEGORIES
    return plans


def _filter_constant_y_bar_plans(
    plans: List[Dict[str, Any]],
    df: pd.DataFrame,
    failure_reasons: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    dropped_y: set[str] = set()

    for plan in plans:
        spec = plan.get("chart_spec") or {}
        chart_type = str(spec.get("chart_type") or "").lower()
        if not chart_type.startswith("bar"):
            filtered.append(plan)
            continue

        y = spec.get("y")
        if not isinstance(y, str) or y not in df.columns:
            filtered.append(plan)
            continue

        numeric_y = pd.to_numeric(df[y], errors="coerce").dropna()
        if numeric_y.empty or numeric_y.nunique(dropna=True) > 1:
            filtered.append(plan)
            continue

        dropped_y.add(y)

    for y_col in sorted(dropped_y):
        _record_failure(failure_reasons, f"bar_skipped_constant_y: {y_col}")
    if dropped_y:
        log_event(
            "rule_engine.bar.constant_y_skipped",
            {"y_columns": sorted(dropped_y), "dropped_count": len(dropped_y)},
        )
    return filtered


# 입력: intent, group_var, time_var, available_columns, context_flags
# 출력: None | Exception
# 임상 규칙 위반 시 예외 발생 (잘못된 차트 생성 방지)


def validate_plan(
    intent: str | None,
    group_var: Optional[str],
    time_info: Optional[Dict[str, str]],
    available_columns: List[str],
    context_flags: Dict[str, bool],
) -> None:
    cols_lower = {c.lower() for c in available_columns}
    group_lower = (group_var or "").lower()

    # 1) ICU/입실 후 trend는 stay_id 없이 생성 금지, subject_id trajectory 금지
    # 이유: ICU 경과는 ICU 체류(STAY_ID) 단위가 아니면 해석이 무의미하다.
    if intent == "trend" and context_flags.get("icu_context"):
        if "stay_id" not in cols_lower:
            raise ValueError("ICU/입실 후 trend는 stay_id 없이 생성할 수 없습니다.")
        if "intime" not in cols_lower:
            raise ValueError("ICU/입실 후 trend는 ICUSTAYS.INTIME 조인이 필요합니다.")
        if not any(t in cols_lower for t in _TIME_CANDIDATES):
            raise ValueError("ICU/입실 후 trend는 시간 컬럼(chart/start/end/out/store time)이 필요합니다.")
        if group_lower in _FORBIDDEN_TRAJECTORY:
            raise ValueError(
                "ICU/입실 후 trend에서 subject_id/patient_id trajectory는 금지입니다.")
        # 경과 시간 축 강제
        # 이유: ICU 맥락에서 calendar time은 입실 기준을 흐려 의미 오류를 만든다.
        if not time_info or time_info.get("type") != "elapsed":
            raise ValueError("ICU/입실 후 trend는 경과시간 축만 허용됩니다.")
        if time_info.get("expr") is None:
            raise ValueError("ICU/입실 후 trend는 경과시간 파생 컬럼이 필요합니다.")

    # 2) trend line group은 trajectory 단위만 허용
    # 이유: 개인/입원 단위 trajectory가 아닌 집계 라인은 임상 해석을 왜곡한다.
    if intent == "trend" and group_lower:
        if group_lower in _FORBIDDEN_TRAJECTORY:
            raise ValueError("trend의 group_var로 subject_id/patient_id는 금지입니다.")
        if group_lower not in _ALLOWED_TRAJECTORY:
            if context_flags.get("icu_context") or context_flags.get("admit_context"):
                raise ValueError("ICU/입원 trend의 group_var는 stay_id/hadm_id만 허용됩니다.")
            if any(tok in group_lower for tok in _IDENTIFIER_COLS):
                raise ValueError("trend의 group_var로 식별자 컬럼은 금지입니다.")

    # 3) comparison/distribution에서 식별자 그룹 금지
    # 이유: 식별자 기준 그룹은 과도한 분할로 해석 불가능.
    if intent in ("comparison", "distribution") and group_lower:
        if any(tok in group_lower for tok in _IDENTIFIER_COLS):
            raise ValueError("comparison/distribution에서 식별자 그룹은 금지입니다.")

    # 4) correlation은 식별자 변수 제외
    # 이유: 식별자와의 상관은 통계적으로 의미가 없다.
    if intent == "correlation":
        if any(tok in group_lower for tok in _IDENTIFIER_COLS):
            raise ValueError("correlation에서 식별자 group은 금지입니다.")

    # 5) '후 N일' 맥락은 경과시간 축 필수
    # 이유: 기준 시점 이후 경과를 묻는 질문은 경과시간 파생 없이는 답할 수 없다.
    if intent == "trend" and context_flags.get("post_days"):
        if not time_info or time_info.get("type") != "elapsed" or time_info.get("expr") is None:
            raise ValueError("'후 N일' 맥락은 경과시간 파생 컬럼이 필요합니다.")

    # 6) 입원 기준 trend는 ADMITTIME 필요
    # 이유: 입원 기준 분석은 ADMISSIONS 기준 시간이 필수다.
    if intent == "trend" and context_flags.get("admit_context"):
        if "admittime" not in cols_lower:
            raise ValueError("입원 기준 trend는 ADMISSIONS.ADMITTIME 조인이 필요합니다.")
        if not any(t in cols_lower for t in _TIME_CANDIDATES):
            raise ValueError("입원 기준 trend는 시간 컬럼(chart/start/end/out/store time)이 필요합니다.")

    # 7) INPUT/OUTPUT 계열 rate/amount는 시간 binning 없이 의미 없음
    # 이유: rate/amount는 시간 집계 없으면 임상적으로 해석 불가.
    if intent == "trend":
        has_rate_amount = any("rate" in c or "amount" in c for c in cols_lower)
        if has_rate_amount and (not time_info or time_info.get("expr") in (None, "charttime")):
            raise ValueError("rate/amount trend는 시간 binning(경과시간 포함)이 필요합니다.")

# 입력: intent, context_flags, available_columns
# 출력: str | None
# ICU/입실 문맥이면 stay_id 최우선, 금지 그룹은 반환하지 않음


def choose_group_var(
    intent: Optional[str],
    context_flags: Dict[str, bool],
    available_columns: List[str],
) -> Optional[str]:
    cols = [c.lower() for c in available_columns]

    if context_flags.get("icu_context"):
        if "stay_id" in cols:
            return "stay_id"
        if "hadm_id" in cols:
            return "hadm_id"
        return None

    # trend는 trajectory 단위만 허용
    if intent == "trend":
        if "stay_id" in cols:
            return "stay_id"
        if "hadm_id" in cols:
            return "hadm_id"
        return None

    # distribution/comparison은 low-cardinality만 허용
    group = _first_matching_col(available_columns, list(_ALLOWED_GROUP_COLS))
    if group and group.lower() in _FORBIDDEN_GROUP_COLS:
        return None
    return group

# 입력: intent, context_flags, available_columns
# 출력: dict | None
# 입실 후 문맥에서는 경과시간 축만 허용


def derive_time_var(
    intent: Optional[str],
    context_flags: Dict[str, bool],
    available_columns: List[str],
) -> Optional[Dict[str, str]]:
    cols_lower = {c.lower() for c in available_columns}
    if intent != "trend":
        return None

    # ICU 맥락: 경과시간 강제 (Oracle: DATE - DATE = days)
    if context_flags.get("icu_context"):
        elapsed_col = _find_elapsed_column(available_columns, "icu")
        if not elapsed_col:
            return {"type": "elapsed", "expr": None, "source": "charttime - intime", "unit": "day"}
        return {"type": "elapsed", "expr": elapsed_col, "source": "charttime - intime", "unit": "day"}

    # 입원 맥락: 경과시간 강제
    if context_flags.get("admit_context"):
        elapsed_col = _find_elapsed_column(available_columns, "admit")
        if not elapsed_col:
            return {"type": "elapsed", "expr": None, "source": "charttime - admittime", "unit": "day"}
        return {"type": "elapsed", "expr": elapsed_col, "source": "charttime - admittime", "unit": "day"}

    # 단순 시간 추세: 이용 가능한 시간 컬럼 우선 선택
    chart_col = _first_matching_col(available_columns, ["charttime", "chart_time", "charttimestamp"])
    if not chart_col:
        chart_col = _first_time_col(available_columns)
    if chart_col:
        return {"type": "calendar", "expr": chart_col, "unit": "day"}
    return None

# 입력: intent_info, df
# 출력: List[Dict[str, Any]]
# intent_info 기반으로 분석 플랜 여러 개 생성


def plan_analyses(
    intent_info: Dict[str, Any],
    df: pd.DataFrame,
    retrieved_context: Optional[str] = None,
    retry_mode: str = "normal",
    failure_reasons: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """intent_info를 기반으로 분석 플랜 여러 개를 반환."""
    retry_mode = (retry_mode or "normal").lower()
    intent = intent_info.get("analysis_intent")
    primary = intent_info.get("primary_outcome")
    user_query = intent_info.get("user_query")
    # RAG 컨텍스트에서 예시 chart_spec 추출 (가능하면 우선 제안)
    suggested_plan = _extract_chart_spec_from_context(retrieved_context, df)
    column_only_plan = _infer_chart_from_columns(df)
    context_flags = intent_info.get("context_flags") or _infer_context_flags(
        user_query, list(df.columns))
    bar_style = _infer_bar_style(user_query)
    preferred_chart = str(intent_info.get("recommended_chart") or "").strip().lower()
    explicit_chart_preference = _infer_chart_preference(user_query) or ""
    if explicit_chart_preference:
        # 사용자가 차트 타입을 명시하면 intent extractor 결과보다 우선한다.
        preferred_chart = explicit_chart_preference

    # time/group은 규칙 기반으로 강제 선택
    time_info = derive_time_var(intent, context_flags, list(df.columns))
    group_var = intent_info.get("group_var") or choose_group_var(
        intent, context_flags, list(df.columns))
    time_var = time_info.get(
        "expr") if time_info else intent_info.get("time_var")

    # 재시도 모드에서는 우선 복구 가능성이 높은 단순 플랜으로 유도한다.
    if retry_mode == "relaxed" and intent in ("trend", "distribution", "comparison", "proportion"):
        group_var = None

    # low-cardinality 검사 (group_var가 허용 리스트여도 값이 과다하면 제거)
    if group_var and group_var in df.columns and not _is_low_cardinality(df, group_var, 30):
        group_var = None
    # intent별 금지 group_var 정리
    if group_var:
        group_lower = group_var.lower()
        if intent != "trend" and group_lower in _FORBIDDEN_GROUP_COLS:
            group_var = None
        elif intent == "trend" and group_lower not in _ALLOWED_TRAJECTORY:
            if context_flags.get("icu_context") or context_flags.get("admit_context"):
                group_var = None
            elif _is_identifier_col(group_var):
                group_var = None
            elif group_var in df.columns and not _is_low_cardinality(df, group_var, 20):
                group_var = None
        elif intent in ("distribution", "comparison") and _is_identifier_col(group_var):
            group_var = None
    multisplit_slots = _infer_multisplit_bar_slots(
        user_query,
        df,
        seed_axis=group_var,
    )
    intent_multisplit_axis = intent_info.get("multisplit_axis")
    intent_multisplit_group = intent_info.get("multisplit_group")
    intent_multisplit_secondary = intent_info.get("multisplit_secondary_group")
    if isinstance(intent_multisplit_axis, str) and intent_multisplit_axis in df.columns:
        multisplit_slots["axis"] = intent_multisplit_axis
    if isinstance(intent_multisplit_group, str) and intent_multisplit_group in df.columns:
        multisplit_slots["group"] = intent_multisplit_group
    if isinstance(intent_multisplit_secondary, str) and intent_multisplit_secondary in df.columns:
        multisplit_slots["secondary_group"] = intent_multisplit_secondary
    multisplit_axis = multisplit_slots.get("axis")
    multisplit_group = multisplit_slots.get("group")
    multisplit_secondary_group = multisplit_slots.get("secondary_group")

    log_event(
        "rule_engine.start",
        {
            "intent": intent,
            "primary": primary,
            "time_var": time_var,
            "group_var": group_var,
            "multisplit_axis": multisplit_axis,
            "multisplit_group": multisplit_group,
            "multisplit_secondary_group": multisplit_secondary_group,
            "retry_mode": retry_mode,
        },
    )

    plans: List[Dict[str, Any]] = []
    if suggested_plan:
        spec = suggested_plan.get("chart_spec", {})
        if intent == "trend":
            # trend는 시간축 정합성 확인
            if time_info and spec.get("x") == time_info.get("expr"):
                try:
                    validate_plan(
                        intent,
                        spec.get("group"),
                        time_info,
                        list(df.columns),
                        context_flags,
                    )
                except Exception as exc:
                    _record_failure(
                        failure_reasons,
                        f"suggested_plan_blocked: {str(exc)}",
                    )
                    log_event(
                        "rule_engine.suggested_plan.blocked",
                        {"reason": str(exc), "chart_spec": spec},
                    )
                else:
                    plans.append(suggested_plan)
            else:
                _record_failure(
                    failure_reasons,
                    "suggested_plan_mismatch: trend x-axis does not match derived time axis",
                )
        else:
            plans.append(suggested_plan)

    # suggested_plan이 차단/미스매치된 경우에도 column 기반 플랜으로 복구를 시도한다.
    if not plans and column_only_plan:
        plans.append(column_only_plan)

    if intent == "trend" and time_var and primary:
        try:
            validate_plan(intent, group_var, time_info,
                          list(df.columns), context_flags)
        except Exception as exc:
            _record_failure(failure_reasons, f"trend_blocked: {str(exc)}")
            # 규칙 위반 시 trend 플랜을 만들지 않는다(의미 보존 우선)
            log_event("rule_engine.trend.blocked",
                      {"reason": str(exc)})
            if retry_mode == "relaxed":
                # relaxed 모드에서는 엄격한 trajectory 라인 대신 분포형 대안을 허용한다.
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": "box",
                            "x": time_var,
                            "y": primary,
                        },
                        "reason": "재시도 모드: trend 제약으로 line이 불가해 분포형 대안을 생성했습니다.",
                    }
                )
        else:
            patient_group = _pick_patient_group(df)
            line_group = patient_group or group_var
            line_chart_type = "line_scatter" if preferred_chart == "line_scatter" else "line"
            # trajectory 그룹이 있으면 그룹 라인/라인+스캐터 생성
            if line_group:
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": line_chart_type,
                            "x": time_var,
                            "y": primary,
                            "group": line_group,
                        },
                        "reason": "환자별 변화(trajectory)를 직접 확인할 수 있습니다." if line_chart_type == "line" else "시간 흐름에서 선과 점을 함께 확인할 수 있습니다.",
                    }
                )
                if line_group in df.columns and _is_low_cardinality(df, line_group, 8):
                    plans.append(
                        {
                            "chart_spec": {
                                "chart_type": "area",
                                "x": time_var,
                                "y": primary,
                                "group": line_group,
                            },
                            "reason": "면적 차트로 시간대별 누적 기여도를 시각적으로 확인할 수 있습니다.",
                        }
                    )
            elif not context_flags.get("icu_context") and not context_flags.get("admit_context"):
                # 일반 추세에서는 그룹이 없어도 라인 계열 차트를 허용한다.
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": line_chart_type,
                            "x": time_var,
                            "y": primary,
                        },
                        "reason": "시간 축 기반 집계 추세를 확인할 수 있습니다." if line_chart_type == "line" else "선과 점을 함께 사용해 추세와 개별 관측치를 같이 확인할 수 있습니다.",
                    }
                )
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": "area",
                            "x": time_var,
                            "y": primary,
                        },
                        "reason": "면적 차트로 시간 흐름의 규모 변화(볼륨)를 함께 볼 수 있습니다.",
                    }
                )
            plans.append(
                {
                    "chart_spec": {
                        "chart_type": "box",
                        "x": time_var,
                        "y": primary,
                    },
                    "reason": "시간 구간별 분포와 이상치를 함께 확인할 수 있습니다.",
                }
            )
            if group_var:
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": "box",
                            "x": group_var,
                            "y": primary,
                        },
                        "reason": "그룹별 분포 차이와 이상치를 추가로 비교할 수 있습니다.",
                    }
                )
    elif intent == "distribution" and primary:
        plans.append(
            {
                "chart_spec": {
                    "chart_type": "hist",
                    "x": primary,
                },
                "reason": "전체 분포를 확인하기에 적합합니다.",
            }
        )
        plans.append(
            {
                "chart_spec": {
                    "chart_type": "violin",
                    "y": primary,
                },
                "reason": "밀도와 이상치를 동시에 보여주기 위해 바이올린 차트를 추가합니다.",
            }
        )
        if group_var:
            plans.append(
                {
                    "chart_spec": {
                        "chart_type": "box",
                        "x": group_var,
                        "y": primary,
                    },
                    "reason": "그룹별 분포 차이를 비교할 수 있습니다.",
                }
            )
            plans.append(
                {
                    "chart_spec": {
                        "chart_type": "violin",
                        "x": group_var,
                        "y": primary,
                    },
                    "reason": "그룹별 분포 형태 차이를 밀도 기반으로 비교할 수 있습니다.",
                }
            )
    elif intent == "comparison" and primary:
        if multisplit_axis and multisplit_axis in df.columns and (multisplit_group or multisplit_secondary_group):
            detailed_spec: Dict[str, Any] = {
                "chart_type": "bar_grouped",
                "x": multisplit_axis,
                "y": primary,
                "bar_mode": "group",
            }
            if multisplit_group:
                detailed_spec["group"] = multisplit_group
            elif multisplit_secondary_group:
                detailed_spec["group"] = multisplit_secondary_group
            if multisplit_group and multisplit_secondary_group:
                detailed_spec["secondary_group"] = multisplit_secondary_group
            plans.append(
                {
                    "chart_spec": detailed_spec,
                    "reason": "질문의 복합 분할(연령/성별/사망-생존)을 반영해 막대 차트 슬롯을 조합했습니다.",
                }
            )
            if multisplit_group and multisplit_secondary_group:
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": "bar_stacked",
                            "x": multisplit_axis,
                            "y": primary,
                            "group": multisplit_secondary_group,
                            "secondary_group": multisplit_group,
                            "bar_mode": "stack",
                        },
                        "reason": "보조 분할 축을 누적으로 바꾼 대안 시각화입니다.",
                    }
                )
        if group_var:
            # BAR variants: simple -> detailed
            plans.append(
                {
                    "chart_spec": {
                        "chart_type": "bar_basic",
                        "x": group_var,
                        "y": primary,
                    },
                    "reason": "가장 단순한 막대 비교(기본형)입니다.",
                }
            )
            plans.append(
                {
                    "chart_spec": {
                        "chart_type": "lollipop",
                        "x": group_var,
                        "y": primary,
                    },
                    "reason": "Graph Gallery 스타일의 로리팝 차트로 순위/격차를 선명하게 보여줍니다.",
                }
            )
            plans.append(
                {
                    "chart_spec": {
                        "chart_type": "box",
                        "x": group_var,
                        "y": primary,
                    },
                    "reason": "그룹별 분포 차이와 이상치를 비교하기 좋습니다.",
                }
            )
            second_group = multisplit_secondary_group or _pick_secondary_group(df, exclude=group_var)
            if second_group:
                if (
                    group_var in df.columns
                    and second_group in df.columns
                    and _is_low_cardinality(df, group_var, 16)
                    and _is_low_cardinality(df, second_group, 16)
                ):
                    plans.append(
                        {
                            "chart_spec": {
                                "chart_type": "treemap",
                                "x": group_var,
                                "group": second_group,
                                "y": primary,
                                "agg": "sum",
                            },
                            "reason": "Graph Gallery 스타일의 트리맵으로 상·하위 구성 비율을 동시에 요약합니다.",
                        }
                    )
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": "bar_grouped",
                            "x": group_var,
                            "y": primary,
                            "group": second_group,
                            "bar_mode": "group",
                        },
                        "reason": "그룹형 막대로 카테고리별 세부 비교를 제공합니다.",
                    }
                )
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": "bar_stacked",
                            "x": group_var,
                            "y": primary,
                            "group": second_group,
                            "bar_mode": "stack",
                        },
                        "reason": "누적 막대로 전체 대비 구성 기여를 확인합니다.",
                    }
                )
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": "bar_hstack",
                            "x": group_var,
                            "y": primary,
                            "group": second_group,
                            "bar_mode": "stack",
                            "orientation": "h",
                        },
                        "reason": "수평 누적 막대로 라벨 가독성을 높입니다.",
                    }
                )
                if (
                    group_var in df.columns
                    and second_group in df.columns
                    and _is_low_cardinality(df, group_var, 20)
                    and _is_low_cardinality(df, second_group, 20)
                ):
                    plans.append(
                        {
                            "chart_spec": {
                                "chart_type": "heatmap",
                                "x": group_var,
                                "y": second_group,
                                "group": primary,
                                "agg": "sum",
                            },
                            "reason": "Graph Gallery 스타일의 히트맵으로 두 범주 축의 강도를 한 화면에서 비교합니다.",
                        }
                    )
                    cm_spec: Dict[str, Any] = {
                        "chart_type": "confusion_matrix",
                        "x": second_group,
                        "y": group_var,
                    }
                    if primary in df.columns and pdt.is_numeric_dtype(df[primary]):
                        cm_spec["group"] = primary
                        cm_spec["agg"] = "sum"
                    plans.append(
                        {
                            "chart_spec": cm_spec,
                            "reason": "두 범주 조합의 오분류/집중 구간을 확인하기 위해 혼동행렬 스타일 시각화를 추가합니다.",
                        }
                    )
                if bar_style.get("percent"):
                    plans.append(
                        {
                            "chart_spec": {
                                "chart_type": "bar_percent",
                                "x": group_var,
                                "y": primary,
                                "group": second_group,
                                "bar_mode": "stack",
                            },
                            "reason": "100% 누적 막대로 비율 중심 비교를 제공합니다.",
                        }
                    )
                if not bar_style.get("requested"):
                    plans.append(
                        {
                            "chart_spec": {
                                "chart_type": "nested_pie",
                                "x": group_var,
                                "group": second_group,
                                "y": primary,
                                "agg": "sum",
                            },
                            "reason": "비교 질문에서 상·하위 그룹 기여도를 한 번에 보여줍니다.",
                        }
                    )
            elif bar_style.get("horizontal"):
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": "bar_hgroup",
                            "x": group_var,
                            "y": primary,
                            "bar_mode": "group",
                            "orientation": "h",
                        },
                        "reason": "수평 막대로 비교 가독성을 높입니다.",
                    }
                )
    elif intent == "proportion" and primary:
        # 비율 질문은 추세/그룹 비교 둘 다 가능하므로 time 우선, 없으면 group 기반 bar.
        if (
            not time_var
            and multisplit_axis
            and multisplit_axis in df.columns
            and (multisplit_group or multisplit_secondary_group)
        ):
            detailed_spec: Dict[str, Any] = {
                "chart_type": "bar_percent" if bar_style.get("percent") else "bar_grouped",
                "x": multisplit_axis,
                "y": primary,
                "bar_mode": "stack" if bar_style.get("percent") else "group",
            }
            if multisplit_group:
                detailed_spec["group"] = multisplit_group
            elif multisplit_secondary_group:
                detailed_spec["group"] = multisplit_secondary_group
            if multisplit_group and multisplit_secondary_group:
                detailed_spec["secondary_group"] = multisplit_secondary_group
            plans.append(
                {
                    "chart_spec": detailed_spec,
                    "reason": "비율 질문의 복합 분할(연령/성별/사망-생존)을 막대형 슬롯 조합으로 반영했습니다.",
                }
            )
        if time_var:
            plans.append(
                {
                    "chart_spec": {
                        "chart_type": "line",
                        "x": time_var,
                        "y": primary,
                        "group": group_var,
                    },
                    "reason": "시간에 따른 비율 변화를 확인할 수 있습니다.",
                }
            )
        elif group_var:
            if preferred_chart == "lollipop":
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": "lollipop",
                            "x": group_var,
                            "y": primary,
                        },
                        "reason": "사용자가 로리팝 차트를 명시해 순위/격차 중심 비교를 우선 제공합니다.",
                    }
                )
            if not bar_style.get("requested") and group_var in df.columns and _is_low_cardinality(df, group_var, 12):
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": "pie",
                            "x": group_var,
                            "y": primary,
                            "agg": "sum",
                        },
                        "reason": "그룹 비율을 직관적으로 비교하기 위해 파이 차트를 우선 제공합니다.",
                    }
                )
            plans.append(
                {
                    "chart_spec": {
                        "chart_type": "bar_basic",
                        "x": group_var,
                        "y": primary,
                    },
                    "reason": "그룹별 비율 차이를 확인할 수 있습니다.",
                }
            )
            second_group = multisplit_secondary_group or _pick_secondary_group(df, exclude=group_var)
            if second_group:
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": "bar_grouped",
                            "x": group_var,
                            "y": primary,
                            "group": second_group,
                            "bar_mode": "group",
                        },
                        "reason": "그룹형 막대로 세부 비율을 비교할 수 있습니다.",
                    }
                )
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": "bar_stacked",
                            "x": group_var,
                            "y": primary,
                            "group": second_group,
                            "bar_mode": "stack",
                        },
                        "reason": "누적 막대로 전체 내 구성 비중을 확인할 수 있습니다.",
                    }
                )
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": "bar_hstack",
                            "x": group_var,
                            "y": primary,
                            "group": second_group,
                            "bar_mode": "stack",
                            "orientation": "h",
                        },
                        "reason": "수평 누적 막대로 라벨 겹침을 줄이고 가독성을 높입니다.",
                    }
                )
                if (
                    group_var in df.columns
                    and second_group in df.columns
                    and _is_low_cardinality(df, group_var, 20)
                    and _is_low_cardinality(df, second_group, 20)
                ):
                    cm_spec: Dict[str, Any] = {
                        "chart_type": "confusion_matrix",
                        "x": second_group,
                        "y": group_var,
                    }
                    if primary in df.columns and pdt.is_numeric_dtype(df[primary]):
                        cm_spec["group"] = primary
                        cm_spec["agg"] = "sum"
                    plans.append(
                        {
                            "chart_spec": cm_spec,
                            "reason": "두 범주 조합의 혼동/집중 구간을 보기 위해 혼동행렬형 시각화를 추가합니다.",
                        }
                    )
                if bar_style.get("percent"):
                    plans.append(
                        {
                            "chart_spec": {
                                "chart_type": "bar_percent",
                                "x": group_var,
                                "y": primary,
                                "group": second_group,
                                "bar_mode": "stack",
                            },
                            "reason": "100% 누적 막대로 구성 비율을 직접 비교합니다.",
                        }
                    )
                if not bar_style.get("requested"):
                    plans.append(
                        {
                            "chart_spec": {
                                "chart_type": "nested_pie",
                                "x": group_var,
                                "group": second_group,
                                "y": primary,
                                "agg": "sum",
                            },
                            "reason": "상·하위 비율을 동시에 보여주기 위해 중첩 파이를 추가합니다.",
                        }
                    )
            elif bar_style.get("horizontal"):
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": "bar_hgroup",
                            "x": group_var,
                            "y": primary,
                            "bar_mode": "group",
                            "orientation": "h",
                        },
                        "reason": "수평 막대로 그룹별 비율을 비교합니다.",
                    }
                )
    elif intent == "correlation" and primary:
        if not _is_identifier_col(primary):
            other = None
            for col in df.columns:
                if col == primary:
                    continue
                if pdt.is_string_dtype(df[col]) or pdt.is_categorical_dtype(df[col]):
                    continue
                if _is_identifier_col(col):
                    continue
                other = col
                break
            if other:
                chart_type = "scatter"
                if preferred_chart in ("line", "line_scatter", "dynamic_scatter", "scatter"):
                    chart_type = preferred_chart
                if chart_type == "dynamic_scatter":
                    animation_frame = _pick_animation_frame_col(df)
                    numeric_cols = _numeric_candidates(df, exclude=[animation_frame or ""])
                    x_col = _pick_numeric_by_tokens(numeric_cols, ["age", "x", "bill", "amount", "value"]) or (numeric_cols[0] if numeric_cols else other)
                    y_col = _pick_numeric_by_tokens(numeric_cols, ["los", "rate", "cnt", "count", "score", "days", "y"]) or primary
                    if y_col == x_col and len(numeric_cols) >= 2:
                        y_col = next((c for c in numeric_cols if c != x_col), y_col)
                    size_col = _pick_numeric_by_tokens(numeric_cols, ["cnt", "count", "size", "volume"]) or _pick_size_col(df, exclude=[x_col or "", y_col or "", animation_frame or ""])
                    color_group = group_var if group_var in df.columns else _pick_secondary_group(df)
                    if animation_frame and x_col and y_col:
                        plans.append(
                            {
                                "chart_spec": {
                                    "chart_type": "dynamic_scatter",
                                    "x": x_col,
                                    "y": y_col,
                                    "group": color_group,
                                    "size": size_col,
                                    "animation_frame": animation_frame,
                                },
                                "reason": "시간/프레임에 따라 점의 이동을 보여주는 동적 산점도입니다.",
                            }
                        )
                    else:
                        plans.append(
                            {
                                "chart_spec": {
                                    "chart_type": "scatter",
                                    "x": other,
                                    "y": primary,
                                    "group": color_group,
                                },
                                "reason": "동적 산점도 요청이지만 프레임 컬럼이 없어 정적 산점도로 대체했습니다.",
                            }
                        )
                elif chart_type == "line_scatter":
                    plans.append(
                        {
                            "chart_spec": {
                                "chart_type": "line_scatter",
                                "x": other,
                                "y": primary,
                                "group": group_var if group_var in df.columns else None,
                            },
                            "reason": "선과 점을 함께 사용해 관계와 변동을 동시에 표시합니다.",
                        }
                    )
                elif chart_type == "line":
                    plans.append(
                        {
                            "chart_spec": {
                                "chart_type": "line",
                                "x": other,
                                "y": primary,
                                "group": group_var if group_var in df.columns else None,
                            },
                            "reason": "두 변수의 변화 패턴을 선형 흐름으로 확인합니다.",
                        }
                    )
                else:
                    plans.append(
                        {
                            "chart_spec": {
                                "chart_type": "scatter",
                                "x": other,
                                "y": primary,
                                "group": group_var if group_var in df.columns else None,
                            },
                            "reason": "두 변수의 상관관계를 시각화합니다.",
                        }
                    )
    else:
        # 기본: 컬럼 개요 수준의 간단한 차트
        if primary:
            plans.append(
                {
                    "chart_spec": {"chart_type": "hist", "x": primary},
                    "reason": "기본 분포를 확인하기 위한 플랜입니다.",
                }
            )

    # 임상 의료진 대상 힌트는 후보로만 사용 (distribution intent에선 group 사용 금지)
    if _has_column_hint(df, _CLINICAL_HINTS) and intent not in ("distribution", "comparison"):
        clinical_group = _pick_clinical_group(df) or _pick_safe_group(df)
        if primary and clinical_group:
            plans.append(
                {
                    "chart_spec": {
                        "chart_type": "box",
                        "x": clinical_group,
                        "y": primary,
                    },
                    "reason": "임상 의사결정에 유용한 그룹별 분포를 확인합니다.",
                }
            )

    plans = _ensure_hist_plan(plans, preferred_chart, primary, group_var, column_only_plan, df)
    plans = _ensure_confusion_matrix_plan(plans, preferred_chart, df, column_only_plan)
    plans = _apply_default_max_categories(plans)
    plans = _dedupe_plans(plans)
    plans = _prioritize_requested_chart(plans, preferred_chart)
    plans = _ensure_bar_plan(plans, bar_style, primary, group_var, column_only_plan)
    plans = _prioritize_bar_plans(plans, bar_style)
    plans = _filter_constant_y_bar_plans(plans, df, failure_reasons)
    if not plans:
        _record_failure(
            failure_reasons,
            f"{retry_mode}_plan_empty",
        )
    log_event("rule_engine.plans", {"count": len(plans)})

    return plans
