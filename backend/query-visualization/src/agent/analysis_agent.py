"""Query visualization orchestration pipeline."""
from __future__ import annotations

import os
import re
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional

import pandas as pd
from pandas.api import types as pdt
from dotenv import load_dotenv
from openai import OpenAI

from src.agent import chart_rule_engine, code_generator, intent_extractor, retrieval
from src.config.llm_config import OPENAI_MODEL
from src.db.schema_introspect import summarize_dataframe_schema
from src.models.chart_spec import AnalysisCard, ChartSpec, VisualizationResponse
from src.utils.logging import log_event

_DOTENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_DOTENV_PATH)
_YEAR_TOKEN_RE = re.compile(r"(?<!\d)(1[6-9]\d{2}|20\d{2}|21\d{2}|22\d{2})(?!\d)")
_YEAR_COLUMN_HINT_RE = re.compile(r"(year|yr|연도|년도|date|month|년|월)", re.IGNORECASE)
_HANGUL_CHAR_RE = re.compile(r"[가-힣]")
_LATIN_CHAR_RE = re.compile(r"[A-Za-z]")


def _is_korean_insight_text(text: str) -> bool:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    if not normalized:
        return False
    hangul_count = len(_HANGUL_CHAR_RE.findall(normalized))
    if hangul_count == 0:
        return False
    latin_count = len(_LATIN_CHAR_RE.findall(normalized))
    # Allow mixed tokens (SQL aliases / clinical acronyms), but require Korean dominance.
    return hangul_count >= 8 or (hangul_count * 2 >= max(1, latin_count))


def _translate_insight_to_korean(client: OpenAI, model_name: str, insight: str) -> str:
    source_text = re.sub(r"\s+", " ", str(insight or "")).strip()
    if not source_text:
        return ""
    response = client.responses.create(
        model=model_name,
        input=[
            {
                "role": "system",
                "content": (
                    "너는 임상 데이터 해석 문장을 한국어로만 변환하는 편집기다. "
                    "출력은 한국어 평문만 반환하고, 영어 문장이나 영어 불릿을 포함하지 마라."
                ),
            },
            {
                "role": "user",
                "content": (
                    "다음 해석 문장을 의미 손실 없이 자연스러운 한국어로 변환하라.\n"
                    "- 숫자/단위/코드/컬럼명(ICD, ICU, SQL alias)은 유지할 것\n"
                    "- 결과는 3~6문장으로 작성할 것\n"
                    "- '-습니다/입니다' 종결형 존댓말을 사용할 것\n"
                    "- 마크다운, 번호 목록, 불릿 없이 일반 문장으로만 작성할 것\n\n"
                    f"원문:\n{source_text}"
                ),
            },
        ],
    )
    return (getattr(response, "output_text", None) or "").strip()


def _coerce_year_value(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric != numeric:  # NaN guard
            return None
        rounded = int(round(numeric))
        if abs(numeric - rounded) > 1e-6:
            return None
        if 1600 <= rounded <= 2200:
            return rounded
        return None
    text = str(value or "").strip()
    if not text:
        return None
    match = _YEAR_TOKEN_RE.search(text)
    if not match:
        return None
    year = int(match.group(1))
    if 1600 <= year <= 2200:
        return year
    return None


def _derive_year_bounds_from_df(df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
    bounds: Dict[str, Dict[str, int]] = {}
    if df.empty:
        return bounds
    for column in df.columns:
        col_name = str(column or "").strip()
        if not col_name or not _YEAR_COLUMN_HINT_RE.search(col_name):
            continue
        series = df[column]
        years: List[int] = []
        if pdt.is_datetime64_any_dtype(series):
            years = [
                int(value)
                for value in pd.to_datetime(series, errors="coerce").dt.year.dropna().astype(int).tolist()
                if 1600 <= int(value) <= 2200
            ]
        else:
            years = [
                year
                for year in (_coerce_year_value(value) for value in series.tolist())
                if year is not None
            ]
        if not years:
            continue
        bounds[col_name] = {"min": int(min(years)), "max": int(max(years))}
    return bounds


def _contains_out_of_bounds_year(text: str, *, min_year: int, max_year: int) -> bool:
    for match in _YEAR_TOKEN_RE.finditer(str(text or "")):
        year = int(match.group(1))
        if year < min_year or year > max_year:
            return True
    return False


def _ground_insight_to_year_bounds(
    insight: str,
    *,
    year_bounds: Dict[str, Dict[str, int]],
) -> str:
    normalized = str(insight or "").strip()
    if not normalized or not year_bounds:
        return normalized
    mins = [int(item["min"]) for item in year_bounds.values() if isinstance(item, dict) and "min" in item]
    maxs = [int(item["max"]) for item in year_bounds.values() if isinstance(item, dict) and "max" in item]
    if not mins or not maxs:
        return normalized
    global_min_year = min(mins)
    global_max_year = max(maxs)
    if not _contains_out_of_bounds_year(normalized, min_year=global_min_year, max_year=global_max_year):
        return normalized
    bounded_column, bounded = sorted(year_bounds.items(), key=lambda item: item[0])[0]
    bounded_min = int(bounded.get("min", global_min_year))
    bounded_max = int(bounded.get("max", global_max_year))
    if bounded_min == bounded_max:
        return f"제공된 쿼리 결과 기준 {bounded_column}는 {bounded_min}년 값만 확인됩니다."
    return f"제공된 쿼리 결과 기준 {bounded_column} 범위는 {bounded_min}년부터 {bounded_max}년까지입니다."


def summarize_schema(df: pd.DataFrame) -> Dict[str, Any]:
    return summarize_dataframe_schema(df)


def _add_elapsed_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}

    def _col(*names: str) -> str | None:
        for n in names:
            if n in cols:
                return cols[n]
        return None

    chart_col = _col(
        "charttime",
        "chart_time",
        "charttimestamp",
        "starttime",
        "endtime",
        "storetime",
        "outtime",
        "dischtime",
        "transfertime",
        "ordertime",
    )
    intime_col = _col("intime", "in_time", "icu_intime")
    admittime_col = _col("admittime", "admit_time")

    if chart_col and intime_col:
        try:
            ct = pd.to_datetime(df[chart_col], errors="coerce")
            it = pd.to_datetime(df[intime_col], errors="coerce")
            df["elapsed_icu_days"] = (ct - it).dt.total_seconds() / 86400.0
        except Exception:
            pass

    if chart_col and admittime_col:
        try:
            ct = pd.to_datetime(df[chart_col], errors="coerce")
            at = pd.to_datetime(df[admittime_col], errors="coerce")
            df["elapsed_admit_days"] = (ct - at).dt.total_seconds() / 86400.0
        except Exception:
            pass

    return df


def _stats_snapshot(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    stats: Dict[str, Dict[str, float]] = {}
    if df.empty:
        return stats
    numeric_cols = list(df.select_dtypes(include=["number"]).columns)[:8]
    for col in numeric_cols:
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if series.empty:
            continue
        stats[col] = {
            "min": float(series.min()),
            "q1": float(series.quantile(0.25)),
            "median": float(series.quantile(0.5)),
            "q3": float(series.quantile(0.75)),
            "max": float(series.max()),
            "mean": float(series.mean()),
        }
    return stats


def _numeric_columns_for_visualization(df: pd.DataFrame) -> List[str]:
    numeric_cols: List[str] = []
    for col in df.columns:
        series = df[col]
        if pdt.is_bool_dtype(series):
            continue
        if pdt.is_numeric_dtype(series):
            numeric_cols.append(col)
            continue
        coerced = pd.to_numeric(series, errors="coerce")
        if coerced.notna().any():
            numeric_cols.append(col)
    return numeric_cols


def _fallback_insight(user_query: str, df: pd.DataFrame, analyses: List[AnalysisCard]) -> str:
    row_count = len(df)
    col_count = len(df.columns)
    chart_hint = "차트 추천이 생성되지 않았습니다."
    if analyses:
        first = analyses[0]
        if first.chart_spec and first.chart_spec.chart_type:
            chart_hint = f"주요 추천 차트는 {first.chart_spec.chart_type} 입니다."
        if first.reason:
            chart_hint = f"{chart_hint} {first.reason}"
    stats = _stats_snapshot(df)
    stats_hint = "수치형 통계 요약 대상이 부족합니다."
    if stats:
        top = sorted(
            stats.items(),
            key=lambda kv: (kv[1].get("max", 0.0) - kv[1].get("min", 0.0)),
            reverse=True,
        )[0]
        stats_hint = (
            f"통계표 기준 '{top[0]}'의 범위가 가장 큽니다 "
            f"(min {top[1].get('min'):.3f}, max {top[1].get('max'):.3f})."
        )
    return (
        f"질문 '{user_query}' 기준으로 결과 {row_count}행, {col_count}개 컬럼을 분석했습니다. "
        f"{chart_hint} {stats_hint} 쿼리 결과, 차트, 통계표를 함께 보고 해석하세요."
    )


def _record_failure(failure_reasons: List[str], reason: str) -> None:
    normalized = (reason or "").strip()
    if normalized and normalized not in failure_reasons:
        failure_reasons.append(normalized)


def _has_renderable_chart(analyses: List[AnalysisCard]) -> bool:
    return any(card.figure_json is not None for card in analyses)


def _build_analyses_from_plans(
    plans: List[Dict[str, Any]],
    df: pd.DataFrame,
    failure_reasons: List[str],
    pass_label: str,
    request_id: Optional[str],
) -> List[AnalysisCard]:
    analyses: List[AnalysisCard] = []
    if not plans:
        _record_failure(failure_reasons, f"{pass_label}: no_plans")
        return analyses

    for plan in plans:
        chart_spec_dict = plan.get("chart_spec") or {}
        reason = plan.get("reason")
        chart_type = chart_spec_dict.get("chart_type", "unknown")

        try:
            chart_spec = ChartSpec(**chart_spec_dict)
        except Exception as exc:
            _record_failure(failure_reasons, f"{pass_label}: invalid_chart_spec({chart_type}) - {str(exc)}")
            chart_spec = ChartSpec(chart_type="unknown")

        try:
            chart_result = code_generator.generate_chart(chart_spec_dict, df)
            if chart_result.get("figure_json") is None:
                _record_failure(failure_reasons, f"{pass_label}: empty_figure({chart_type})")
            else:
                log_event(
                    "analysis.chart.success",
                    {"request_id": request_id, "pass": pass_label, "chart_type": chart_type},
                )
        except Exception as exc:
            _record_failure(failure_reasons, f"{pass_label}: chart_error({chart_type}) - {str(exc)}")
            log_event(
                "analysis.chart.error",
                {"request_id": request_id, "pass": pass_label, "error": str(exc)},
                level="error",
            )
            chart_result = {"figure_json": None, "code": None}

        analyses.append(
            AnalysisCard(
                chart_spec=chart_spec,
                reason=reason,
                figure_json=chart_result.get("figure_json"),
                image_data_url=chart_result.get("image_data_url"),
                render_engine=chart_result.get("render_engine"),
                code=chart_result.get("code"),
            )
        )

    if analyses and not _has_renderable_chart(analyses):
        _record_failure(failure_reasons, f"{pass_label}: all_figures_empty")

    return analyses


def _llm_generate_insight(
    user_query: str,
    sql: str,
    df: pd.DataFrame,
    analyses: List[AnalysisCard],
    df_schema: Dict[str, Any],
) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY가 설정되어 있지 않습니다.")

    client = OpenAI(api_key=api_key)
    analysis_briefs = [
        {
            "chart_type": a.chart_spec.chart_type if a.chart_spec else None,
            "x": a.chart_spec.x if a.chart_spec else None,
            "y": a.chart_spec.y if a.chart_spec else None,
            "reason": a.reason,
            "summary": a.summary,
        }
        for a in analyses[:3]
    ]
    stats_snapshot = _stats_snapshot(df)
    model_name = OPENAI_MODEL or "gpt-4o-mini"

    def _build_prompt(max_rows: int) -> str:
        sample_rows = df.head(max_rows).to_dict(orient="records")
        return (
            "다음 정보를 바탕으로 한국어 데이터 분석 인사이트를 작성하라.\n"
            "- 사용자 질문, SQL, 쿼리 결과 샘플, 통계요약, 차트추천 정보를 종합할 것\n"
            "- 출력은 4~6문장, 실행 가능한 인사이트 중심으로 작성\n"
            "- 단순 나열 금지, 핵심 패턴/이상치/해석/주의사항 포함\n"
            "- 출력 언어는 반드시 한국어만 사용하고 영어 문장/영문 불릿을 작성하지 말 것\n"
            "- 문장 종결은 '-습니다/입니다'를 사용할 것\n"
            "- SQL 핵심, 결과 요약, 차트 해석, 주의사항을 한 번에 포함할 것\n\n"
            f"질문: {user_query}\n"
            f"SQL: {sql}\n"
            f"스키마 요약: {df_schema}\n"
            f"통계 요약: {stats_snapshot}\n"
            f"차트 추천: {analysis_briefs}\n"
            f"결과 샘플(최대 {max_rows}행): {sample_rows}\n"
        )

    last_error: Optional[Exception] = None
    for max_rows in (20, 8):
        try:
            response = client.responses.create(
                model=model_name,
                input=[
                    {
                        "role": "system",
                        "content": (
                            "너는 임상 데이터 분석 인사이트 작성 도우미다. "
                            "출력은 한국어만 사용하고 영어 문장을 포함하지 마라."
                        ),
                    },
                    {"role": "user", "content": _build_prompt(max_rows)},
                ],
            )
            text = (getattr(response, "output_text", None) or "").strip()
            if text:
                if _is_korean_insight_text(text):
                    return text
                translated = _translate_insight_to_korean(client, model_name, text)
                if _is_korean_insight_text(translated):
                    return translated
                raise RuntimeError("LLM insight 응답이 한국어 기준을 충족하지 않습니다.")
            raise RuntimeError("LLM insight 응답이 비어 있습니다.")
        except Exception as exc:
            last_error = exc

    raise RuntimeError(f"LLM insight 생성 실패: {str(last_error) if last_error else 'unknown'}")


def analyze_and_visualize(
    user_query: str,
    sql: str,
    df: pd.DataFrame,
    *,
    analysis_query: Optional[str] = None,
    request_id: Optional[str] = None,
) -> VisualizationResponse:
    stage_latency_ms: Dict[str, float] = {}
    t0 = perf_counter()
    failure_reasons: List[str] = []
    fallback_used = False
    fallback_stage: Optional[str] = None
    attempt_count = 1

    def _tick(stage: str, start: float) -> None:
        stage_latency_ms[stage] = round((perf_counter() - start) * 1000.0, 2)

    log_event(
        "analysis.start",
        {
            "request_id": request_id,
            "row_count": len(df),
            "sql_len": len(sql),
            "analysis_query_len": len(str(analysis_query or "")),
        },
    )

    s = perf_counter()
    df = _add_elapsed_columns(df)
    _tick("add_elapsed_columns", s)

    s = perf_counter()
    df_schema = summarize_schema(df)
    _tick("schema_summary", s)
    year_bounds = _derive_year_bounds_from_df(df)

    def _generate_insight_with_fallback(analyses_for_insight: List[AnalysisCard]) -> str:
        s_insight = perf_counter()
        try:
            insight_text = _llm_generate_insight(user_query, sql, df, analyses_for_insight, df_schema)
        except Exception as exc:
            log_event("analysis.insight.error", {"request_id": request_id, "error": str(exc)}, level="error")
            _record_failure(failure_reasons, f"insight_error: {str(exc)}")
            insight_text = _fallback_insight(user_query, df, analyses_for_insight)
        insight_text = _ground_insight_to_year_bounds(insight_text, year_bounds=year_bounds)
        _tick("insight", s_insight)
        return insight_text

    s = perf_counter()
    column_count = len(df.columns)
    _tick("column_guard", s)
    if column_count < 2:
        _record_failure(failure_reasons, "insufficient_columns: visualization_skipped")
        insight = _generate_insight_with_fallback([])
        total_latency_ms = round((perf_counter() - t0) * 1000.0, 2)
        log_event(
            "analysis.skip.insufficient_columns",
            {
                "request_id": request_id,
                "row_count": len(df),
                "column_count": column_count,
                "total_latency_ms": total_latency_ms,
                "stage_latency_ms": stage_latency_ms,
            },
        )
        return VisualizationResponse(
            sql=sql,
            table_preview=df.head(20).to_dict(orient="records"),
            analyses=[],
            insight=insight,
            fallback_used=False,
            fallback_stage="insufficient_columns",
            failure_reasons=failure_reasons,
            attempt_count=attempt_count,
            request_id=request_id,
            total_latency_ms=total_latency_ms,
            stage_latency_ms=stage_latency_ms,
        )

    s = perf_counter()
    numeric_columns = _numeric_columns_for_visualization(df)
    _tick("numeric_guard", s)
    if not numeric_columns:
        _record_failure(failure_reasons, "no_numeric_columns: visualization_skipped")
        insight = (
            "SQL 결과 컬럼에 수치형 데이터가 없어 시각화를 생성하지 않았습니다. "
            "COUNT/AVG/SUM 같은 수치형 컬럼을 포함해 다시 실행해 주세요."
        )
        total_latency_ms = round((perf_counter() - t0) * 1000.0, 2)
        log_event(
            "analysis.skip.no_numeric_columns",
            {
                "request_id": request_id,
                "row_count": len(df),
                "column_count": len(df.columns),
                "total_latency_ms": total_latency_ms,
                "stage_latency_ms": stage_latency_ms,
            },
        )
        return VisualizationResponse(
            sql=sql,
            table_preview=df.head(20).to_dict(orient="records"),
            analyses=[],
            insight=insight,
            fallback_used=False,
            fallback_stage="no_numeric_columns",
            failure_reasons=failure_reasons,
            attempt_count=attempt_count,
            request_id=request_id,
            total_latency_ms=total_latency_ms,
            stage_latency_ms=stage_latency_ms,
        )

    s = perf_counter()
    rag = retrieval.retrieve_context(
        user_query,
        df_schema,
        sql=sql,
        analysis_query=str(analysis_query or ""),
    )
    rag_context = rag.get("context_text", "")
    _tick("rag_retrieve", s)

    s = perf_counter()
    intent_info = intent_extractor.extract_intent(user_query, df_schema, rag_context)
    _tick("intent_extract", s)

    s = perf_counter()
    plans = chart_rule_engine.plan_analyses(
        intent_info,
        df,
        rag_context,
        retry_mode="normal",
        failure_reasons=failure_reasons,
    )
    analyses = _build_analyses_from_plans(plans, df, failure_reasons, "normal", request_id)
    _tick("plan_codegen_normal", s)

    if not _has_renderable_chart(analyses):
        fallback_used = True
        fallback_stage = "retry_relaxed"
        attempt_count = 2
        _record_failure(failure_reasons, "normal: no_renderable_chart")

        s = perf_counter()
        relaxed_intent_info = dict(intent_info)
        relaxed_intent_info["group_var"] = None
        relaxed_plans = chart_rule_engine.plan_analyses(
            relaxed_intent_info,
            df,
            rag_context,
            retry_mode="relaxed",
            failure_reasons=failure_reasons,
        )
        relaxed_analyses = _build_analyses_from_plans(
            relaxed_plans,
            df,
            failure_reasons,
            "relaxed",
            request_id,
        )
        if relaxed_analyses:
            analyses = relaxed_analyses
        if not _has_renderable_chart(analyses):
            _record_failure(failure_reasons, "relaxed: no_renderable_chart")
        _tick("plan_codegen_relaxed", s)

    insight = _generate_insight_with_fallback(analyses)

    total_latency_ms = round((perf_counter() - t0) * 1000.0, 2)
    log_event(
        "analysis.done",
        {
            "request_id": request_id,
            "fallback_used": fallback_used,
            "attempt_count": attempt_count,
            "failure_count": len(failure_reasons),
            "total_latency_ms": total_latency_ms,
            "stage_latency_ms": stage_latency_ms,
        },
    )

    return VisualizationResponse(
        sql=sql,
        table_preview=df.head(20).to_dict(orient="records"),
        analyses=analyses,
        insight=insight,
        fallback_used=fallback_used,
        fallback_stage=fallback_stage,
        failure_reasons=failure_reasons,
        attempt_count=attempt_count,
        request_id=request_id,
        total_latency_ms=total_latency_ms,
        stage_latency_ms=stage_latency_ms,
    )
