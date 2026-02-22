"""시각화 결과 타입 정의."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

# 입력: chart_type, x, y, group, agg
# 출력: ChartSpec 모델
# 차트 스펙 모델
# 차트 타입과 축/그룹/집계 정보를 담는다
class ChartSpec(BaseModel):
    # 차트 타입 (line, bar, hist, scatter, box 등)
    chart_type: str
    # 축/그룹/집계 정보
    x: Optional[str] = None
    y: Optional[str] = None
    group: Optional[str] = None
    secondary_group: Optional[str] = None
    agg: Optional[str] = None
    size: Optional[str] = None
    animation_frame: Optional[str] = None
    mode: Optional[str] = None
    bar_mode: Optional[str] = None
    orientation: Optional[str] = None
    series_cols: Optional[List[str]] = None
    max_categories: Optional[int] = None
    title: Optional[str] = None
    x_title: Optional[str] = None
    y_title: Optional[str] = None


# 입력: chart_spec, reason, figure_json, code, summary
# 출력: AnalysisCard 모델
# 분석 카드 모델
# 하나의 시각화 추천 단위를 나타낸다
class AnalysisCard(BaseModel):
    # 차트 스펙
    chart_spec: ChartSpec
    # 왜 이 차트를 추천했는지
    reason: Optional[str] = None
    # Plotly figure JSON
    figure_json: Optional[Dict[str, Any]] = None
    # Seaborn/Matplotlib 렌더 PNG(data URL)
    image_data_url: Optional[str] = None
    # 실제 렌더 엔진 식별자 (seaborn | plotly)
    render_engine: Optional[str] = None
    # 코드 요약(디버깅용)
    code: Optional[str] = None
    # 자연어 요약(선택)
    summary: Optional[str] = None

# 입력: sql, table_preview, analyses
# 출력: VisualizationResponse 모델
# 시각화 응답 모델
class VisualizationResponse(BaseModel):
    # 원본 SQL
    sql: str
    # 테이블 미리보기
    table_preview: List[Dict[str, Any]]
    # 추천 시각화 카드 목록
    analyses: List[AnalysisCard]
    # 통합 분석 인사이트(LLM 생성)
    insight: Optional[str] = None
    # fallback 경로가 사용되었는지 여부
    fallback_used: bool = False
    # fallback 단계(normal 실패 후 retry_relaxed/minimal_chart 등)
    fallback_stage: Optional[str] = None
    # 분석 과정 실패 사유 목록
    failure_reasons: List[str] = Field(default_factory=list)
    # 시도 횟수(기본 1, 재시도 시 증가)
    attempt_count: int = 1
    # 요청 추적 ID
    request_id: Optional[str] = None
    # 전체 처리 지연시간(ms)
    total_latency_ms: Optional[float] = None
    # 단계별 처리 지연시간(ms)
    stage_latency_ms: Dict[str, float] = Field(default_factory=dict)
