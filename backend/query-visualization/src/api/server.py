from __future__ import annotations

import os
import math
from typing import Any, Dict, List

import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from src.db.oracle_client import fetch_all
from src.models.chart_spec import VisualizationResponse
from src.utils.logging import log_event, new_request_id

load_dotenv()

app = FastAPI(title="Query Visualization API")

origins = [
    origin.strip()
    for origin in os.getenv(
        "CORS_ALLOW_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000",
    ).split(",")
    if origin.strip()
]
if origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, value)


MAX_ROWS = _env_int("VIS_MAX_ROWS", 10000, minimum=1)
MAX_QUERY_TEXT_LENGTH = _env_int("VIS_MAX_QUERY_TEXT_LENGTH", 4000, minimum=1)
MAX_SQL_TEXT_LENGTH = _env_int("VIS_MAX_SQL_TEXT_LENGTH", 12000, minimum=1)


class VisualizeRequest(BaseModel):
    user_query: str = Field(..., min_length=1, max_length=MAX_QUERY_TEXT_LENGTH)
    # Deprecated: ignore client-provided analysis_query to prevent conversational context leakage.
    analysis_query: str | None = Field(default=None, max_length=MAX_QUERY_TEXT_LENGTH)
    sql: str = Field(..., min_length=1, max_length=MAX_SQL_TEXT_LENGTH)
    rows: List[Dict[str, Any]]


def _sanitize_non_finite(value: Any) -> Any:
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float)):
        try:
            numeric = float(value)
        except Exception:
            return value
        return value if math.isfinite(numeric) else None
    if isinstance(value, dict):
        return {str(k): _sanitize_non_finite(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_non_finite(item) for item in value]
    return value


def _validate_payload(req: VisualizeRequest) -> None:
    if len(req.rows) > MAX_ROWS:
        raise HTTPException(
            status_code=413,
            detail={"code": "ROWS_LIMIT_EXCEEDED", "message": f"rows size must be <= {MAX_ROWS}"},
        )
    if req.rows and not isinstance(req.rows[0], dict):
        raise HTTPException(
            status_code=422,
            detail={"code": "INVALID_ROWS", "message": "rows must be a list of objects"},
        )


@app.get("/health")
def health_check() -> dict:
    return {"status": "ok"}


@app.get("/db-test")
def db_test() -> dict:
    try:
        rows = fetch_all("SELECT * from sso.patients where rownum = 1")
        return {"ok": True, "rows": rows}
    except Exception as exc:  # pragma: no cover
        log_event("db.test.error", {"error": str(exc)}, level="error")
        raise HTTPException(
            status_code=500,
            detail={"code": "DB_TEST_FAILED", "message": str(exc)},
        ) from exc


@app.post("/visualize", response_model=VisualizationResponse)
def visualize(req: VisualizeRequest) -> VisualizationResponse:
    _validate_payload(req)
    request_id = new_request_id()
    df = pd.DataFrame(req.rows)
    log_event(
        "request.visualize",
        {
            "request_id": request_id,
            "row_count": len(req.rows),
            "column_count": len(df.columns),
        },
    )

    try:
        from src.agent.analysis_agent import analyze_and_visualize
    except Exception as exc:  # pragma: no cover
        log_event("analysis.import.error", {"request_id": request_id, "error": str(exc)}, level="error")
        raise HTTPException(
            status_code=501,
            detail={"code": "ANALYSIS_IMPORT_ERROR", "message": "analysis agent import failed"},
        ) from exc

    result = analyze_and_visualize(
        req.user_query,
        req.sql,
        df,
        analysis_query=None,
        request_id=request_id,
    )
    try:
        raw_payload = result.model_dump() if hasattr(result, "model_dump") else result
        payload = _sanitize_non_finite(raw_payload)
        return VisualizationResponse.model_validate(payload)
    except Exception:
        # Fail-safe: if sanitation/validation unexpectedly fails, return original response.
        return result
