from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.services.runtime.state_store import get_state_store
from app.services.runtime.user_scope import scoped_state_key

router = APIRouter()


class MetricItem(BaseModel):
    label: str
    value: str
    trend: str | None = None


class PreviewData(BaseModel):
    columns: list[str] = []
    rows: list[list[Any]] = []
    row_count: int = 0
    row_cap: int | None = None
    total_count: int | None = None


class ChartSpec(BaseModel):
    id: str
    type: str
    x: str | None = None
    y: str | None = None
    config: dict[str, Any] | None = None
    thumbnailUrl: str | None = None
    pngUrl: str | None = None


class ColumnStatsRow(BaseModel):
    column: str
    n: int = 0
    missing: int = 0
    nulls: int = 0
    min: Any | None = None
    q1: Any | None = None
    median: Any | None = None
    q3: Any | None = None
    max: Any | None = None
    mean: Any | None = None


class CohortProvenance(BaseModel):
    source: str = "NONE"
    libraryCohortId: str | None = None
    libraryCohortName: str | None = None
    pdfCohortId: str | None = None
    pdfPaperTitle: str | None = None
    libraryUsed: bool | None = None


class DashboardQuery(BaseModel):
    id: str
    title: str
    description: str
    insight: str | None = None
    llmSummary: str | None = None
    query: str
    lastRun: str
    executedAt: str | None = None
    schedule: str | None = None
    isPinned: bool = False
    category: str
    folderId: str | None = None
    cohort: CohortProvenance | None = None
    pdfAnalysis: dict[str, Any] | None = None
    preview: PreviewData | None = None
    stats: list[ColumnStatsRow] = []
    recommendedCharts: list[ChartSpec] = []
    primaryChart: ChartSpec | None = None
    metrics: list[MetricItem] = []
    chartType: str


class DashboardFolder(BaseModel):
    id: str
    name: str
    tone: str | None = None
    createdAt: str | None = None


class DashboardPayload(BaseModel):
    user: str | None = None
    queries: list[DashboardQuery] | None = None
    folders: list[DashboardFolder] | None = None


class SaveQueryPayload(BaseModel):
    user: str | None = None
    question: str
    sql: str
    metadata: dict[str, Any] | None = None


class QueryBundlesPayload(BaseModel):
    user: str | None = None
    queryIds: list[str] = []


def _dashboard_key(user: str | None) -> str:
    return scoped_state_key("dashboard::queries", user)


BUNDLE_COLLECTION = "dashboard_query_bundles"


def _bundle_store():
    return get_state_store(BUNDLE_COLLECTION)


def _bundle_key(user: str | None, query_id: str) -> str:
    return scoped_state_key(f"dashboard::bundle::{query_id}", user)


def _extract_bundle_from_query(entry: dict[str, Any]) -> dict[str, Any] | None:
    query_id = str(entry.get("id", "")).strip()
    if not query_id:
        return None
    bundle: dict[str, Any] = {"queryId": query_id}
    for field in ("title", "description", "query", "insight", "llmSummary", "executedAt", "lastRun", "category", "folderId", "chartType"):
        value = entry.get(field)
        if isinstance(value, str):
            value = value.strip()
            if value:
                bundle[field] = value
        elif value is not None:
            bundle[field] = value
    for field in ("preview", "cohort", "primaryChart", "pdfAnalysis"):
        value = entry.get(field)
        if value is None and field == "pdfAnalysis":
            value = entry.get("pdf_analysis")
        if isinstance(value, dict):
            bundle[field] = value
    for field in ("stats", "recommendedCharts", "metrics"):
        value = entry.get(field)
        if isinstance(value, list) and len(value) > 0:
            bundle[field] = value
    return bundle


def _upsert_query_bundle(user: str | None, entry: dict[str, Any]) -> None:
    store = _bundle_store()
    if not store.enabled:
        return
    bundle = _extract_bundle_from_query(entry)
    if not bundle:
        return
    query_id = str(bundle.get("queryId", "")).strip()
    if not query_id:
        return
    key = _bundle_key(user, query_id)
    existing = store.get(key) or {}
    merged = dict(existing) if isinstance(existing, dict) else {}
    for field, value in bundle.items():
        if field == "queryId":
            merged[field] = value
            continue
        if isinstance(value, str):
            if value.strip():
                merged[field] = value
            continue
        merged[field] = value
    store.set(key, merged)


def _merge_bundle_into_query(query_item: dict[str, Any], bundle: dict[str, Any]) -> dict[str, Any]:
    merged = dict(query_item)
    for field in ("title", "description", "query", "insight", "llmSummary", "executedAt", "lastRun", "category", "folderId", "chartType"):
        value = bundle.get(field)
        if isinstance(value, str):
            if value.strip():
                merged[field] = value
        elif value is not None:
            merged[field] = value
    for field in ("preview", "cohort", "primaryChart", "pdfAnalysis"):
        value = bundle.get(field)
        if value is None and field == "pdfAnalysis":
            value = bundle.get("pdf_analysis")
        if isinstance(value, dict):
            merged[field] = value
    for field in ("stats", "recommendedCharts", "metrics"):
        value = bundle.get(field)
        if isinstance(value, list):
            merged[field] = value
    return merged


@router.get("/queries")
def get_queries(user: str | None = Query(default=None)):
    store = get_state_store()
    if not store.enabled:
        return {"queries": [], "folders": [], "detail": "MongoDB is not configured"}
    key = _dashboard_key(user)
    value = store.get(key) or {}
    queries = value.get("queries", []) if isinstance(value, dict) else []
    folders = value.get("folders", []) if isinstance(value, dict) else []
    bundle_store = _bundle_store()
    if bundle_store.enabled and isinstance(queries, list):
        enriched_queries: list[Any] = []
        for item in queries:
            if not isinstance(item, dict):
                enriched_queries.append(item)
                continue
            query_id = str(item.get("id", "")).strip()
            if not query_id:
                enriched_queries.append(item)
                continue
            bundle = bundle_store.get(_bundle_key(user, query_id))
            if isinstance(bundle, dict):
                enriched_queries.append(_merge_bundle_into_query(item, bundle))
            else:
                enriched_queries.append(item)
        queries = enriched_queries
    return {"queries": queries, "folders": folders}


@router.post("/queries")
def save_queries(payload: DashboardPayload):
    store = get_state_store()
    if not store.enabled:
        return {"ok": False, "detail": "MongoDB is not configured"}
    key = _dashboard_key(payload.user)
    existing = store.get(key) or {}
    existing_queries = existing.get("queries", []) if isinstance(existing, dict) else []
    existing_folders = existing.get("folders", []) if isinstance(existing, dict) else []

    queries = []
    for item in payload.queries or []:
        if hasattr(item, "model_dump"):
            queries.append(item.model_dump())
        else:
            queries.append(item.dict())

    folders = []
    for item in payload.folders or []:
        if hasattr(item, "model_dump"):
            folders.append(item.model_dump())
        else:
            folders.append(item.dict())

    next_queries = queries if payload.queries is not None else existing_queries
    next_folders = folders if payload.folders is not None else existing_folders
    store.set(key, {"queries": next_queries, "folders": next_folders})
    for item in next_queries:
        if isinstance(item, dict):
            _upsert_query_bundle(payload.user, item)
    return {"ok": True, "count": len(next_queries), "folders": len(next_folders)}


@router.post("/saveQuery")
def save_query(payload: SaveQueryPayload):
    store = get_state_store()
    if not store.enabled:
        return {"ok": False, "detail": "MongoDB is not configured"}

    key = _dashboard_key(payload.user)
    existing = store.get(key) or {}
    existing_queries = existing.get("queries", []) if isinstance(existing, dict) else []
    existing_folders = existing.get("folders", []) if isinstance(existing, dict) else []

    metadata = payload.metadata or {}
    entry = metadata.get("entry") if isinstance(metadata, dict) else None
    new_folder = metadata.get("new_folder") if isinstance(metadata, dict) else None
    if not isinstance(entry, dict):
        cohort = metadata.get("cohort") if isinstance(metadata.get("cohort"), dict) else None
        pdf_analysis = (
            metadata.get("pdf_analysis")
            if isinstance(metadata.get("pdf_analysis"), dict)
            else metadata.get("pdfAnalysis")
            if isinstance(metadata.get("pdfAnalysis"), dict)
            else None
        )
        stats = metadata.get("stats") if isinstance(metadata.get("stats"), list) else []
        recommended_charts = (
            metadata.get("recommended_charts")
            if isinstance(metadata.get("recommended_charts"), list)
            else []
        )
        primary_chart = (
            metadata.get("primary_chart")
            if isinstance(metadata.get("primary_chart"), dict)
            else None
        )
        entry = {
            "id": f"dashboard-{len(existing_queries) + 1}",
            "title": payload.question,
            "description": "Query result summary",
            "insight": str(metadata.get("insight") or "").strip() or None,
            "llmSummary": str(metadata.get("llm_summary") or metadata.get("insight") or "").strip() or None,
            "query": payload.sql,
            "lastRun": "just now",
            "executedAt": None,
            "isPinned": True,
            "category": "all",
            "cohort": cohort,
            "pdfAnalysis": pdf_analysis,
            "stats": stats,
            "recommendedCharts": recommended_charts,
            "primaryChart": primary_chart,
            "metrics": [
                {"label": "rows", "value": str(metadata.get("row_count", 0))},
                {"label": "columns", "value": str(metadata.get("column_count", 0))},
            ],
            "chartType": "bar",
        }

    next_folders = list(existing_folders)
    if isinstance(new_folder, dict):
        folder_id = str(new_folder.get("id", "")).strip()
        folder_name = str(new_folder.get("name", "")).strip()
        if folder_id and folder_name:
            exists = any(str(item.get("id", "")).strip() == folder_id for item in next_folders if isinstance(item, dict))
            if not exists:
                next_folders.append(
                    {
                        "id": folder_id,
                        "name": folder_name,
                        "tone": str(new_folder.get("tone", "")).strip() or None,
                        "createdAt": str(new_folder.get("createdAt", "")).strip() or None,
                    }
                )

    next_queries = [entry, *existing_queries]
    store.set(key, {"queries": next_queries, "folders": next_folders})
    if isinstance(entry, dict):
        _upsert_query_bundle(payload.user, entry)
    return {"ok": True, "count": len(next_queries), "folders": len(next_folders)}


@router.post("/queryBundles")
def get_query_bundles(payload: QueryBundlesPayload):
    store = _bundle_store()
    if not store.enabled:
        return {"bundles": {}, "detail": "MongoDB is not configured"}
    bundles: dict[str, Any] = {}
    for raw_query_id in payload.queryIds:
        query_id = str(raw_query_id or "").strip()
        if not query_id:
            continue
        bundle = store.get(_bundle_key(payload.user, query_id))
        if isinstance(bundle, dict):
            bundles[query_id] = bundle
    return {"bundles": bundles, "count": len(bundles)}
