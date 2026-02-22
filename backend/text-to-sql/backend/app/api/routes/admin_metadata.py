from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel
from pathlib import Path
import json

from app.core.paths import project_path
from app.services.oracle.metadata_extractor import extract_metadata
from app.services.rag.indexer import reindex
from app.services.rag.mongo_store import MongoStore
from app.services.runtime.column_value_store import load_column_value_rows

router = APIRouter()
rag_router = APIRouter()


class MetadataSyncRequest(BaseModel):
    owner: str


class RagTemplateItem(BaseModel):
    name: str
    sql: str


class RagGlossaryItem(BaseModel):
    term: str
    definition: str = ""


class RagContextPayload(BaseModel):
    joins: list[RagTemplateItem] = []
    metrics: list[RagTemplateItem] = []
    terms: list[RagGlossaryItem] = []


@router.post("/sync")
def sync_metadata(req: MetadataSyncRequest):
    return extract_metadata(req.owner)


@rag_router.post("/reindex")
def rag_reindex():
    return reindex()


def _load_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _count_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def _write_jsonl(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(row, ensure_ascii=True) for row in rows]
    payload = "\n".join(lines)
    if payload:
        payload += "\n"
    path.write_text(payload, encoding="utf-8")


@router.get("/tables")
def list_tables():
    base = project_path("var/metadata")
    schema = _load_json(base / "schema_catalog.json") or {}
    owner = schema.get("owner") or ""
    tables = schema.get("tables", {}) if isinstance(schema, dict) else {}
    results = []
    for name, entry in tables.items():
        columns = entry.get("columns", []) if isinstance(entry, dict) else []
        table_owner = ""
        if isinstance(entry, dict):
            table_owner = str(entry.get("owner") or "").strip()
        results.append({
            "name": name,
            "schema": table_owner or owner,
            "columns": len(columns),
            "primary_keys": entry.get("primary_keys", []) if isinstance(entry, dict) else [],
        })
    results.sort(key=lambda item: str(item.get("name") or ""))
    return {"owner": owner, "tables": results}


@rag_router.get("/status")
def rag_status():
    base = project_path("var/metadata")
    schema = _load_json(base / "schema_catalog.json") or {"tables": {}}
    return {
        "schema_docs": len(schema.get("tables", {})),
        "sql_examples_docs": _count_jsonl(base / "sql_examples.jsonl"),
        "join_templates_docs": _count_jsonl(base / "join_templates.jsonl") + _count_jsonl(base / "sql_templates.jsonl"),
        "glossary_docs": _count_jsonl(base / "glossary_docs.jsonl"),
        "diagnosis_map_docs": _count_jsonl(base / "diagnosis_icd_map.jsonl"),
        "procedure_map_docs": _count_jsonl(base / "procedure_icd_map.jsonl"),
        "label_intent_docs": _count_jsonl(base / "label_intent_profiles.jsonl"),
        "column_value_docs": len(load_column_value_rows()),
    }


@rag_router.get("/docs")
def rag_docs(
    request: Request,
    doc_type: str | None = None,
    kind: str | None = None,
    limit: int = 200,
    skip: int = 0,
):
    if doc_type is None:
        legacy_type = request.query_params.get("type")
        if legacy_type:
            doc_type = legacy_type
    where: dict[str, str] = {}
    if doc_type:
        where["type"] = doc_type
    if kind:
        where["kind"] = kind
    store = MongoStore()
    docs = store.list_documents(where=where or None, limit=limit, skip=skip)
    return {"docs": docs, "count": len(docs), "limit": limit, "skip": skip}


@rag_router.post("/context")
def save_rag_context(payload: RagContextPayload):
    base = project_path("var/metadata")

    joins = [
        {"name": item.name.strip(), "sql": item.sql.strip()}
        for item in payload.joins
        if item.name.strip() and item.sql.strip()
    ]
    metrics = [
        {"name": item.name.strip(), "sql": item.sql.strip()}
        for item in payload.metrics
        if item.name.strip() and item.sql.strip()
    ]
    terms = [
        {"term": item.term.strip(), "definition": item.definition.strip()}
        for item in payload.terms
        if item.term.strip()
    ]

    _write_jsonl(base / "join_templates.jsonl", joins)
    _write_jsonl(base / "sql_templates.jsonl", metrics)
    _write_jsonl(base / "glossary_docs.jsonl", terms)

    reindex_result = reindex()
    return {
        "ok": True,
        "counts": {
            "joins": len(joins),
            "metrics": len(metrics),
            "terms": len(terms),
        },
        "reindex": reindex_result,
    }
