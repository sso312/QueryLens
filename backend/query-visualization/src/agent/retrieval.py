from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List

from openai import OpenAI

from src.config.rag_config import (
    EMBEDDING_MODEL,
    EMBEDDING_DIM,
    RAG_ENABLED,
    RAG_CONTEXT_MAX_CHARS,
    RAG_DOC_VERSION,
    RAG_MIN_SCORE,
    RAG_TOP_K,
)
from src.db.vector_store import get_mongo_collection, search_embeddings
from src.utils.logging import log_event


_LOCAL_DOCS_CACHE: List[dict] | None = None
_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")
_SQL_FROM_JOIN_RE = re.compile(r"\b(?:from|join)\s+([a-zA-Z0-9_.$]+)", re.IGNORECASE)
_SQL_GROUP_BY_RE = re.compile(r"\bgroup\s+by\s+(.+?)(?:\border\s+by\b|\bhaving\b|\blimit\b|$)", re.IGNORECASE | re.DOTALL)
_SQL_ORDER_BY_RE = re.compile(r"\border\s+by\s+(.+?)(?:\blimit\b|$)", re.IGNORECASE | re.DOTALL)
_SQL_AGG_FN_RE = re.compile(r"\b(count|sum|avg|min|max|median|stddev|variance)\s*\(", re.IGNORECASE)


def _clip_text(text: str, *, max_chars: int) -> str:
    normalized = str(text or "").strip()
    if len(normalized) <= max_chars:
        return normalized
    return f"{normalized[:max_chars]}..."


def _split_sql_clause_list(clause_text: str, *, max_items: int = 8) -> List[str]:
    raw = str(clause_text or "").replace("\n", " ")
    parts = [part.strip() for part in raw.split(",")]
    cleaned = [part for part in parts if part]
    return cleaned[:max_items]


def _summarize_sql(sql: str) -> Dict[str, Any]:
    text = str(sql or "").strip()
    if not text:
        return {
            "tables": [],
            "aggregates": [],
            "group_by": [],
            "order_by": [],
            "has_where": False,
            "has_having": False,
        }

    tables = []
    seen_tables = set()
    for match in _SQL_FROM_JOIN_RE.findall(text):
        table = str(match or "").strip()
        if not table:
            continue
        lower = table.lower()
        if lower in seen_tables:
            continue
        seen_tables.add(lower)
        tables.append(table)

    aggregates = []
    seen_aggs = set()
    for fn in _SQL_AGG_FN_RE.findall(text):
        name = str(fn or "").lower()
        if name and name not in seen_aggs:
            seen_aggs.add(name)
            aggregates.append(name)

    group_match = _SQL_GROUP_BY_RE.search(text)
    order_match = _SQL_ORDER_BY_RE.search(text)
    group_by = _split_sql_clause_list(group_match.group(1) if group_match else "")
    order_by = _split_sql_clause_list(order_match.group(1) if order_match else "")

    return {
        "tables": tables[:12],
        "aggregates": aggregates[:8],
        "group_by": group_by,
        "order_by": order_by,
        "has_where": bool(re.search(r"\bwhere\b", text, flags=re.IGNORECASE)),
        "has_having": bool(re.search(r"\bhaving\b", text, flags=re.IGNORECASE)),
    }


def _build_query_text(
    user_query: str,
    df_schema: Dict[str, Any],
    *,
    sql: str = "",
    analysis_query: str = "",
) -> str:
    columns = df_schema.get("columns", [])
    dtypes = df_schema.get("dtypes", {})
    normalized_user_query = _clip_text(user_query, max_chars=1000)
    normalized_analysis_query = _clip_text(analysis_query, max_chars=1000)
    normalized_sql = _clip_text(sql, max_chars=3000)
    sql_summary = _summarize_sql(sql)
    return (
        "User query:\n"
        f"{normalized_user_query}\n\n"
        "Analysis focus query:\n"
        f"{normalized_analysis_query}\n\n"
        "SQL query:\n"
        f"{normalized_sql}\n\n"
        "SQL summary:\n"
        f"- tables: {sql_summary['tables']}\n"
        f"- aggregates: {sql_summary['aggregates']}\n"
        f"- group_by: {sql_summary['group_by']}\n"
        f"- order_by: {sql_summary['order_by']}\n"
        f"- has_where: {sql_summary['has_where']}\n"
        f"- has_having: {sql_summary['has_having']}\n\n"
        "DataFrame schema summary:\n"
        f"- columns: {columns}\n"
        f"- dtypes: {dtypes}\n"
    )


def _embed_texts(texts: List[str]) -> List[List[float]]:
    client = OpenAI()
    kwargs = {"model": EMBEDDING_MODEL, "input": texts}
    if EMBEDDING_MODEL.startswith("text-embedding-3") and EMBEDDING_DIM > 0:
        kwargs["dimensions"] = EMBEDDING_DIM
    response = client.embeddings.create(**kwargs)
    return [item.embedding for item in response.data]


def _tokenize(text: str) -> List[str]:
    return [tok.lower() for tok in _TOKEN_RE.findall(str(text or "")) if len(tok) >= 2]


def _normalize_seed_doc(doc: dict) -> str:
    if "text" in doc:
        return str(doc.get("text") or "")
    if "question" in doc and "sql" in doc:
        return (
            f"Question: {doc.get('question')}\n"
            f"Intent: {doc.get('intent')}\n"
            f"Chart: {doc.get('chart_type')}\n"
            f"SQL: {doc.get('sql')}"
        )
    if "table" in doc and "column" in doc and "top_values" in doc:
        values = []
        for item in list(doc.get("top_values") or [])[:12]:
            if not isinstance(item, dict):
                continue
            value = str(item.get("value") or "").strip()
            count = item.get("count")
            if not value:
                continue
            if count is None:
                values.append(value)
            else:
                values.append(f"{value}({count})")
        return (
            f"Table value profile: {str(doc.get('table') or '').upper()}.{str(doc.get('column') or '').upper()} "
            f"({str(doc.get('data_type') or '').upper()}). "
            f"Distinct={doc.get('num_distinct')}, Nulls={doc.get('num_nulls')}. "
            f"Top values: {', '.join(values) if values else '-'}."
        )
    if "template_id" in doc and "sql" in doc:
        return (
            f"Template: {doc.get('template_id')}\n"
            f"x_alias: {doc.get('x_alias')}\n"
            f"y_alias: {doc.get('y_alias')}\n"
            f"SQL: {doc.get('sql')}"
        )
    return json.dumps(doc, ensure_ascii=False)


def _load_local_docs() -> List[dict]:
    global _LOCAL_DOCS_CACHE
    if _LOCAL_DOCS_CACHE is not None:
        return _LOCAL_DOCS_CACHE
    data_dir = Path(__file__).resolve().parents[2] / "data"
    docs: List[dict] = []
    if data_dir.exists():
        for path in sorted(data_dir.glob("*.jsonl")):
            for line in path.read_text(encoding="utf-8").splitlines():
                raw = line.strip()
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    docs.append({"text": _normalize_seed_doc(obj)})
    _LOCAL_DOCS_CACHE = docs
    return docs


def _local_search(query_text: str, *, k: int) -> List[str]:
    docs = _load_local_docs()
    if not docs:
        return []
    q_tokens = set(_tokenize(query_text))
    if not q_tokens:
        return []
    scored: List[tuple[float, str]] = []
    for doc in docs:
        text = str(doc.get("text") or "")
        d_tokens = set(_tokenize(text))
        if not d_tokens:
            continue
        overlap = len(q_tokens & d_tokens)
        if overlap <= 0:
            continue
        score = overlap / float(len(q_tokens))
        scored.append((score, text))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [text for _, text in scored[: max(1, k)]]


def retrieve_context(
    user_query: str,
    df_schema: Dict[str, Any],
    *,
    sql: str = "",
    analysis_query: str = "",
) -> Dict[str, Any]:
    """Retrieve related context snippets from vector store."""
    if not RAG_ENABLED:
        query_text = _build_query_text(
            user_query,
            df_schema,
            sql=sql,
            analysis_query=analysis_query,
        )
        local_snippets = _local_search(query_text, k=RAG_TOP_K)
        log_event("rag.search.skip", {"reason": "RAG_ENABLED=false", "local_fallback": len(local_snippets)})
        context_text = "\n\n".join(local_snippets)[:RAG_CONTEXT_MAX_CHARS]
        return {"snippets": local_snippets, "context_text": context_text, "scores": []}

    try:
        query_text = _build_query_text(
            user_query,
            df_schema,
            sql=sql,
            analysis_query=analysis_query,
        )
        query_embedding = _embed_texts([query_text])[0]

        collection = get_mongo_collection()
        hits = search_embeddings(collection, query_embedding, limit=RAG_TOP_K)

        snippets = []
        kept_scores: List[float] = []
        for hit in hits:
            score = float(hit.get("score", 0.0))
            metadata = hit.get("metadata") or {}
            if score < RAG_MIN_SCORE:
                continue
            if metadata.get("doc_version") and metadata.get("doc_version") != RAG_DOC_VERSION:
                continue
            payload = (hit.get("metadata") or {}) | {"text": hit.get("text")}
            text = payload.get("text")
            if text:
                snippets.append(text)
                kept_scores.append(score)

        context_text = "\n\n".join(snippets)
        if len(context_text) > RAG_CONTEXT_MAX_CHARS:
            context_text = context_text[:RAG_CONTEXT_MAX_CHARS]
        log_event(
            "rag.search",
            {
                "count": len(snippets),
                "top_k": RAG_TOP_K,
                "min_score": RAG_MIN_SCORE,
                "score_max": max(kept_scores) if kept_scores else None,
                "score_min": min(kept_scores) if kept_scores else None,
            },
        )
        return {
            "snippets": snippets,
            "context_text": context_text,
            "scores": kept_scores,
        }
    except Exception as exc:  # pragma: no cover - environment dependent
        log_event("rag.search.error", {"error": str(exc)})
        query_text = _build_query_text(
            user_query,
            df_schema,
            sql=sql,
            analysis_query=analysis_query,
        )
        local_snippets = _local_search(query_text, k=RAG_TOP_K)
        context_text = "\n\n".join(local_snippets)[:RAG_CONTEXT_MAX_CHARS]
        log_event("rag.search.local_fallback", {"count": len(local_snippets)})
        return {"snippets": local_snippets, "context_text": context_text, "scores": []}
