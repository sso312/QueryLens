from __future__ import annotations

import json
import sys
import hashlib
from pathlib import Path
from typing import Iterable, List, Tuple

from openai import OpenAI

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.config.rag_config import EMBEDDING_MODEL, EMBEDDING_DIM, RAG_BATCH_SIZE, RAG_DOC_VERSION
from src.db.vector_store import ensure_collection, get_mongo_collection, upsert_embeddings


def _load_jsonl(path: Path) -> List[dict]:
    rows: List[dict] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _iter_seed_docs(data_dir: Path) -> Iterable[dict]:
    for path in sorted(data_dir.glob("*.jsonl")):
        yield from _load_jsonl(path)


def _normalize_doc(doc: dict) -> Tuple[str, dict]:
    """Normalize heterogeneous docs into (text, metadata)."""
    if "text" in doc:
        return doc["text"], doc.get("metadata", {})

    if "template_id" in doc and "sql" in doc:
        template_id = doc.get("template_id")
        x_alias = doc.get("x_alias")
        y_alias = doc.get("y_alias")
        chart_candidates = doc.get("chart_candidates", [])
        chart_type = chart_candidates[0] if chart_candidates else "bar"
        text = (
            f"Template: {template_id}\n"
            f"x_alias: {x_alias}\n"
            f"y_alias: {y_alias}\n"
            f"chart_candidates: {chart_candidates}\n"
            f"SQL: {doc.get('sql')}\n"
            f"chart_spec: {{\"chart_type\": \"{chart_type}\", \"x\": \"{x_alias}\", \"y\": \"{y_alias}\"}}"
        )
        metadata = doc.get("metadata", {}) | {
            "type": "template",
            "template_id": template_id,
        }
        return text, metadata

    if "question" in doc and "sql" in doc:
        text = (
            f"Question: {doc.get('question')}\n"
            f"Intent: {doc.get('intent')}\n"
            f"X meaning: {doc.get('x_meaning')}\n"
            f"Y meaning: {doc.get('y_meaning')}\n"
            f"Chart: {doc.get('chart_type')}\n"
            f"SQL: {doc.get('sql')}"
        )
        metadata = doc.get("metadata", {}) | {"type": "example"}
        return text, metadata

    if "table" in doc and "column" in doc and "top_values" in doc:
        table = str(doc.get("table") or "").strip().upper()
        column = str(doc.get("column") or "").strip().upper()
        data_type = str(doc.get("data_type") or "").strip().upper()
        num_distinct = doc.get("num_distinct")
        num_nulls = doc.get("num_nulls")
        row_count = doc.get("row_count")
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
        value_text = ", ".join(values) if values else "-"
        text = (
            f"Table value profile: {table}.{column} ({data_type or 'UNKNOWN'}). "
            f"Distinct={num_distinct}, Nulls={num_nulls}, Rows={row_count}. "
            f"Top values: {value_text}."
        )
        metadata = doc.get("metadata", {}) | {"type": "table_profile", "table": table, "column": column}
        return text, metadata

    return json.dumps(doc, ensure_ascii=False), doc.get("metadata", {})


def _embed_texts(texts: List[str]) -> List[List[float]]:
    client = OpenAI()
    kwargs = {"model": EMBEDDING_MODEL, "input": texts}
    if EMBEDDING_MODEL.startswith("text-embedding-3") and EMBEDDING_DIM > 0:
        kwargs["dimensions"] = EMBEDDING_DIM
    response = client.embeddings.create(**kwargs)
    return [item.embedding for item in response.data]


def _stable_doc_id(text: str, metadata: dict) -> str:
    seed = f"{RAG_DOC_VERSION}\n{text}\n{json.dumps(metadata, sort_keys=True, ensure_ascii=False)}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()


def _batch(iterable: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def build_index() -> None:
    data_dir = BASE_DIR / "data"
    docs = list(_iter_seed_docs(data_dir))

    if not docs:
        raise RuntimeError("RAG seed data is missing. Check data/*.jsonl.")

    normalized = [_normalize_doc(d) for d in docs]
    texts = [text for text, _ in normalized]
    metadatas = [meta | {"text": text, "doc_version": RAG_DOC_VERSION} for text, meta in normalized]
    ids = [_stable_doc_id(text, meta) for text, meta in normalized]
    for metadata, doc_id in zip(metadatas, ids):
        metadata["doc_id"] = doc_id

    collection = get_mongo_collection()
    collection.delete_many({"metadata.doc_version": RAG_DOC_VERSION})

    # Bootstrap collection with first embedding vector size.
    first_embeddings = _embed_texts(texts[:1])
    ensure_collection(collection, vector_size=len(first_embeddings[0]))
    upsert_embeddings(collection, first_embeddings, metadatas[:1], ids[:1])

    # Upsert the remaining documents in batches.
    start_idx = 1
    for batch_texts in _batch(texts[start_idx:], RAG_BATCH_SIZE):
        batch_embeddings = _embed_texts(batch_texts)
        batch_size = len(batch_texts)
        batch_metadatas = metadatas[start_idx : start_idx + batch_size]
        batch_ids = ids[start_idx : start_idx + batch_size]
        upsert_embeddings(collection, batch_embeddings, batch_metadatas, batch_ids)
        start_idx += batch_size


if __name__ == "__main__":
    build_index()

