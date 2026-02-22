from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
import hashlib
import json
import math
import re

try:
    from pymongo import MongoClient, ReplaceOne
    from pymongo.errors import PyMongoError
except Exception:  # pragma: no cover
    MongoClient = None  # type: ignore[assignment]
    ReplaceOne = None  # type: ignore[assignment]

    class PyMongoError(Exception):
        pass

from app.core.config import get_settings

try:
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover
    OpenAI = None


def _hash_token(token: str, dim: int) -> int:
    digest = hashlib.md5(token.encode("utf-8")).hexdigest()
    return int(digest, 16) % dim


_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+|[가-힣]+")


def _tokenize(text: str) -> list[str]:
    return [token for token in _TOKEN_RE.findall(text.lower()) if token]


def _lexical_overlap(query: str, text: str) -> float:
    q_tokens = set(_tokenize(query))
    d_tokens = set(_tokenize(text))
    if not q_tokens or not d_tokens:
        return 0.0
    return len(q_tokens & d_tokens) / float(len(q_tokens))


def _blend_score(base_score: float, query: str, text: str) -> float:
    lexical = _lexical_overlap(query, text)
    return base_score + (0.20 * lexical)


def _embed_text(text: str, dim: int = 128) -> list[float]:
    vec = [0.0] * dim
    for tok in _tokenize(text):
        idx = _hash_token(tok, dim)
        vec[idx] += 1.0
    norm = math.sqrt(sum(v * v for v in vec))
    if norm > 0:
        vec = [v / norm for v in vec]
    return vec


def _embed_texts(texts: list[str], dim: int = 128) -> list[list[float]]:
    return [_embed_text(t, dim=dim) for t in texts]


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    if len(a) != len(b):
        n = min(len(a), len(b))
        if n <= 0:
            return 0.0
        return sum(a[idx] * b[idx] for idx in range(n))
    return sum(x * y for x, y in zip(a, b))


def _build_metadata_filter(where: dict[str, Any] | None) -> dict[str, Any]:
    if not where:
        return {}
    return {f"metadata.{key}": value for key, value in where.items()}


@dataclass
class SimpleStore:
    path: Path
    dim: int = 128
    embed_fn: Callable[[str], list[float]] | None = None
    docs: dict[str, dict[str, Any]] = None  # type: ignore

    def __post_init__(self) -> None:
        self.docs = {}
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
                self.docs = data.get("docs", {})
            except json.JSONDecodeError:
                self.docs = {}

    def persist(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"docs": self.docs}
        self.path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")

    def _embed(self, text: str) -> list[float]:
        if self.embed_fn is not None:
            vec = self.embed_fn(text)
            if vec:
                return vec
        return _embed_text(text, dim=self.dim)

    def upsert(self, ids: list[str], texts: list[str], metadatas: list[dict[str, Any]]) -> None:
        vectors = [self._embed(text) for text in texts]
        for doc_id, text, meta, vec in zip(ids, texts, metadatas, vectors):
            self.docs[doc_id] = {"text": text, "meta": meta, "vec": vec}
        self.persist()

    def query(self, query_text: str, k: int = 5, where: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        qvec = self._embed(query_text)
        scored = []
        for doc_id, doc in self.docs.items():
            if where:
                match = True
                for key, value in where.items():
                    if doc.get("meta", {}).get(key) != value:
                        match = False
                        break
                if not match:
                    continue
            doc_vec = list(doc.get("vec") or [])
            if not doc_vec or len(doc_vec) != len(qvec):
                doc_vec = self._embed(str(doc.get("text") or ""))
                doc["vec"] = doc_vec
            base_score = _cosine(qvec, doc_vec)
            score = _blend_score(base_score, query_text, str(doc.get("text", "")))
            scored.append((score, doc_id, doc))
        scored.sort(reverse=True)
        results = []
        for score, doc_id, doc in scored[:k]:
            results.append({
                "id": doc_id,
                "text": doc["text"],
                "metadata": doc["meta"],
                "score": score,
            })
        return results


class MongoStore:
    def __init__(self, collection_name: str = "rag_docs") -> None:
        settings = get_settings()
        self.persist_dir = Path(settings.rag_persist_dir)
        self.collection_name = settings.mongo_collection or collection_name
        self.dim = settings.rag_embedding_dim
        self.vector_index = settings.mongo_vector_index
        self._embedding_provider = (settings.rag_embedding_provider or "hash").strip().lower()
        self._embedding_model = (settings.rag_embedding_model or "text-embedding-3-small").strip()
        self._embedding_batch_size = max(1, int(settings.rag_embedding_batch_size or 64))

        self._simple: SimpleStore | None = None
        self._client: MongoClient | None = None
        self._collection = None
        self._embedding_client = None
        if self._embedding_provider == "openai" and OpenAI is not None and settings.openai_api_key:
            try:
                self._embedding_client = OpenAI(
                    api_key=settings.openai_api_key or None,
                    base_url=settings.openai_base_url or None,
                    organization=settings.openai_org or None,
                )
            except Exception:
                self._embedding_client = None

        if settings.mongo_uri and MongoClient is not None:
            self._client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=2000)
            try:
                self._client.admin.command("ping")
            except Exception as exc:  # pragma: no cover - depends on runtime Mongo
                raise RuntimeError("MongoDB connection failed. Check MONGO_URI.") from exc
            database = self._client[settings.mongo_db]
            self._collection = database[self.collection_name]
            self._collection.create_index("metadata.type")
        else:
            self._simple = SimpleStore(
                self.persist_dir / "simple_store.json",
                dim=self.dim,
                embed_fn=self._embed_text,
            )

    def _uses_openai_embeddings(self) -> bool:
        return self._embedding_provider == "openai" and self._embedding_client is not None

    def _openai_supports_dimensions(self) -> bool:
        return self._embedding_model.startswith("text-embedding-3")

    def _embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        if not self._uses_openai_embeddings():
            return _embed_texts(texts, dim=self.dim)

        try:
            vectors: list[list[float]] = []
            for idx in range(0, len(texts), self._embedding_batch_size):
                chunk = texts[idx: idx + self._embedding_batch_size]
                kwargs: dict[str, Any] = {"model": self._embedding_model, "input": chunk}
                if self._openai_supports_dimensions():
                    kwargs["dimensions"] = self.dim
                response = self._embedding_client.embeddings.create(**kwargs)
                data = getattr(response, "data", []) or []
                chunk_vectors = [list(getattr(item, "embedding", []) or []) for item in data]
                if len(chunk_vectors) != len(chunk) or any(not vec for vec in chunk_vectors):
                    raise RuntimeError("OpenAI embedding response size mismatch")
                if any(len(vec) != self.dim for vec in chunk_vectors):
                    raise RuntimeError("OpenAI embedding dimension mismatch")
                vectors.extend(chunk_vectors)
            return vectors
        except Exception:
            return _embed_texts(texts, dim=self.dim)

    def _embed_text(self, text: str) -> list[float]:
        vectors = self._embed_texts([text])
        if vectors:
            return vectors[0]
        return _embed_text(text, dim=self.dim)

    def upsert_documents(self, docs: list[dict[str, Any]]) -> None:
        ids = [d["id"] for d in docs]
        texts = [d["text"] for d in docs]
        metas = [d.get("metadata", {}) for d in docs]

        if self._simple is not None:
            self._simple.upsert(ids, texts, metas)
            return
        if ReplaceOne is None:
            self._simple = SimpleStore(
                self.persist_dir / "simple_store.json",
                dim=self.dim,
                embed_fn=self._embed_text,
            )
            self._simple.upsert(ids, texts, metas)
            return

        vectors = self._embed_texts(texts)
        ops = []
        for doc_id, text, meta, vec in zip(ids, texts, metas, vectors):
            ops.append(
                ReplaceOne(
                    {"_id": doc_id},
                    {"_id": doc_id, "text": text, "metadata": meta, "embedding": vec},
                    upsert=True,
                )
            )
        if ops:
            self._collection.bulk_write(ops, ordered=False)

    def _python_search(
        self,
        query_text: str,
        query_vec: list[float],
        filter_query: dict[str, Any],
        k: int,
    ) -> list[dict[str, Any]]:
        cursor = self._collection.find(filter_query, {"text": 1, "metadata": 1, "embedding": 1})
        scored = []
        for doc in cursor:
            text = doc.get("text", "")
            embedding = doc.get("embedding")
            if not embedding:
                embedding = self._embed_text(text)
            base_score = _cosine(query_vec, embedding)
            score = _blend_score(base_score, query_text, text)
            scored.append((score, doc))
        scored.sort(key=lambda item: item[0], reverse=True)
        results = []
        for score, doc in scored[:k]:
            results.append({
                "id": str(doc.get("_id")),
                "text": doc.get("text", ""),
                "metadata": doc.get("metadata", {}),
                "score": score,
            })
        return results

    def search(self, query_text: str, k: int = 5, where: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        if self._simple is not None:
            return self._simple.query(query_text, k=k, where=where)

        query_vec = self._embed_text(query_text)
        filter_query = _build_metadata_filter(where)

        if self.vector_index:
            stage: dict[str, Any] = {
                "index": self.vector_index,
                "queryVector": query_vec,
                "path": "embedding",
                "numCandidates": max(k * 10, 50),
                "limit": k,
            }
            if filter_query:
                stage["filter"] = filter_query
            pipeline = [
                {"$vectorSearch": stage},
                {
                    "$project": {
                        "text": 1,
                        "metadata": 1,
                        "score": {"$meta": "vectorSearchScore"},
                    }
                },
            ]
            try:
                docs = list(self._collection.aggregate(pipeline))
            except PyMongoError:
                return self._python_search(query_text, query_vec, filter_query, k)
            scored_docs: list[tuple[float, dict[str, Any]]] = []
            for doc in docs:
                base_score = float(doc.get("score") or 0.0)
                text = str(doc.get("text", ""))
                score = _blend_score(base_score, query_text, text)
                scored_docs.append((score, doc))
            scored_docs.sort(key=lambda item: item[0], reverse=True)
            results = []
            for score, doc in scored_docs[:k]:
                results.append({
                    "id": str(doc.get("_id")),
                    "text": doc.get("text", ""),
                    "metadata": doc.get("metadata", {}),
                    "score": score,
                })
            return results

        return self._python_search(query_text, query_vec, filter_query, k)

    def list_documents(
        self,
        where: dict[str, Any] | None = None,
        limit: int = 200,
        skip: int = 0,
    ) -> list[dict[str, Any]]:
        if self._simple is not None:
            items = []
            for doc_id, doc in self._simple.docs.items():
                meta = doc.get("meta", {})
                if where:
                    match = True
                    for key, value in where.items():
                        if meta.get(key) != value:
                            match = False
                            break
                    if not match:
                        continue
                items.append({
                    "id": doc_id,
                    "text": doc.get("text", ""),
                    "metadata": meta,
                })
            items.sort(key=lambda item: str(item.get("id")))
            return items[skip: skip + limit]

        if self._collection is None:
            return []

        filter_query = _build_metadata_filter(where)
        cursor = (
            self._collection
            .find(filter_query, {"text": 1, "metadata": 1})
            .sort("_id", 1)
            .skip(max(skip, 0))
            .limit(max(limit, 1))
        )
        results = []
        for doc in cursor:
            results.append({
                "id": str(doc.get("_id")),
                "text": doc.get("text", ""),
                "metadata": doc.get("metadata", {}),
            })
        return results
