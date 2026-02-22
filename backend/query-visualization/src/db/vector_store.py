from __future__ import annotations

import os
from typing import Iterable, List, Optional
from uuid import uuid4

from pymongo import MongoClient, ReplaceOne

from src.config.rag_config import (
    MONGODB_URI,
    MONGODB_DB,
    MONGODB_COLLECTION,
    MONGODB_VECTOR_INDEX,
    MONGODB_EMBED_FIELD,
)


def get_mongo_collection():
    if not MONGODB_URI:
        raise RuntimeError("MONGODB_URI is not set")
    # Fail fast when Atlas is unreachable so visualization can continue with fallback context.
    selection_timeout_ms = int(os.getenv("MONGODB_SERVER_SELECTION_TIMEOUT_MS", "2500"))
    connect_timeout_ms = int(os.getenv("MONGODB_CONNECT_TIMEOUT_MS", "2500"))
    socket_timeout_ms = int(os.getenv("MONGODB_SOCKET_TIMEOUT_MS", "2500"))
    client = MongoClient(
        MONGODB_URI,
        serverSelectionTimeoutMS=selection_timeout_ms,
        connectTimeoutMS=connect_timeout_ms,
        socketTimeoutMS=socket_timeout_ms,
    )
    db = client[MONGODB_DB]
    return db[MONGODB_COLLECTION]


def ensure_collection(_collection, *_args, **_kwargs) -> None:
    # Atlas Vector Search index is managed in Atlas UI.
    # Keep no-op here to preserve call sites.
    return None


def upsert_embeddings(
    collection,
    embeddings: List[List[float]],
    payloads: List[dict],
    ids: Optional[Iterable[str]] = None,
) -> None:
    point_ids = list(ids) if ids else [str(uuid4()) for _ in embeddings]
    ops = []
    for pid, vec, payload in zip(point_ids, embeddings, payloads):
        doc = {
            "_id": pid,
            MONGODB_EMBED_FIELD: vec,
            "text": payload.get("text"),
            "metadata": {k: v for k, v in payload.items() if k != "text"},
        }
        ops.append(ReplaceOne({"_id": pid}, doc, upsert=True))
    if ops:
        collection.bulk_write(ops, ordered=False)


def search_embeddings(
    collection,
    query_embedding: List[float],
    limit: int,
) -> list:
    pipeline = [
        {
            "$vectorSearch": {
                "index": MONGODB_VECTOR_INDEX,
                "path": MONGODB_EMBED_FIELD,
                "queryVector": query_embedding,
                "numCandidates": max(limit * 10, 100),
                "limit": limit,
            }
        },
        {
            "$project": {
                "_id": 1,
                "text": 1,
                "metadata": 1,
                "score": {"$meta": "vectorSearchScore"},
            }
        },
    ]
    return list(collection.aggregate(pipeline))
