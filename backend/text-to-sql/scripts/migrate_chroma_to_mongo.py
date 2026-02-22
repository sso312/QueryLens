from __future__ import annotations

from pathlib import Path
from typing import Any
import argparse
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "backend"))

try:
    import chromadb  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    chromadb = None

from app.core.config import get_settings
import hashlib
import math


def _list_collections(client) -> list[str]:
    collections = client.list_collections()
    names: list[str] = []
    for entry in collections:
        if hasattr(entry, "name"):
            names.append(entry.name)
        elif isinstance(entry, dict) and "name" in entry:
            names.append(str(entry["name"]))
        else:
            names.append(str(entry))
    return names


def _load_collection(client, name: str | None) -> tuple[Any | None, list[str]]:
    names = _list_collections(client)
    if name:
        return client.get_collection(name), names
    if len(names) == 1:
        return client.get_collection(names[0]), names
    return None, names


def _ensure_chroma() -> None:
    if chromadb is None:
        raise RuntimeError(
            "chromadb is required for migration. "
            "Install with: pip install chromadb==0.4.24"
        )


def _hash_token(token: str, dim: int) -> int:
    digest = hashlib.md5(token.encode("utf-8")).hexdigest()
    return int(digest, 16) % dim


def _embed_text(text: str, dim: int = 128) -> list[float]:
    vec = [0.0] * dim
    for tok in text.lower().split():
        idx = _hash_token(tok, dim)
        vec[idx] += 1.0
    norm = math.sqrt(sum(v * v for v in vec))
    if norm > 0:
        vec = [v / norm for v in vec]
    return vec


def _embed_texts(texts: list[str], dim: int = 128) -> list[list[float]]:
    return [_embed_text(t, dim=dim) for t in texts]


def _safe_text(value: str | None) -> str:
    if value is None:
        return ""
    return str(value)


def _safe_meta(value) -> dict:
    if isinstance(value, dict):
        return value
    return {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate Chroma DB data to MongoDB.")
    parser.add_argument("--chroma-path", default="var/chroma", help="Chroma persist dir")
    parser.add_argument("--chroma-collection", default=None, help="Chroma collection name")
    parser.add_argument("--mongo-uri", default=None, help="MongoDB URI")
    parser.add_argument("--mongo-db", default=None, help="MongoDB database name")
    parser.add_argument("--mongo-collection", default=None, help="MongoDB collection name")
    parser.add_argument("--embedding-dim", type=int, default=None, help="Embedding dimension")
    parser.add_argument("--batch-size", type=int, default=500, help="Batch size for migration")
    parser.add_argument("--drop", action="store_true", help="Drop target Mongo collection first")
    parser.add_argument("--dry-run", action="store_true", help="Scan only, no writes")
    parser.add_argument("--list-collections", action="store_true", help="List Chroma collections and exit")
    args = parser.parse_args()

    try:
        _ensure_chroma()
    except RuntimeError as exc:
        print(str(exc))
        return 1

    settings = get_settings()
    mongo_uri = args.mongo_uri or settings.mongo_uri
    mongo_db = args.mongo_db or settings.mongo_db
    mongo_collection = args.mongo_collection or settings.mongo_collection or "rag_docs"
    embedding_dim = args.embedding_dim or settings.rag_embedding_dim

    if not mongo_uri and not args.dry_run and not args.list_collections:
        print("MONGO_URI is required unless --dry-run is set.")
        return 1

    chroma_path = Path(args.chroma_path)
    if not chroma_path.exists():
        print(f"Chroma path not found: {chroma_path}")
        return 1

    client = chromadb.PersistentClient(path=str(chroma_path))
    if args.list_collections:
        names = _list_collections(client)
        print("Chroma collections:", ", ".join(names) if names else "(none)")
        return 0

    collection, names = _load_collection(client, args.chroma_collection)
    if collection is None:
        if not names:
            print("No Chroma collections found.")
        else:
            print("Multiple collections found. Specify --chroma-collection:")
            for name in names:
                print(f" - {name}")
        return 1

    total = collection.count()
    print(f"Chroma docs: {total}")
    if total == 0:
        return 0

    if not args.dry_run:
        try:
            from pymongo import MongoClient, ReplaceOne
        except Exception:
            print("pymongo is required for migration. Install with: pip install pymongo==4.6.3")
            return 1

        mongo = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
        mongo.admin.command("ping")
        target = mongo[mongo_db][mongo_collection]
        if args.drop:
            target.drop()
        target.create_index("metadata.type")
    else:
        target = None

    offset = 0
    batch_size = max(1, args.batch_size)

    while offset < total:
        try:
            batch = collection.get(
                limit=batch_size,
                offset=offset,
                include=["documents", "metadatas"],
            )
        except TypeError:
            if offset > 0:
                raise
            batch = collection.get(include=["documents", "metadatas"])
            offset = total
        ids = batch.get("ids") or []
        documents = batch.get("documents") or []
        metadatas = batch.get("metadatas") or []

        if not ids:
            break

        texts = []
        metas = []
        for idx, doc_id in enumerate(ids):
            text = _safe_text(documents[idx] if idx < len(documents) else "")
            meta = _safe_meta(metadatas[idx] if idx < len(metadatas) else {})
            texts.append(text)
            metas.append(meta)

        embeddings = _embed_texts(texts, dim=embedding_dim)

        if not args.dry_run:
            ops = []
            for doc_id, text, meta, emb in zip(ids, texts, metas, embeddings):
                if doc_id is None:
                    continue
                ops.append(
                    ReplaceOne(
                        {"_id": str(doc_id)},
                        {"_id": str(doc_id), "text": text, "metadata": meta, "embedding": emb},
                        upsert=True,
                    )
                )
            if ops:
                target.bulk_write(ops, ordered=False)

        offset += len(ids)
        print(f"Migrated {min(offset, total)}/{total}")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
