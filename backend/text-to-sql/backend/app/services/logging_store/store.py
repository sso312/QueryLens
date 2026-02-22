from __future__ import annotations

from pathlib import Path
import json
import time
from typing import Any

try:
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
except Exception:  # pragma: no cover
    MongoClient = None  # type: ignore[assignment]

    class PyMongoError(Exception):
        pass

from app.core.config import get_settings


_EVENT_COLLECTION = None
_EVENT_COLLECTION_READY = False
_EVENT_COLLECTION_FAILED = False
_EVENT_COLLECTION_NAME = "app_events"


def _get_event_collection():
    global _EVENT_COLLECTION, _EVENT_COLLECTION_READY, _EVENT_COLLECTION_FAILED
    if _EVENT_COLLECTION_READY:
        return _EVENT_COLLECTION
    if _EVENT_COLLECTION_FAILED:
        return None

    settings = get_settings()
    if not settings.mongo_uri or MongoClient is None:
        _EVENT_COLLECTION_FAILED = True
        return None

    try:
        client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=2000)
        database = client[settings.mongo_db]
        collection = database[_EVENT_COLLECTION_NAME]
        collection.create_index([("ts", 1)])
        collection.create_index([("type", 1), ("ts", -1)])
        _EVENT_COLLECTION = collection
        _EVENT_COLLECTION_READY = True
        return _EVENT_COLLECTION
    except Exception:
        _EVENT_COLLECTION_FAILED = True
        return None


def append_event(path: str, payload: dict[str, Any]) -> None:
    payload = dict(payload or {})
    payload.setdefault("ts", int(time.time()))

    collection = _get_event_collection()
    if collection is not None:
        try:
            collection.insert_one(payload)
            return
        except PyMongoError:
            pass

    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=True) + "\n")


def read_events(path: str, limit: int | None = None) -> list[dict[str, Any]]:
    collection = _get_event_collection()
    if collection is not None:
        try:
            if limit is None:
                docs = list(collection.find({}).sort([("ts", 1), ("_id", 1)]))
            else:
                latest_docs = list(collection.find({}).sort([("ts", -1), ("_id", -1)]).limit(int(limit)))
                latest_docs.reverse()
                docs = latest_docs
            items: list[dict[str, Any]] = []
            for doc in docs:
                if not isinstance(doc, dict):
                    continue
                item = {k: v for k, v in doc.items() if k != "_id"}
                item["__mongo_id"] = str(doc.get("_id"))
                items.append(item)
            return items
        except PyMongoError:
            pass

    file_path = Path(path)
    if not file_path.exists():
        return []
    lines = file_path.read_text(encoding="utf-8").splitlines()
    if limit is not None:
        lines = lines[-limit:]
    items: list[dict[str, Any]] = []
    for line in lines:
        try:
            items.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return items


def write_events(path: str, events: list[dict[str, Any]]) -> None:
    collection = _get_event_collection()
    if collection is not None:
        try:
            docs: list[dict[str, Any]] = []
            for item in events:
                if not isinstance(item, dict):
                    continue
                doc = dict(item)
                doc.pop("__mongo_id", None)
                doc.pop("_id", None)
                docs.append(doc)
            collection.delete_many({})
            if docs:
                collection.insert_many(docs, ordered=True)
            return
        except PyMongoError:
            pass

    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as f:
        for item in events:
            if not isinstance(item, dict):
                continue
            f.write(json.dumps(item, ensure_ascii=True) + "\n")
