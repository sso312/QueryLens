"""Structured logging helpers for query visualization."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict
from uuid import uuid4

try:
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
except Exception:  # pragma: no cover
    MongoClient = None  # type: ignore[assignment]

    class PyMongoError(Exception):
        pass


_LOGGER_NAME = "query_visualization"
_EVENT_COLLECTION = None
_EVENT_COLLECTION_READY = False
_EVENT_COLLECTION_FAILED = False
_EVENT_COLLECTION_NAME = "app_events"


def get_logger() -> logging.Logger:
    """Return a shared logger instance."""
    logger = logging.getLogger(_LOGGER_NAME)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def new_request_id() -> str:
    """Generate a request id to trace a single pipeline execution."""
    return f"qv-{uuid4().hex[:12]}"


def _get_event_collection():
    global _EVENT_COLLECTION, _EVENT_COLLECTION_READY, _EVENT_COLLECTION_FAILED
    if _EVENT_COLLECTION_READY:
        return _EVENT_COLLECTION
    if _EVENT_COLLECTION_FAILED:
        return None

    mongo_uri = str(os.getenv("MONGODB_URI", "")).strip()
    mongo_db = str(os.getenv("MONGODB_DB", "QueryLENs")).strip() or "QueryLENs"
    if not mongo_uri or MongoClient is None:
        _EVENT_COLLECTION_FAILED = True
        return None

    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
        db = client[mongo_db]
        collection = db[_EVENT_COLLECTION_NAME]
        collection.create_index([("ts", 1)])
        collection.create_index([("event", 1), ("ts", -1)])
        collection.create_index([("service", 1), ("ts", -1)])
        _EVENT_COLLECTION = collection
        _EVENT_COLLECTION_READY = True
        return _EVENT_COLLECTION
    except Exception:
        _EVENT_COLLECTION_FAILED = True
        return None


def _safe_mongo_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    # Convert non-BSON-compatible values (e.g. numpy scalars) to strings.
    return json.loads(json.dumps(data, ensure_ascii=False, default=str))


def log_event(
    event: str,
    payload: Dict[str, Any] | None = None,
    *,
    level: str = "info",
) -> None:
    """Write one structured log event in JSON format."""
    logger = get_logger()
    data: Dict[str, Any] = {
        "event": event,
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": "query-visualization",
        "level": level.lower(),
    }
    if payload:
        data.update(payload)

    message = json.dumps(data, ensure_ascii=False, default=str)
    writer = getattr(logger, level.lower(), logger.info)
    writer("%s", message)

    collection = _get_event_collection()
    if collection is not None:
        try:
            collection.insert_one(_safe_mongo_payload(data))
        except PyMongoError:
            pass
