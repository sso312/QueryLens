from __future__ import annotations

from dataclasses import dataclass
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


@dataclass
class AppStateStore:
    collection_name: str = "app_state"
    _client: Any | None = None
    _collection: Any | None = None

    def __post_init__(self) -> None:
        settings = get_settings()
        if not settings.mongo_uri or MongoClient is None:
            return
        self._client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=2000)
        database = self._client[settings.mongo_db]
        self._collection = database[self.collection_name]
        try:
            self._collection.create_index("_id", unique=True)
        except PyMongoError:
            pass

    @property
    def enabled(self) -> bool:
        return self._collection is not None

    def get(self, key: str) -> dict[str, Any] | None:
        if self._collection is None:
            return None
        try:
            doc = self._collection.find_one({"_id": key})
        except PyMongoError:
            return None
        if not doc:
            return None
        return doc.get("value")

    def set(self, key: str, value: dict[str, Any]) -> bool:
        if self._collection is None:
            return False
        try:
            self._collection.replace_one(
                {"_id": key},
                {"_id": key, "value": value, "updated_at": int(time.time())},
                upsert=True,
            )
            return True
        except PyMongoError:
            return False

    def delete(self, key: str) -> bool:
        if self._collection is None:
            return False
        try:
            self._collection.delete_one({"_id": key})
            return True
        except PyMongoError:
            return False

    def find_one(self, query: dict[str, Any]) -> dict[str, Any] | None:
        if self._collection is None:
            return None
        try:
            return self._collection.find_one(query)
        except PyMongoError:
            return None


_STORES: dict[str, AppStateStore] = {}


def get_state_store(collection_name: str = "app_state") -> AppStateStore:
    store = _STORES.get(collection_name)
    if store is None:
        store = AppStateStore(collection_name=collection_name)
        _STORES[collection_name] = store
    return store
