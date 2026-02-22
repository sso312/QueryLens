from __future__ import annotations

"""
Backward-compatibility shim.

Chroma has been replaced by MongoDB for RAG storage. Keep this module so
older imports do not break.
"""

from app.services.rag.mongo_store import MongoStore as ChromaStore

__all__ = ["ChromaStore"]
