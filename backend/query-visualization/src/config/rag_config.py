from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load project-level .env regardless of current working directory.
_DOTENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_DOTENV_PATH)


def _env_bool(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

# Embedding
EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
EMBEDDING_DIM = int(os.getenv("OPENAI_EMBEDDING_DIM", os.getenv("RAG_EMBEDDING_DIM", "128")))

# MongoDB Atlas Vector Search
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB", "QueryLENs")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "sql-to-plot")
MONGODB_VECTOR_INDEX = os.getenv("MONGODB_VECTOR_INDEX", "rag_vector_index")
MONGODB_EMBED_FIELD = os.getenv("MONGODB_EMBED_FIELD", "embedding")

# RAG settings
RAG_TOP_K = int(os.getenv("RAG_TOP_K", "6"))
RAG_BATCH_SIZE = int(os.getenv("RAG_BATCH_SIZE", "64"))
RAG_DISTANCE = os.getenv("RAG_DISTANCE", "Cosine")
RAG_MIN_SCORE = float(os.getenv("RAG_MIN_SCORE", "0.2"))
RAG_CONTEXT_MAX_CHARS = int(os.getenv("RAG_CONTEXT_MAX_CHARS", "4000"))
RAG_DOC_VERSION = os.getenv("RAG_DOC_VERSION", "v1")
RAG_ENABLED = _env_bool("RAG_ENABLED", True)
