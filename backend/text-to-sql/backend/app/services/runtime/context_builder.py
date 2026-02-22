from __future__ import annotations

from typing import Any

from app.services.rag.retrieval import build_candidate_context, build_candidate_context_multi


def build_context_payload(question: str) -> dict[str, Any]:
    context = build_candidate_context(question)
    return {
        "schemas": context.schemas,
        "examples": context.examples,
        "templates": context.templates,
        "glossary": context.glossary,
    }


def build_context_payload_multi(questions: list[str]) -> dict[str, Any]:
    context = build_candidate_context_multi(questions)
    return {
        "schemas": context.schemas,
        "examples": context.examples,
        "templates": context.templates,
        "glossary": context.glossary,
    }
