from __future__ import annotations

from typing import Any
import json

try:
    import tiktoken  # type: ignore
except Exception:  # pragma: no cover
    tiktoken = None

from app.core.paths import project_path


_SCHEMA_TABLE_COUNT_CACHE: int | None = None


def _active_table_scope_size() -> int:
    try:
        from app.services.runtime.settings_store import load_table_scope
    except Exception:
        return 0
    try:
        selected = {
            str(name).strip().lower()
            for name in load_table_scope()
            if str(name or "").strip()
        }
        return len(selected)
    except Exception:
        return 0


def _schema_table_count() -> int:
    global _SCHEMA_TABLE_COUNT_CACHE
    if _SCHEMA_TABLE_COUNT_CACHE is not None:
        return _SCHEMA_TABLE_COUNT_CACHE
    path = project_path("var/metadata/schema_catalog.json")
    if not path.exists():
        _SCHEMA_TABLE_COUNT_CACHE = 0
        return 0
    try:
        schema_catalog = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        _SCHEMA_TABLE_COUNT_CACHE = 0
        return 0
    tables = schema_catalog.get("tables", {}) if isinstance(schema_catalog, dict) else {}
    _SCHEMA_TABLE_COUNT_CACHE = len(tables) if isinstance(tables, dict) else 0
    return _SCHEMA_TABLE_COUNT_CACHE


def _count_tokens(text: str) -> int:
    if tiktoken is None:
        return max(1, len(text.split()))
    enc = tiktoken.get_encoding("cl100k_base")
    return len(enc.encode(text))


def _item_score(item: dict[str, Any]) -> float:
    value = item.get("score")
    try:
        return float(value) if value is not None else 0.0
    except Exception:
        return 0.0


def _rank_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not items:
        return []
    if all(item.get("score") is None for item in items):
        return list(items)
    return sorted(items, key=_item_score, reverse=True)


def _trim_items(
    items: list[dict[str, Any]],
    budget: int,
) -> tuple[list[dict[str, Any]], int, list[dict[str, Any]]]:
    kept: list[dict[str, Any]] = []
    remaining_items: list[dict[str, Any]] = []
    used = 0
    if budget <= 0:
        return kept, used, list(items)
    for item in items:
        text = item.get("text", "")
        cost = _count_tokens(text)
        if used + cost > budget:
            remaining_items.append(item)
            continue
        kept.append(item)
        used += cost
    return kept, used, remaining_items


def trim_context_to_budget(context: Any, budget: int) -> Any:
    if budget <= 0:
        return context.__class__(schemas=[], examples=[], templates=[], glossary=[])

    remaining = budget
    schemas = _rank_items(list(getattr(context, "schemas", [])))
    examples = _rank_items(list(getattr(context, "examples", [])))
    templates = _rank_items(list(getattr(context, "templates", [])))
    glossary = _rank_items(list(getattr(context, "glossary", [])))

    scope_size = _active_table_scope_size()
    total_schema_tables = _schema_table_count()
    broad_scope = (
        scope_size > 0
        and total_schema_tables > 0
        and (scope_size / float(total_schema_tables)) >= 0.80
    )
    if broad_scope:
        # If scope is effectively "all tables", allocate less to schema to reduce context bias.
        quotas = {
            "schemas": int(budget * 0.50),
            "examples": int(budget * 0.28),
            "glossary": int(budget * 0.14),
        }
    elif scope_size > 0:
        schema_ratio = 0.62 if scope_size >= 8 else 0.58
        quotas = {
            "schemas": int(budget * schema_ratio),
            "examples": int(budget * 0.20),
            "glossary": int(budget * 0.10),
        }
    else:
        quotas = {
            "schemas": int(budget * 0.55),
            "examples": int(budget * 0.25),
            "glossary": int(budget * 0.12),
        }
    quotas["templates"] = max(0, budget - quotas["schemas"] - quotas["examples"] - quotas["glossary"])

    items = {
        "schemas": schemas,
        "examples": examples,
        "glossary": glossary,
        "templates": templates,
    }
    kept: dict[str, list[dict[str, Any]]] = {key: [] for key in items}

    # Pass 1: Reserve capacity for critical context first.
    for key in ("schemas", "examples", "glossary", "templates"):
        if remaining <= 0:
            break
        part_budget = min(remaining, quotas.get(key, 0))
        if part_budget <= 0:
            continue
        chunk, used, leftover = _trim_items(items[key], part_budget)
        kept[key].extend(chunk)
        items[key] = leftover
        remaining -= used

    # Pass 2: Fill leftovers by priority.
    for key in ("schemas", "examples", "glossary", "templates"):
        if remaining <= 0:
            break
        chunk, used, leftover = _trim_items(items[key], remaining)
        kept[key].extend(chunk)
        items[key] = leftover
        remaining -= used

    return context.__class__(
        schemas=kept["schemas"],
        examples=kept["examples"],
        templates=kept["templates"],
        glossary=kept["glossary"],
    )
