from __future__ import annotations

import re
from typing import Any

from .extract_budget import ExtractBudget


_BASE_KEYWORDS = (
    "methods",
    "study population",
    "population",
    "inclusion",
    "exclusion",
    "cohort",
    "eligibility",
    "definition",
    "outcome",
    "variable",
    "measurement",
    "icd",
    "diagnosis",
    "within",
    "first icu",
    "코호트",
    "선정",
    "제외",
    "측정",
)

_EXPANDED_KEYWORDS = (
    "appendix",
    "supplementary",
    "footnote",
    "table",
    "flowchart",
    "index event",
    "primary diagnosis",
    "secondary diagnosis",
    "last 24",
    "부록",
    "보충",
    "표",
)


_TABLE_HINTS = ("table", "tbl", "표")


def split_text_by_pages(full_text: str) -> list[dict[str, Any]]:
    text = str(full_text or "")
    marker_re = re.compile(r"===\s*PAGE\s+(\d+)\s*===\n?", re.IGNORECASE)
    matches = list(marker_re.finditer(text))
    if not matches:
        return [{"page": 1, "text": text, "global_start": 0}]

    chunks: list[dict[str, Any]] = []
    for idx, match in enumerate(matches):
        page_no = int(match.group(1)) if match.group(1).isdigit() else idx + 1
        content_start = match.end()
        content_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        chunk_text = text[content_start:content_end].strip()
        if not chunk_text:
            continue
        chunks.append({"page": page_no, "text": chunk_text, "global_start": content_start})
    return chunks


def _is_heading(line: str) -> bool:
    value = str(line or "").strip()
    if not value or len(value) > 90:
        return False
    lower = value.lower()
    return bool(
        re.search(
            r"^(methods?|methodology|study population|population|eligibility|inclusion|exclusion|"
            r"outcomes?|definitions?|appendix|supplementary|연구대상|선정 기준|제외 기준|코호트)",
            lower,
            flags=re.IGNORECASE,
        )
    )


def extract_snippets(
    *,
    full_text: str,
    budget: ExtractBudget,
    expanded_keywords: bool = True,
) -> list[dict[str, Any]]:
    keywords = list(_BASE_KEYWORDS)
    if expanded_keywords and budget.level in {"accurate", "strict"}:
        keywords.extend(_EXPANDED_KEYWORDS)

    snippets: list[dict[str, Any]] = []
    seen: set[tuple[int, str]] = set()

    for page_chunk in split_text_by_pages(full_text):
        page_no = int(page_chunk.get("page") or 0)
        page_text = str(page_chunk.get("text") or "")
        global_start = int(page_chunk.get("global_start") or 0)
        lines = page_text.splitlines()
        if not lines:
            continue

        offsets: list[int] = []
        running = 0
        for line in lines:
            offsets.append(running)
            running += len(line) + 1

        for idx, line in enumerate(lines):
            normalized = str(line or "").strip().lower()
            if not normalized:
                continue
            is_table_hint = any(token in normalized for token in _TABLE_HINTS)
            if not any(k in normalized for k in keywords) and not (budget.include_tables and is_table_hint):
                continue

            section = ""
            for back in range(idx, max(-1, idx - 8), -1):
                candidate = str(lines[back] or "").strip()
                if _is_heading(candidate):
                    section = candidate
                    break

            context = 7 if budget.level == "strict" else (5 if budget.level == "accurate" else 3)
            start = max(0, idx - context)
            end = min(len(lines), idx + context + 1)
            snippet_text = "\n".join(lines[start:end]).strip()
            if budget.include_tables and is_table_hint:
                start = max(0, idx - (context + 2))
                end = min(len(lines), idx + (context + 4) + 1)
                snippet_text = "\n".join(lines[start:end]).strip()

            if is_table_hint and len(snippet_text) > budget.table_capture_chars:
                snippet_text = snippet_text[: budget.table_capture_chars]
            if len(snippet_text) > budget.context_chars:
                snippet_text = snippet_text[: budget.context_chars]
            if not snippet_text:
                continue

            key = (page_no, snippet_text[:180])
            if key in seen:
                continue
            seen.add(key)

            span_start = global_start + offsets[start]
            span_end = global_start + offsets[min(end - 1, len(offsets) - 1)] + len(lines[min(end - 1, len(lines) - 1)])
            snippets.append(
                {
                    "page": page_no,
                    "section": section or "Unknown",
                    "text": snippet_text,
                    "span": [span_start, span_end],
                    "is_table_hint": bool(is_table_hint),
                }
            )
            if len(snippets) >= budget.max_snippets:
                return snippets
    return snippets
