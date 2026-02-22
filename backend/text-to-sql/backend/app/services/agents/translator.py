from __future__ import annotations

import re
from typing import Any

from app.core.config import get_settings
from app.services.agents.llm_client import LLMClient


_HANGUL_RE = re.compile(r"[\uac00-\ud7a3]")

TRANSLATE_SYSTEM_PROMPT = (
    "Translate Korean to concise English. Preserve medical terms, acronyms, "
    "table/column names, and code values as-is. "
    "Do not normalize or substitute categorical meanings. "
    "For explicit admission-type category semantics, preserve exact mapping: "
    "응급->EMERGENCY, 긴급->URGENT, 예약/선택 입원->ELECTIVE. "
    "Do not force this mapping when the source does not ask about admission type categories. "
    "If the source uses one category, never replace it with another. "
    "Return only the translation."
)


def contains_korean(text: str) -> bool:
    return bool(_HANGUL_RE.search(text))


def _replace_word(text: str, src: str, dst: str) -> str:
    return re.sub(rf"\b{re.escape(src)}\b", dst, text, flags=re.IGNORECASE)


def _enforce_admission_type_fidelity(source_ko: str, translated_en: str) -> str:
    source = str(source_ko or "")
    text = str(translated_en or "")
    if not source or not text:
        return text

    source_lower = source.lower()
    source_compact = re.sub(r"\s+", "", source_lower)

    has_emergency_ko = "응급" in source
    has_urgent_ko = "긴급" in source
    has_elective_ko = ("예약" in source) or ("선택입원" in source_compact)
    has_admission_type_phrase = ("입원유형" in source_compact) or ("admissiontype" in source_compact)
    has_admission_type_category = ("입원" in source) and (has_emergency_ko or has_urgent_ko or has_elective_ko)
    if not (has_admission_type_phrase or has_admission_type_category):
        return text

    # Preserve distinction between "응급(EMERGENCY)" and "긴급(URGENT)".
    if has_urgent_ko and not has_emergency_ko:
        text = _replace_word(text, "emergency", "urgent")
    elif has_emergency_ko and not has_urgent_ko:
        text = _replace_word(text, "urgent", "emergency")

    # Preserve elective admission semantics when source implies 예약/선택.
    if has_elective_ko:
        if not re.search(r"\belective\b", text, re.IGNORECASE):
            text = _replace_word(text, "scheduled", "elective")
            text = _replace_word(text, "optional", "elective")
            text = _replace_word(text, "selective", "elective")

    return text


def translate_to_english(text: str) -> tuple[str, dict[str, Any]]:
    settings = get_settings()
    client = LLMClient()
    messages = [
        {"role": "system", "content": TRANSLATE_SYSTEM_PROMPT},
        {"role": "user", "content": text},
    ]
    response = client.chat(
        messages=messages,
        model=settings.expert_model,
        max_tokens=min(settings.llm_max_output_tokens, 256),
    )
    translated = (response.get("content") or "").strip()
    if translated.startswith('"') and translated.endswith('"'):
        translated = translated[1:-1].strip()
    translated = _enforce_admission_type_fidelity(text, translated)
    return translated, response.get("usage", {})
