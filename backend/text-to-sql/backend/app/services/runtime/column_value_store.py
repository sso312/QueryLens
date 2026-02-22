from __future__ import annotations

from pathlib import Path
from typing import Any
from zipfile import ZipFile
import xml.etree.ElementTree as ET
import json
import re

from app.core.paths import project_path


_COLUMN_VALUE_JSONL_PATH = project_path("var/metadata/column_value_docs.jsonl")
_COLUMN_VALUE_XLSX_PATH = project_path("docs/데이터 탐색 항목_컬럼 값.xlsx")
_COLUMN_VALUE_CACHE_MTIME: float = -1.0
_COLUMN_VALUE_CACHE: list[dict[str, Any]] = []

_NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "pkgrel": "http://schemas.openxmlformats.org/package/2006/relationships",
}

_HEADER_ALIASES = {
    "table": {"테이블명", "table", "table_name"},
    "column": {"컬럼명", "column", "column_name"},
    "value": {"정보", "값", "value"},
    "description": {"설명", "description", "desc"},
}

_COLUMN_VALUE_STOPWORDS = {
    "the",
    "and",
    "or",
    "for",
    "with",
    "from",
    "to",
    "of",
    "in",
    "on",
    "by",
    "show",
    "list",
    "compare",
    "vs",
    "환자",
    "전체",
    "비교",
    "보여줘",
    "해줘",
}

_CATEGORY_INTENT_RE = re.compile(
    r"(admission\s*type|discharge\s*location|admission\s*location|insurance|race|ethnicity|language|status|category|enum|"
    r"유형|종류|구분|상태|범주|코드값|컬럼\s*값|열\s*값|입원\s*유형|퇴원\s*위치)",
    re.IGNORECASE,
)

_SERVICE_INTENT_RE = re.compile(
    r"(service|department|curr_service|prev_service|진료과|서비스|내과|외과|신경과|정형외과|이비인후과|산부인과|심장외과|흉부외과)",
    re.IGNORECASE,
)

_SERVICE_COLUMN_SET = {"CURR_SERVICE", "PREV_SERVICE"}
_KO_PARTICLE_SUFFIXES = (
    "에서",
    "으로",
    "에게",
    "까지",
    "부터",
    "은",
    "는",
    "이",
    "가",
    "을",
    "를",
    "와",
    "과",
    "의",
    "도",
    "만",
    "로",
    "에",
)


def _normalize(text: str) -> str:
    return re.sub(r"\s+", "", text.lower())


def _has_korean(text: str) -> bool:
    return bool(re.search(r"[가-힣]", text))


def _expand_token_variants(token: str) -> list[str]:
    text = str(token or "").strip()
    if not text:
        return []
    variants = [text]
    if not _has_korean(text):
        return variants
    for suffix in _KO_PARTICLE_SUFFIXES:
        if text.endswith(suffix) and len(text) - len(suffix) >= 2:
            base = text[: -len(suffix)]
            if base and base not in variants:
                variants.append(base)
    return variants


def _column_index(ref: str) -> int:
    letters = "".join(ch for ch in ref if ch.isalpha())
    value = 0
    for ch in letters:
        value = (value * 26) + (ord(ch.upper()) - 64)
    return max(0, value - 1)


def _read_shared_strings(zf: ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    shared: list[str] = []
    for si in root.findall("main:si", _NS):
        parts = [node.text or "" for node in si.findall(".//main:t", _NS)]
        shared.append("".join(parts))
    return shared


def _sheet_targets(zf: ZipFile) -> list[tuple[str, str]]:
    workbook = ET.fromstring(zf.read("xl/workbook.xml"))
    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rid_to_target: dict[str, str] = {}
    for rel in rels.findall("pkgrel:Relationship", _NS):
        rel_id = rel.attrib.get("Id")
        target = rel.attrib.get("Target") or ""
        if rel_id:
            rid_to_target[rel_id] = target

    results: list[tuple[str, str]] = []
    for sheet in workbook.findall("main:sheets/main:sheet", _NS):
        name = sheet.attrib.get("name") or ""
        rel_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id") or ""
        target = rid_to_target.get(rel_id, "")
        if target and not target.startswith("xl/"):
            target = f"xl/{target}"
        if name and target:
            results.append((name, target))
    return results


def _read_cell_text(cell: ET.Element, shared: list[str]) -> str:
    ctype = cell.attrib.get("t")
    value = cell.find("main:v", _NS)
    if ctype == "s" and value is not None and value.text is not None:
        try:
            return shared[int(value.text)]
        except Exception:
            return value.text or ""
    if ctype == "inlineStr":
        parts = [node.text or "" for node in cell.findall("main:is//main:t", _NS)]
        return "".join(parts)
    if value is not None and value.text is not None:
        return value.text
    return ""


def _rows_from_sheet(zf: ZipFile, target: str, shared: list[str]) -> list[list[str]]:
    if target not in zf.namelist():
        return []
    root = ET.fromstring(zf.read(target))
    rows: list[list[str]] = []
    for row in root.findall("main:sheetData/main:row", _NS):
        values_by_col: dict[int, str] = {}
        for cell in row.findall("main:c", _NS):
            ref = cell.attrib.get("r") or ""
            text = _read_cell_text(cell, shared).strip()
            if not ref or not text:
                continue
            values_by_col[_column_index(ref)] = text
        if not values_by_col:
            continue
        max_col = max(values_by_col)
        rows.append([values_by_col.get(idx, "") for idx in range(max_col + 1)])
    return rows


def _header_index_map(header: list[str]) -> dict[str, int]:
    normalized = [_normalize(item) for item in header]
    index_map: dict[str, int] = {}
    for key, aliases in _HEADER_ALIASES.items():
        for idx, name in enumerate(normalized):
            if name in {_normalize(alias) for alias in aliases}:
                index_map[key] = idx
                break
    return index_map


def _load_from_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(item, dict):
            continue
        table = str(item.get("table") or "").strip().upper()
        column = str(item.get("column") or "").strip().upper()
        value = str(item.get("value") or "").strip()
        description = str(item.get("description") or "").strip()
        sheet = str(item.get("sheet") or "").strip()
        if not table or not column or not value:
            continue
        rows.append({
            "sheet": sheet,
            "table": table,
            "column": column,
            "value": value,
            "description": description,
        })
    return rows


def _dedupe_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in items:
        key = (item["table"], item["column"], item["value"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def load_column_value_rows() -> list[dict[str, Any]]:
    global _COLUMN_VALUE_CACHE_MTIME
    global _COLUMN_VALUE_CACHE

    source_path: Path | None = None
    if _COLUMN_VALUE_JSONL_PATH.exists():
        source_path = _COLUMN_VALUE_JSONL_PATH
    elif _COLUMN_VALUE_XLSX_PATH.exists():
        source_path = _COLUMN_VALUE_XLSX_PATH

    if source_path is None:
        _COLUMN_VALUE_CACHE_MTIME = -1.0
        _COLUMN_VALUE_CACHE = []
        return []

    mtime = source_path.stat().st_mtime
    if _COLUMN_VALUE_CACHE and _COLUMN_VALUE_CACHE_MTIME == mtime:
        return _COLUMN_VALUE_CACHE

    if source_path.suffix.lower() == ".jsonl":
        deduped = _dedupe_rows(_load_from_jsonl(source_path))
        _COLUMN_VALUE_CACHE_MTIME = mtime
        _COLUMN_VALUE_CACHE = deduped
        return deduped

    parsed: list[dict[str, Any]] = []
    with ZipFile(_COLUMN_VALUE_XLSX_PATH) as zf:
        shared = _read_shared_strings(zf)
        for sheet_name, target in _sheet_targets(zf):
            rows = _rows_from_sheet(zf, target, shared)
            if not rows:
                continue
            header = rows[0]
            idx_map = _header_index_map(header)
            if not {"table", "column", "value"}.issubset(idx_map):
                continue
            for row in rows[1:]:
                table_idx = idx_map["table"]
                column_idx = idx_map["column"]
                value_idx = idx_map["value"]
                desc_idx = idx_map.get("description")
                table = row[table_idx].strip() if table_idx < len(row) else ""
                column = row[column_idx].strip() if column_idx < len(row) else ""
                value = row[value_idx].strip() if value_idx < len(row) else ""
                description = row[desc_idx].strip() if desc_idx is not None and desc_idx < len(row) else ""
                if not table or not column or not value:
                    continue
                parsed.append({
                    "sheet": sheet_name,
                    "table": table.upper(),
                    "column": column.upper(),
                    "value": value,
                    "description": description,
                })
    deduped = _dedupe_rows(parsed)

    _COLUMN_VALUE_CACHE_MTIME = mtime
    _COLUMN_VALUE_CACHE = deduped
    return deduped


def match_column_value_rows(question: str, rows: list[dict[str, Any]] | None = None, k: int = 8) -> list[dict[str, Any]]:
    normalized_question = _normalize(question)
    if not normalized_question:
        return []
    raw_tokens = re.split(r"[^0-9A-Za-z가-힣]+", question.lower())
    tokens = [
        token
        for token in (_normalize(item) for item in raw_tokens)
        if len(token) >= 2 and token not in _COLUMN_VALUE_STOPWORDS
    ]
    token_set = set(tokens)
    category_intent = bool(_CATEGORY_INTENT_RE.search(question))
    service_intent = bool(_SERVICE_INTENT_RE.search(question))
    source = rows if rows is not None else load_column_value_rows()

    matched: list[dict[str, Any]] = []
    for item in source:
        table = str(item.get("table") or "")
        column = str(item.get("column") or "")
        value = str(item.get("value") or "")
        description = str(item.get("description") or "")
        haystack = _normalize(" ".join([table, column, value, description]))
        if not haystack:
            continue

        score = 0
        table_col = _normalize(f"{table}.{column}")
        table_key = _normalize(table)
        table_col_match = bool(table_col and table_col in normalized_question)
        table_match = bool(table_key and table_key in normalized_question)
        column_key = _normalize(column)
        column_match = bool(column_key and column_key in normalized_question)

        if table_col_match:
            score += 28
        elif table_match and column_match:
            score += 20
        elif column_match:
            score += 10
        elif table_match:
            score += 4

        value_match = False
        value_key = _normalize(value)
        if len(value_key) >= 3 and value_key in normalized_question:
            score += 28
            value_match = True
        else:
            value_tokens = [
                token
                for token in (_normalize(item) for item in re.split(r"[^0-9A-Za-z가-힣]+", value.lower()))
                if len(token) >= 3 and token not in _COLUMN_VALUE_STOPWORDS
            ]
            if value_tokens and any(token in token_set for token in value_tokens):
                score += 14
                value_match = True

        desc_key = _normalize(description)
        desc_hits = 0
        for token in tokens:
            if len(token) < 2:
                continue
            is_ko = _has_korean(token)
            if len(token) < 3 and not is_ko:
                continue
            matched_value = False
            matched_desc = False
            for variant in _expand_token_variants(token):
                if value_key and variant in value_key:
                    matched_value = True
                    break
                if desc_key and variant in desc_key:
                    matched_desc = True
            if matched_value:
                score += 8 if is_ko else 6
                continue
            if matched_desc:
                desc_hits += 1
                score += 3 if is_ko else 2

        service_desc_match = bool(
            service_intent
            and table.upper() == "SERVICES"
            and column.upper() in _SERVICE_COLUMN_SET
            and desc_hits > 0
        )
        if service_desc_match:
            # Allow department-name mentions (e.g., 내과/외과) to resolve service codes
            # even when table/column names are not explicitly stated in the question.
            score += 10 + min(desc_hits, 2)
            value_match = True
            generic_dept_query = (
                ("내과" in normalized_question or "외과" in normalized_question or "medicine" in normalized_question or "surgery" in normalized_question)
                and not any(
                    token in normalized_question
                    for token in ("심장", "순환기", "종양", "신경", "흉부", "정형", "비뇨", "이비인후", "cardiac", "neuro", "onco", "thoracic", "ortho")
                )
            )
            if generic_dept_query and ("일반" in desc_key or "general" in desc_key):
                score += 6

        struct_match = table_col_match or (table_match and column_match) or (column_match and category_intent)
        if not (value_match or struct_match):
            continue
        min_score = 10 if value_match else 14
        if score < min_score:
            continue

        matched.append({
            **item,
            "_score": score,
            "_struct_match": struct_match,
            "_value_match": value_match,
            "_table_match": table_match,
            "_column_match": column_match,
            "_table_col_match": table_col_match,
            "_desc_hits": desc_hits,
        })

    matched.sort(
        key=lambda item: (
            -int(item.get("_score") or 0),
            item.get("table") or "",
            item.get("column") or "",
            item.get("value") or "",
        )
    )
    return matched[: max(1, k)]
