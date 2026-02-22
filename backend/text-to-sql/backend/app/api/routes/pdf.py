from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Query
from app.services.pdf_service import PDFCohortService
from app.services.logging_store.store import append_event
from app.core.config import get_settings
from app.services.runtime.state_store import get_state_store
from app.services.runtime.user_scope import normalize_user_id, scoped_state_key
import logging
import uuid
import time
from datetime import datetime
from typing import Optional, Any

router = APIRouter()
logger = logging.getLogger(__name__)

# 임시 작업 결과 저장소
# 메모리 기반 저장소이므로 서버 재시작 시 초기화됩니다. 
# 프로덕션 환경에서는 Redis나 DB를 사용하는 것이 좋습니다.
jobs = {}

PDF_CONFIRMED_COLLECTION = "pdf_confirmed_cohorts"
COHORT_LIBRARY_STATE_KEY = "cohort::library"

def _fmt_ts(ts: int) -> str:
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _sec(ms: int) -> float:
    return round(float(ms) / 1000.0, 3)


async def process_pdf_task(
    task_id: str,
    content: bytes,
    file_hash: str,
    filename: str,
    relax_mode: bool = False,
    deterministic: bool = True,
    reuse_existing: bool = True,
    accuracy_mode: Optional[bool] = None,
    user_id: str | None = None,
):
    """
    백그라운드에서 실행되는 PDF 처리 작업
    """
    submitted_at_ts = int(jobs.get(task_id, {}).get("submitted_at_ts") or time.time())
    started_at_ts = int(time.time())
    queue_wait_ms = max(0, int((started_at_ts - submitted_at_ts) * 1000))
    started_perf = time.perf_counter()

    try:
        logger.info(
            "Starting PDF processing for task %s (Relax: %s, Deterministic: %s, Reuse: %s, Accuracy: %s)",
            task_id,
            relax_mode,
            deterministic,
            reuse_existing,
            accuracy_mode,
        )
        jobs[task_id] = {
            "status": "processing",
            "message": "분석 중...",
            "task_id": task_id,
            "pdf_hash": file_hash,
            "filename": filename,
            "user_id": user_id,
            "accuracy_mode": accuracy_mode,
            "submitted_at_ts": submitted_at_ts,
            "submitted_at": _fmt_ts(submitted_at_ts),
            "started_at_ts": started_at_ts,
            "started_at": _fmt_ts(started_at_ts),
            "queue_wait_ms": queue_wait_ms,
            "queue_wait_sec": _sec(queue_wait_ms),
        }
        
        service = PDFCohortService()
        result = await service.analyze_and_generate_sql(
            content,
            filename=filename,
            relax_mode=relax_mode,
            deterministic=deterministic,
            reuse_existing=reuse_existing,
            accuracy_mode=accuracy_mode,
        )
        result_status = str(result.get("status") or "completed").strip().lower()
        job_status = "completed"
        message = "분석 완료"
        if result_status == "needs_user_input":
            job_status = "needs_user_input"
            message = "모호성 해결이 필요합니다"
        elif result_status == "validation_failed":
            job_status = "validation_failed"
            message = "검증 실패 (리포트 확인 필요)"
        elif result_status == "completed_with_ambiguities":
            job_status = "completed_with_ambiguities"
            message = "분석 완료 (모호성 포함)"

        completed_at_ts = int(time.time())
        analysis_duration_ms = max(0, int((time.perf_counter() - started_perf) * 1000))
        total_elapsed_ms = max(analysis_duration_ms, int((completed_at_ts - submitted_at_ts) * 1000))
        
        jobs[task_id] = {
            "status": job_status,
            "result": result,
            "message": message,
            "task_id": task_id,
            "pdf_hash": file_hash,
            "filename": filename,
            "user_id": user_id,
            "accuracy_mode": accuracy_mode,
            "submitted_at_ts": submitted_at_ts,
            "submitted_at": _fmt_ts(submitted_at_ts),
            "started_at_ts": started_at_ts,
            "started_at": _fmt_ts(started_at_ts),
            "completed_at_ts": completed_at_ts,
            "completed_at": _fmt_ts(completed_at_ts),
            "queue_wait_ms": queue_wait_ms,
            "queue_wait_sec": _sec(queue_wait_ms),
            "analysis_duration_ms": analysis_duration_ms,
            "analysis_duration_sec": _sec(analysis_duration_ms),
            "total_elapsed_ms": total_elapsed_ms,
            "total_elapsed_sec": _sec(total_elapsed_ms),
        }
        append_event(get_settings().events_log_path, {
            "type": "audit",
            "event": "pdf_analysis",
            "status": "success" if job_status in {"completed", "completed_with_ambiguities"} else job_status,
            "task_id": task_id,
            "pdf_hash": file_hash,
            "filename": filename,
            "duration_ms": analysis_duration_ms,
            "queue_wait_ms": queue_wait_ms,
            "total_elapsed_ms": total_elapsed_ms,
            "accuracy_mode": accuracy_mode,
            "rows_returned": int(((result.get("db_result") or {}).get("row_count") or 0)),
        })
        logger.info(f"Completed PDF processing for task {task_id}")
        
    except Exception as e:
        completed_at_ts = int(time.time())
        analysis_duration_ms = max(0, int((time.perf_counter() - started_perf) * 1000))
        total_elapsed_ms = max(analysis_duration_ms, int((completed_at_ts - submitted_at_ts) * 1000))
        logger.exception("Error processing PDF task %s", task_id)
        jobs[task_id] = {
            "status": "failed", 
            "error": str(e),
            "message": "분석 실패",
            "task_id": task_id,
            "pdf_hash": file_hash,
            "filename": filename,
            "user_id": user_id,
            "accuracy_mode": accuracy_mode,
            "submitted_at_ts": submitted_at_ts,
            "submitted_at": _fmt_ts(submitted_at_ts),
            "started_at_ts": started_at_ts,
            "started_at": _fmt_ts(started_at_ts),
            "completed_at_ts": completed_at_ts,
            "completed_at": _fmt_ts(completed_at_ts),
            "queue_wait_ms": queue_wait_ms,
            "queue_wait_sec": _sec(queue_wait_ms),
            "analysis_duration_ms": analysis_duration_ms,
            "analysis_duration_sec": _sec(analysis_duration_ms),
            "total_elapsed_ms": total_elapsed_ms,
            "total_elapsed_sec": _sec(total_elapsed_ms),
        }
        append_event(get_settings().events_log_path, {
            "type": "audit",
            "event": "pdf_analysis",
            "status": "error",
            "task_id": task_id,
            "pdf_hash": file_hash,
            "filename": filename,
            "duration_ms": analysis_duration_ms,
            "queue_wait_ms": queue_wait_ms,
            "total_elapsed_ms": total_elapsed_ms,
            "accuracy_mode": accuracy_mode,
            "error": str(e),
        })

@router.post("/upload")
async def upload_pdf(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    user: str | None = None,
    relax_mode: bool = False,
    deterministic: bool = True,
    reuse_existing: bool = True,
    accuracy_mode: Optional[bool] = None,
):
    filename = str(file.filename or "").strip()
    if not filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")
    
    try:
        content = await file.read()
        if not content:
             raise HTTPException(status_code=400, detail="Empty file")

        import hashlib
        file_hash = hashlib.sha256(content).hexdigest()
        task_id = str(uuid.uuid4())
        submitted_at_ts = int(time.time())
        resolved_user = str(user or "").strip() or None
        
        # 초기 상태 설정
        jobs[task_id] = {
            "status": "pending",
            "message": "대기 중...",
            "task_id": task_id,
            "pdf_hash": file_hash,
            "filename": filename or "uploaded.pdf",
            "user_id": resolved_user,
            "accuracy_mode": accuracy_mode,
            "file_size_bytes": len(content),
            "submitted_at_ts": submitted_at_ts,
            "submitted_at": _fmt_ts(submitted_at_ts),
        }
        
        # 백그라운드 작업 등록
        background_tasks.add_task(
            process_pdf_task,
            task_id,
            content,
            file_hash,
            filename or "uploaded.pdf",
            relax_mode,
            deterministic,
            reuse_existing,
            accuracy_mode,
            resolved_user,
        )
        
        return {
            "task_id": task_id,
            "status": "pending",
            "pdf_hash": file_hash,
            "filename": filename or "uploaded.pdf",
            "user_id": resolved_user,
            "accuracy_mode": accuracy_mode,
            "submitted_at": _fmt_ts(submitted_at_ts),
            "message": "PDF 분석이 시작되었습니다. 잠시 후 결과를 확인해주세요."
        }

    except Exception as e:
        logger.error(f"Error initializing PDF task: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/status/{task_id}")
async def get_task_status(task_id: str):
    """
    작업 상태 조회 엔드포인트
    """
    if task_id not in jobs:
        # 작업이 없으면 404 반환
        raise HTTPException(status_code=404, detail="Task not found")
    
    return jobs[task_id]


def _to_iso_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return f"{text}T00:00:00"
    return text


def _to_epoch(iso_text: str) -> float:
    if not iso_text:
        return 0.0
    text = iso_text.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text).timestamp()
    except Exception:
        return 0.0


def _status_to_history(raw_status: Any) -> str:
    status = str(raw_status or "").strip().lower()
    if status in {"pending", "processing", "running", "needs_user_input"}:
        return "RUNNING"
    if status in {"failed", "error", "validation_failed"}:
        return "ERROR"
    return "DONE"


def _extract_paper_meta(value: dict[str, Any]) -> dict[str, Any]:
    cohort_def = value.get("cohort_definition") if isinstance(value.get("cohort_definition"), dict) else {}
    methods_summary = cohort_def.get("methods_summary")
    structured_summary = methods_summary.get("structured_summary") if isinstance(methods_summary, dict) else {}
    file_name = str(value.get("filename") or value.get("file_name") or "").strip() or None
    paper_title = (
        str(value.get("paper_title") or value.get("title") or cohort_def.get("title") or "").strip() or None
    )
    authors = (
        str(value.get("authors") or structured_summary.get("authors") or structured_summary.get("author") or "").strip()
        or None
    )
    year_raw = value.get("year") if value.get("year") is not None else structured_summary.get("year")
    year: int | None = None
    try:
        year_value = int(year_raw)
        if 1900 <= year_value <= 2100:
            year = year_value
    except Exception:
        year = None
    journal = (
        str(value.get("journal") or structured_summary.get("journal") or structured_summary.get("source") or "").strip()
        or None
    )
    return {
        "fileName": file_name,
        "paperTitle": paper_title,
        "authors": authors,
        "year": year,
        "journal": journal,
    }


def _extract_criteria_lists(value: dict[str, Any]) -> tuple[list[str], list[str]]:
    cohort_def = value.get("cohort_definition") if isinstance(value.get("cohort_definition"), dict) else {}
    extraction_details = (
        cohort_def.get("extraction_details") if isinstance(cohort_def.get("extraction_details"), dict) else {}
    )
    cohort_criteria = (
        extraction_details.get("cohort_criteria") if isinstance(extraction_details.get("cohort_criteria"), dict) else {}
    )
    population = cohort_criteria.get("population")
    inclusion_list: list[str] = []
    exclusion_list: list[str] = []

    if isinstance(population, list):
        for idx, item in enumerate(population):
            if isinstance(item, str):
                text = item.strip()
                if not text:
                    continue
                lowered = text.lower()
                if "exclusion" in lowered or "제외" in lowered:
                    exclusion_list.append(text)
                else:
                    inclusion_list.append(text)
                continue
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or item.get("name") or f"조건 {idx + 1}").strip()
            definition = str(
                item.get("operational_definition")
                or item.get("operationalDefinition")
                or item.get("definition")
                or item.get("description")
                or item.get("criteria")
                or ""
            ).strip()
            if not definition:
                continue
            text = f"{title}: {definition}"
            lowered = title.lower()
            if "exclusion" in lowered or "제외" in title:
                exclusion_list.append(text)
            else:
                inclusion_list.append(text)
    raw_ie = cohort_def.get("inclusion_exclusion")
    if isinstance(raw_ie, list) and not inclusion_list and not exclusion_list:
        for idx, item in enumerate(raw_ie):
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or f"조건 {idx + 1}").strip()
            definition = str(
                item.get("operational_definition")
                or item.get("operationalDefinition")
                or item.get("definition")
                or item.get("description")
                or ""
            ).strip()
            if not definition:
                continue
            text = f"{title}: {definition}"
            lowered = title.lower()
            if "exclusion" in lowered or "제외" in title:
                exclusion_list.append(text)
            else:
                inclusion_list.append(text)
    return inclusion_list[:50], exclusion_list[:50]


def _criteria_summary(value: dict[str, Any], inclusion: list[str], exclusion: list[str]) -> str:
    cohort_def = value.get("cohort_definition") if isinstance(value.get("cohort_definition"), dict) else {}
    summary = str(
        cohort_def.get("criteria_summary_ko")
        or value.get("criteria_summary_ko")
        or value.get("criteria_summary")
        or ""
    ).strip()
    if summary:
        return summary
    merged = [*inclusion[:1], *exclusion[:1]]
    if merged:
        return " / ".join(merged)
    return ""


def _extract_mapping_rows(value: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def append_row(raw: Any, mapped_to: Any, confidence: Any) -> None:
        raw_text = str(raw or "").strip()
        mapped_text = str(mapped_to or "").strip()
        if not raw_text and not mapped_text:
            return
        key = (raw_text, mapped_text)
        if key in seen:
            return
        seen.add(key)
        conf_value: float | None = None
        try:
            conf_float = float(confidence)
            if 0 <= conf_float <= 1:
                conf_value = round(conf_float, 4)
        except Exception:
            conf_value = None
        rows.append(
            {
                "raw": raw_text or mapped_text,
                "mappedTo": mapped_text or None,
                "confidence": conf_value,
            }
        )

    cohort_def = value.get("cohort_definition") if isinstance(value.get("cohort_definition"), dict) else {}
    variables = cohort_def.get("variables")
    if isinstance(variables, list):
        for item in variables:
            if not isinstance(item, dict):
                continue
            append_row(
                item.get("raw")
                or item.get("name")
                or item.get("label")
                or item.get("alias")
                or item.get("description"),
                item.get("mapped_to") or item.get("mapping_id") or item.get("mappingId") or item.get("table"),
                item.get("confidence"),
            )

    mapped_variables = value.get("mapped_variables")
    if isinstance(mapped_variables, list):
        for item in mapped_variables:
            if not isinstance(item, dict):
                continue
            append_row(
                item.get("raw") or item.get("name") or item.get("source") or item.get("variable"),
                item.get("mapped_to") or item.get("target") or item.get("mapping"),
                item.get("confidence"),
            )

    features = value.get("features")
    if isinstance(features, list):
        for item in features:
            if not isinstance(item, dict):
                continue
            append_row(
                item.get("name") or item.get("label") or item.get("description"),
                item.get("table_name") or item.get("mapped_to"),
                item.get("confidence"),
            )

    return rows[:400]


def _extract_methods_text(value: dict[str, Any]) -> str:
    cohort_def = value.get("cohort_definition") if isinstance(value.get("cohort_definition"), dict) else {}
    methods_summary = cohort_def.get("methods_summary") if isinstance(cohort_def.get("methods_summary"), dict) else {}
    structured_summary = (
        methods_summary.get("structured_summary")
        if isinstance(methods_summary.get("structured_summary"), dict)
        else {}
    )
    if structured_summary:
        lines = []
        for key, raw_value in structured_summary.items():
            text = str(raw_value or "").strip()
            if not text:
                continue
            lines.append(f"{str(key).replace('_', ' ').strip()}: {text}")
        if lines:
            return "\n".join(lines[:20])
    text = str(methods_summary or value.get("methods_summary") or "").strip()
    return text


def _load_cohort_library_items(user_id: str | None) -> list[dict[str, Any]]:
    store = get_state_store()
    if not store.enabled:
        return []
    key = scoped_state_key(COHORT_LIBRARY_STATE_KEY, user_id)
    payload = store.get(key) or {}
    items = payload.get("cohorts", []) if isinstance(payload, dict) else []
    if items:
        return [item for item in items if isinstance(item, dict)]
    if user_id:
        fallback = store.get(COHORT_LIBRARY_STATE_KEY) or {}
        fallback_items = fallback.get("cohorts", []) if isinstance(fallback, dict) else []
        return [item for item in fallback_items if isinstance(item, dict)]
    return []


def _find_linked_cohort(value: dict[str, Any], library_items: list[dict[str, Any]]) -> dict[str, str] | None:
    linked = value.get("linked_cohort")
    if isinstance(linked, dict):
        cohort_id = str(linked.get("cohort_id") or linked.get("cohortId") or "").strip()
        cohort_name = str(linked.get("cohort_name") or linked.get("cohortName") or "").strip()
        if cohort_id:
            return {"cohortId": cohort_id, "cohortName": cohort_name or cohort_id}

    id_candidates = {
        str(value.get("task_id") or "").strip(),
        str(value.get("pdf_hash") or "").strip(),
        str(value.get("canonical_hash") or "").strip(),
    }
    id_candidates.discard("")
    meta = _extract_paper_meta(value)
    name_candidates = {
        str(meta.get("fileName") or "").strip(),
        str(meta.get("paperTitle") or "").strip(),
    }
    name_candidates.discard("")
    for item in library_items:
        source = item.get("source") if isinstance(item.get("source"), dict) else {}
        source_analysis_id = str(source.get("pdf_analysis_id") or source.get("pdfAnalysisId") or "").strip()
        source_pdf_name = str(source.get("pdf_name") or source.get("pdfName") or "").strip()
        if source_analysis_id and source_analysis_id in id_candidates:
            cohort_id = str(item.get("id") or "").strip()
            cohort_name = str(item.get("name") or "").strip()
            if cohort_id:
                return {"cohortId": cohort_id, "cohortName": cohort_name or cohort_id}
        if source_pdf_name and source_pdf_name in name_candidates:
            cohort_id = str(item.get("id") or "").strip()
            cohort_name = str(item.get("name") or "").strip()
            if cohort_id:
                return {"cohortId": cohort_id, "cohortName": cohort_name or cohort_id}
    return None


def _build_pdf_result_raw(value: dict[str, Any], history_id: str) -> dict[str, Any]:
    raw = dict(value)
    if not raw.get("filename"):
        meta = _extract_paper_meta(value)
        if meta.get("fileName"):
            raw["filename"] = meta["fileName"]
        elif meta.get("paperTitle"):
            raw["filename"] = meta["paperTitle"]
    if not raw.get("task_id"):
        raw["task_id"] = history_id
    if "summary_ko" not in raw:
        cohort_def = value.get("cohort_definition") if isinstance(value.get("cohort_definition"), dict) else {}
        summary = str(cohort_def.get("summary_ko") or "").strip()
        if summary:
            raw["summary_ko"] = summary
    return raw


def _history_item_from_value(
    history_id: str,
    value: dict[str, Any],
    library_items: list[dict[str, Any]],
) -> dict[str, Any]:
    paper_meta = _extract_paper_meta(value)
    inclusion, exclusion = _extract_criteria_lists(value)
    linked_cohort = _find_linked_cohort(value, library_items)
    status = _status_to_history(value.get("status"))
    updated_at = _to_iso_text(value.get("updated_at") or value.get("confirmed_at") or value.get("created_at"))
    created_at = _to_iso_text(value.get("confirmed_at") or value.get("created_at") or updated_at)
    generated_sql = value.get("generated_sql") if isinstance(value.get("generated_sql"), dict) else {}
    db_result = value.get("db_result") if isinstance(value.get("db_result"), dict) else {}
    mapping_rows = _extract_mapping_rows(value)
    return {
        "id": history_id,
        "createdAt": created_at or updated_at,
        "updatedAt": updated_at or created_at,
        "fileName": paper_meta.get("fileName"),
        "paperTitle": paper_meta.get("paperTitle"),
        "authors": paper_meta.get("authors"),
        "year": paper_meta.get("year"),
        "journal": paper_meta.get("journal"),
        "status": status,
        "errorMessage": str(db_result.get("error") or value.get("error") or "").strip() or None,
        "cohortSaved": linked_cohort is not None,
        "linkedCohortId": linked_cohort["cohortId"] if linked_cohort else None,
        "criteriaSummary": _criteria_summary(value, inclusion, exclusion) or None,
        "mappedVarsCount": len(mapping_rows),
        "sqlReady": bool(str(generated_sql.get("cohort_sql") or value.get("cohort_sql") or "").strip()),
        "_sortTs": _to_epoch(updated_at or created_at),
    }


def _history_item_from_job(task_id: str, job: dict[str, Any], library_items: list[dict[str, Any]]) -> dict[str, Any]:
    result = job.get("result") if isinstance(job.get("result"), dict) else {}
    base = dict(result)
    base.setdefault("filename", job.get("filename"))
    base.setdefault("task_id", task_id)
    base.setdefault("status", job.get("status"))
    base.setdefault("updated_at", _to_iso_text(job.get("completed_at") or job.get("started_at") or job.get("submitted_at")))
    base.setdefault("created_at", _to_iso_text(job.get("submitted_at")))
    item = _history_item_from_value(f"job:{task_id}", base, library_items)
    item["status"] = _status_to_history(job.get("status"))
    item["errorMessage"] = str(job.get("error") or "").strip() or item.get("errorMessage")
    item["updatedAt"] = _to_iso_text(job.get("completed_at") or job.get("started_at") or job.get("submitted_at")) or item.get("updatedAt")
    item["createdAt"] = _to_iso_text(job.get("submitted_at")) or item.get("createdAt")
    item["_sortTs"] = _to_epoch(str(item.get("updatedAt") or item.get("createdAt") or ""))
    return item


def _history_detail_from_value(
    history_id: str,
    value: dict[str, Any],
    library_items: list[dict[str, Any]],
) -> dict[str, Any]:
    paper_meta = _extract_paper_meta(value)
    inclusion, exclusion = _extract_criteria_lists(value)
    mapping_rows = _extract_mapping_rows(value)
    methods_text = _extract_methods_text(value)
    generated_sql = value.get("generated_sql") if isinstance(value.get("generated_sql"), dict) else {}
    db_result = value.get("db_result") if isinstance(value.get("db_result"), dict) else {}
    cohort_def = value.get("cohort_definition") if isinstance(value.get("cohort_definition"), dict) else {}
    linked_cohort = _find_linked_cohort(value, library_items)
    created_at = _to_iso_text(value.get("confirmed_at") or value.get("created_at") or value.get("updated_at"))
    updated_at = _to_iso_text(value.get("updated_at") or created_at)
    summary_text = str(
        cohort_def.get("summary_ko")
        or value.get("summary_ko")
        or value.get("llm_summary")
        or value.get("summary")
        or ""
    ).strip()
    notes_text = str(
        cohort_def.get("criteria_summary_ko")
        or value.get("criteria_summary_ko")
        or value.get("message")
        or value.get("error")
        or ""
    ).strip()
    row_count_raw = db_result.get("total_count")
    if row_count_raw is None:
        row_count_raw = db_result.get("row_count")
    row_count: int | None = None
    try:
        row_count = int(row_count_raw)
    except Exception:
        row_count = None
    return {
        "id": history_id,
        "createdAt": created_at or updated_at,
        "updatedAt": updated_at or created_at,
        "status": _status_to_history(value.get("status")),
        "paperMeta": paper_meta,
        "pdfExtract": {
            "methodsText": methods_text or None,
            "extractedCriteria": {
                "inclusion": inclusion,
                "exclusion": exclusion,
            },
        },
        "mapping": {
            "variables": mapping_rows,
        },
        "sql": {
            "generatedSql": str(generated_sql.get("cohort_sql") or value.get("cohort_sql") or "").strip() or None,
            "engine": str(value.get("engine") or "ORACLE").strip() or "ORACLE",
            "lastRun": {
                "ranAt": updated_at or created_at,
                "rowCount": row_count,
                "ok": not bool(str(db_result.get("error") or "").strip()),
                "error": str(db_result.get("error") or "").strip() or None,
            },
        },
        "llm": {
            "summary": summary_text or None,
            "notes": notes_text or None,
        },
        "linkedCohort": linked_cohort,
        "rawData": _build_pdf_result_raw(value, history_id),
    }


@router.get("/history")
async def get_pdf_history(
    user: str | None = None,
    query: str | None = None,
    status: str | None = None,
    from_date: str | None = Query(default=None, alias="from"),
    to_date: str | None = Query(default=None, alias="to"),
    cohort_saved: str | None = Query(default=None, alias="cohortSaved"),
    sort: str = "newest",
    page: int = 1,
    page_size: int = Query(default=10, alias="pageSize"),
):
    resolved_user = str(user or "").strip() or None
    normalized_user = normalize_user_id(resolved_user)
    safe_page = max(1, int(page or 1))
    safe_page_size = max(1, min(50, int(page_size or 10)))
    status_filter = str(status or "").strip().upper()
    if status_filter not in {"", "DONE", "RUNNING", "ERROR"}:
        status_filter = ""
    cohort_saved_filter = str(cohort_saved or "").strip().lower()
    query_text = str(query or "").strip().lower()
    from_epoch = _to_epoch(_to_iso_text(from_date))
    to_epoch = _to_epoch(_to_iso_text(to_date))
    if to_epoch > 0:
        to_epoch += 86399.0

    library_items = _load_cohort_library_items(resolved_user)
    items: list[dict[str, Any]] = []

    confirmed_store = get_state_store(PDF_CONFIRMED_COLLECTION)
    if confirmed_store.enabled and confirmed_store._collection is not None:
        projection = {"_id": 1, "value": 1}
        cursor = confirmed_store._collection.find({}, projection)
        for doc in cursor:
            if not isinstance(doc, dict):
                continue
            doc_id = str(doc.get("_id") or "").strip()
            value = doc.get("value")
            if not doc_id or not isinstance(value, dict):
                continue
            value_user = str(value.get("user_id") or "").strip()
            if resolved_user:
                if value_user:
                    if value_user != resolved_user:
                        continue
                elif normalized_user and not doc_id.endswith(f"::user::{normalized_user}"):
                    continue
            item = _history_item_from_value(doc_id, value, library_items)
            items.append(item)

    for task_id, job in jobs.items():
        if not isinstance(job, dict):
            continue
        if resolved_user:
            job_user = str(job.get("user_id") or "").strip()
            if job_user and job_user != resolved_user:
                continue
            if not job_user:
                continue
        item = _history_item_from_job(task_id, job, library_items)
        items.append(item)

    deduped: dict[str, dict[str, Any]] = {}
    for item in items:
        item_id = str(item.get("id") or "").strip()
        if not item_id:
            continue
        prev = deduped.get(item_id)
        if prev is None or float(item.get("_sortTs") or 0) >= float(prev.get("_sortTs") or 0):
            deduped[item_id] = item
    filtered_items = list(deduped.values())

    def is_match(item: dict[str, Any]) -> bool:
        item_status = str(item.get("status") or "").upper()
        if status_filter and item_status != status_filter:
            return False
        if cohort_saved_filter in {"saved", "true", "1", "yes"} and not bool(item.get("cohortSaved")):
            return False
        if cohort_saved_filter in {"unsaved", "false", "0", "no"} and bool(item.get("cohortSaved")):
            return False
        ts = float(item.get("_sortTs") or 0)
        if from_epoch > 0 and ts < from_epoch:
            return False
        if to_epoch > 0 and ts > to_epoch:
            return False
        if query_text:
            corpus = " ".join(
                [
                    str(item.get("fileName") or ""),
                    str(item.get("paperTitle") or ""),
                    str(item.get("authors") or ""),
                    str(item.get("journal") or ""),
                    str(item.get("criteriaSummary") or ""),
                ]
            ).lower()
            if query_text not in corpus:
                return False
        return True

    filtered_items = [item for item in filtered_items if is_match(item)]
    reverse = str(sort or "newest").strip().lower() != "oldest"
    filtered_items.sort(key=lambda item: float(item.get("_sortTs") or 0), reverse=reverse)

    total = len(filtered_items)
    start = (safe_page - 1) * safe_page_size
    end = start + safe_page_size
    paged = filtered_items[start:end]
    for item in paged:
        item.pop("_sortTs", None)

    return {
        "items": paged,
        "page": safe_page,
        "pageSize": safe_page_size,
        "total": total,
    }


@router.get("/history/{history_id}")
async def get_pdf_history_detail(history_id: str, user: str | None = None):
    resolved_user = str(user or "").strip() or None
    normalized_user = normalize_user_id(resolved_user)
    library_items = _load_cohort_library_items(resolved_user)
    item_id = str(history_id or "").strip()
    if not item_id:
        raise HTTPException(status_code=400, detail="history_id is required")

    if item_id.startswith("job:"):
        task_id = item_id.split("job:", 1)[1]
        job = jobs.get(task_id)
        if not isinstance(job, dict):
            raise HTTPException(status_code=404, detail="History item not found")
        if resolved_user:
            job_user = str(job.get("user_id") or "").strip()
            if not job_user or job_user != resolved_user:
                raise HTTPException(status_code=404, detail="History item not found")
        result = job.get("result") if isinstance(job.get("result"), dict) else {}
        value = dict(result)
        value.setdefault("filename", job.get("filename"))
        value.setdefault("task_id", task_id)
        value.setdefault("status", job.get("status"))
        value.setdefault("updated_at", _to_iso_text(job.get("completed_at") or job.get("started_at") or job.get("submitted_at")))
        value.setdefault("created_at", _to_iso_text(job.get("submitted_at")))
        value.setdefault("error", job.get("error"))
        return _history_detail_from_value(item_id, value, library_items)

    confirmed_store = get_state_store(PDF_CONFIRMED_COLLECTION)
    if not confirmed_store.enabled:
        raise HTTPException(status_code=500, detail="MongoDB is not configured")

    value = confirmed_store.get(item_id)
    if not isinstance(value, dict):
        if resolved_user:
            alt_key = scoped_state_key(item_id, resolved_user)
            value = confirmed_store.get(alt_key)
            item_id = alt_key if isinstance(value, dict) else item_id
        if not isinstance(value, dict) and resolved_user and len(item_id) == 64:
            alt_key_by_hash = scoped_state_key(item_id, resolved_user)
            value = confirmed_store.get(alt_key_by_hash)
            if isinstance(value, dict):
                item_id = alt_key_by_hash
        if not isinstance(value, dict):
            raise HTTPException(status_code=404, detail="History item not found")

    value_user = str(value.get("user_id") or "").strip()
    if resolved_user:
        if value_user:
            if value_user != resolved_user:
                raise HTTPException(status_code=404, detail="History item not found")
        elif normalized_user and not item_id.endswith(f"::user::{normalized_user}"):
            raise HTTPException(status_code=404, detail="History item not found")

    return _history_detail_from_value(item_id, value, library_items)
