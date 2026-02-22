from __future__ import annotations

import argparse
import csv
import json
import mimetypes
import time
import uuid
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _load_pdf_page_count(path: Path) -> int:
    try:
        from pypdf import PdfReader  # type: ignore

        with path.open("rb") as f:
            return len(PdfReader(f).pages)
    except Exception:
        return 0


def _multipart_pdf(field_name: str, filename: str, content: bytes) -> tuple[bytes, str]:
    boundary = f"----CodexBoundary{uuid.uuid4().hex}"
    crlf = b"\r\n"

    content_type = mimetypes.guess_type(filename)[0] or "application/pdf"
    body = bytearray()
    body.extend(f"--{boundary}".encode("utf-8"))
    body.extend(crlf)
    body.extend(
        f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"'.encode("utf-8")
    )
    body.extend(crlf)
    body.extend(f"Content-Type: {content_type}".encode("utf-8"))
    body.extend(crlf)
    body.extend(crlf)
    body.extend(content)
    body.extend(crlf)
    body.extend(f"--{boundary}--".encode("utf-8"))
    body.extend(crlf)
    return bytes(body), f"multipart/form-data; boundary={boundary}"


def _http_json(
    url: str,
    *,
    method: str = "GET",
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 120.0,
) -> dict[str, Any]:
    req = urllib.request.Request(url, data=body, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=timeout) as res:
        payload = res.read().decode("utf-8")
    return json.loads(payload) if payload else {}


def _build_upload_url(
    base_url: str,
    *,
    relax_mode: bool,
    deterministic: bool,
    reuse_existing: bool,
    user: str,
) -> str:
    query = {
        "relax_mode": "true" if relax_mode else "false",
        "deterministic": "true" if deterministic else "false",
        "reuse_existing": "true" if reuse_existing else "false",
    }
    if user:
        query["user"] = user
    return f"{base_url.rstrip('/')}/pdf/upload?{urllib.parse.urlencode(query)}"


def _build_status_url(base_url: str, task_id: str, user: str) -> str:
    if user:
        return f"{base_url.rstrip('/')}/pdf/status/{task_id}?{urllib.parse.urlencode({'user': user})}"
    return f"{base_url.rstrip('/')}/pdf/status/{task_id}"


def _poll_task(
    base_url: str,
    task_id: str,
    *,
    poll_interval: float,
    timeout_sec: float,
    user: str,
) -> dict[str, Any]:
    status_url = _build_status_url(base_url, task_id, user)
    started = time.monotonic()
    while True:
        status = _http_json(status_url, timeout=60.0)
        state = str(status.get("status") or "")
        if state in {"completed", "failed"}:
            return status
        if time.monotonic() - started > timeout_sec:
            raise TimeoutError(f"Task timeout: {task_id}")
        time.sleep(poll_interval)


def _collect_file_list(paths: list[str], globs: list[str]) -> list[Path]:
    items: list[Path] = []
    for p in paths:
        path = Path(p).expanduser().resolve()
        if path.is_file():
            items.append(path)
    for pattern in globs:
        for path in sorted(Path(".").glob(pattern)):
            if path.is_file():
                items.append(path.resolve())
    dedup: dict[str, Path] = {}
    for item in items:
        dedup[str(item)] = item
    return list(dedup.values())


def _fmt(value: float | int | None, digits: int = 3) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return "-"


def _to_markdown(rows: list[dict[str, Any]]) -> str:
    headers = [
        "#",
        "filename",
        "pages",
        "analysis_duration_sec",
        "total_elapsed_sec",
        "queue_wait_sec",
        "sec_per_page",
        "pages_per_sec",
        "status",
    ]
    md = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for idx, row in enumerate(rows, start=1):
        md.append(
            "| "
            + " | ".join(
                [
                    str(idx),
                    str(row.get("filename") or ""),
                    str(row.get("pages") or 0),
                    _fmt(row.get("analysis_duration_sec")),
                    _fmt(row.get("total_elapsed_sec")),
                    _fmt(row.get("queue_wait_sec")),
                    _fmt(row.get("sec_per_page")),
                    _fmt(row.get("pages_per_sec")),
                    str(row.get("status") or ""),
                ]
            )
            + " |"
        )
    return "\n".join(md)


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark /pdf/upload -> /pdf/status latency and throughput.")
    parser.add_argument("paths", nargs="*", help="PDF file paths")
    parser.add_argument("--glob", action="append", default=[], help="Glob pattern for PDF files (can repeat)")
    parser.add_argument("--base-url", default="http://localhost:8002", help="API base URL")
    parser.add_argument("--poll-interval", type=float, default=2.0, help="Status polling interval seconds")
    parser.add_argument("--timeout-sec", type=float, default=1200.0, help="Per-file timeout seconds")
    parser.add_argument("--reuse-existing", action="store_true", help="Use cached result if available")
    parser.add_argument("--relax-mode", action="store_true", help="Enable relax_mode")
    parser.add_argument("--non-deterministic", action="store_true", help="Send deterministic=false")
    parser.add_argument("--user", default="", help="Optional user parameter")
    parser.add_argument("--csv-out", default="", help="Optional CSV output path")
    parser.add_argument("--md-out", default="", help="Optional markdown output path")
    args = parser.parse_args()

    files = _collect_file_list(args.paths, args.glob)
    if not files:
        print("No PDF files found. Pass file paths or --glob.")
        return 1

    rows: list[dict[str, Any]] = []
    deterministic = not args.non_deterministic
    for path in files:
        print(f"[RUN] {path.name}")
        pdf_bytes = path.read_bytes()
        page_count = _load_pdf_page_count(path)

        upload_url = _build_upload_url(
            args.base_url,
            relax_mode=args.relax_mode,
            deterministic=deterministic,
            reuse_existing=args.reuse_existing,
            user=args.user,
        )
        body, content_type = _multipart_pdf("file", path.name, pdf_bytes)
        try:
            upload_res = _http_json(
                upload_url,
                method="POST",
                body=body,
                headers={"Content-Type": content_type},
                timeout=300.0,
            )
            task_id = str(upload_res.get("task_id") or "").strip()
            if not task_id:
                raise RuntimeError(f"Upload succeeded but no task_id: {upload_res}")
            status = _poll_task(
                args.base_url,
                task_id,
                poll_interval=args.poll_interval,
                timeout_sec=args.timeout_sec,
                user=args.user,
            )
            result = status.get("result") if isinstance(status.get("result"), dict) else {}
            pages = int(result.get("pdf_page_count") or 0) if result else 0
            if pages <= 0:
                pages = page_count

            analysis_sec = _to_float(status.get("analysis_duration_sec"))
            if analysis_sec <= 0:
                analysis_sec = _to_float(status.get("analysis_duration_ms")) / 1000.0
            elapsed_sec = _to_float(status.get("total_elapsed_sec"))
            if elapsed_sec <= 0:
                elapsed_sec = _to_float(status.get("total_elapsed_ms")) / 1000.0
            queue_sec = _to_float(status.get("queue_wait_sec"))
            if queue_sec <= 0:
                queue_sec = _to_float(status.get("queue_wait_ms")) / 1000.0

            sec_per_page = (analysis_sec / pages) if pages > 0 and analysis_sec > 0 else None
            pages_per_sec = (pages / analysis_sec) if pages > 0 and analysis_sec > 0 else None

            row = {
                "filename": path.name,
                "pages": pages,
                "analysis_duration_sec": analysis_sec,
                "total_elapsed_sec": elapsed_sec,
                "queue_wait_sec": queue_sec,
                "sec_per_page": sec_per_page,
                "pages_per_sec": pages_per_sec,
                "status": str(status.get("status") or ""),
                "task_id": str(status.get("task_id") or task_id),
            }
            rows.append(row)
            print(
                f"[DONE] {path.name} | analysis={_fmt(analysis_sec)}s | elapsed={_fmt(elapsed_sec)}s | "
                f"queue={_fmt(queue_sec)}s | pages={pages}"
            )
        except Exception as exc:
            rows.append(
                {
                    "filename": path.name,
                    "pages": page_count,
                    "analysis_duration_sec": 0.0,
                    "total_elapsed_sec": 0.0,
                    "queue_wait_sec": 0.0,
                    "sec_per_page": None,
                    "pages_per_sec": None,
                    "status": f"error: {exc}",
                    "task_id": "",
                }
            )
            print(f"[FAIL] {path.name}: {exc}")

    markdown = _to_markdown(rows)
    print("\n" + markdown)

    if args.md_out:
        md_path = Path(args.md_out)
        md_path.parent.mkdir(parents=True, exist_ok=True)
        md_path.write_text(markdown + "\n", encoding="utf-8")
        print(f"Markdown written to {md_path}")

    if args.csv_out:
        csv_path = Path(args.csv_out)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "filename",
                    "pages",
                    "analysis_duration_sec",
                    "total_elapsed_sec",
                    "queue_wait_sec",
                    "sec_per_page",
                    "pages_per_sec",
                    "status",
                    "task_id",
                ],
            )
            writer.writeheader()
            writer.writerows(rows)
        print(f"CSV written to {csv_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

