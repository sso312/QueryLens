from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel
import json

router = APIRouter()

try:
    from reportlab.lib.pagesizes import letter  # type: ignore
    from reportlab.pdfgen import canvas  # type: ignore
except Exception:  # pragma: no cover
    canvas = None
    letter = None


class EvidenceRequest(BaseModel):
    title: str = "Evidence Report"
    content: dict = {}


@router.post("/evidence")
def evidence(req: EvidenceRequest):
    if canvas is None:
        raise HTTPException(status_code=501, detail="reportlab is not installed")

    import io

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter
    c.setFont("Helvetica", 12)
    c.drawString(40, height - 40, req.title)

    payload = json.dumps(req.content, ensure_ascii=True, indent=2)
    y = height - 70
    for line in payload.splitlines():
        if y < 40:
            c.showPage()
            c.setFont("Helvetica", 12)
            y = height - 40
        c.drawString(40, y, line[:120])
        y -= 14

    c.showPage()
    c.save()
    pdf = buffer.getvalue()
    return Response(content=pdf, media_type="application/pdf")
