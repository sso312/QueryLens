from __future__ import annotations
from typing import BinaryIO
import logging

from pypdf import PdfReader

logger = logging.getLogger(__name__)

def extract_text_from_pdf(file: BinaryIO) -> str:
    """
    Extracts text from a PDF file-like object using pypdf.
    """
    try:
        reader = PdfReader(file)
        text = ""
        for page in reader.pages:
            text += page.extract_text() or ""
        return text.strip()
    except Exception as exc:
        logger.error(f"Failed to extract text from PDF: {exc}")
        raise ValueError(f"Failed to process PDF file: {exc}")
