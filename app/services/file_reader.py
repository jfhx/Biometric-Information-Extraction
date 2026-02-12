import io
import logging
import os
from typing import List, Tuple

import pandas as pd
from docx import Document

from app.core.config import settings
from app.utils.text import join_blocks, normalize_whitespace, trim_text

logger = logging.getLogger(__name__)


class UnsupportedFileTypeError(ValueError):
    pass


def _read_docx(content: bytes) -> str:
    document = Document(io.BytesIO(content))
    paragraphs = [p.text.strip() for p in document.paragraphs if p.text.strip()]
    return join_blocks(paragraphs)


def _read_doc(content: bytes) -> str:
    try:
        import textract
    except ImportError as exc:
        raise UnsupportedFileTypeError(
            "读取 .doc 需要安装 textract：pip install textract"
        ) from exc
    import tempfile

    with tempfile.NamedTemporaryFile(delete=False, suffix=".doc") as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    try:
        text = textract.process(tmp_path, extension="doc")
        return text.decode("utf-8", errors="ignore")
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            logger.warning("Failed to remove temp doc file: %s", tmp_path)


def _read_pdf(content: bytes) -> str:
    try:
        import pdfplumber
    except ImportError as exc:
        raise UnsupportedFileTypeError(
            "读取 .pdf 需要安装 pdfplumber：pip install pdfplumber"
        ) from exc

    blocks: List[str] = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            page_text = page_text.strip()
            if page_text:
                blocks.append(page_text)
    return join_blocks(blocks)


def _read_csv(content: bytes) -> str:
    try:
        df = pd.read_csv(io.BytesIO(content), encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(io.BytesIO(content), encoding="gbk")
    df = df.head(settings.MAX_TABLE_ROWS)
    return df.to_csv(index=False)


def _read_xlsx(content: bytes) -> str:
    xls = pd.ExcelFile(io.BytesIO(content))
    blocks: List[str] = []
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        df = df.head(settings.MAX_TABLE_ROWS)
        blocks.append(f"[Sheet: {sheet_name}]")
        blocks.append(df.to_csv(index=False))
    return join_blocks(blocks)


def read_file_to_text(filename: str, content: bytes) -> Tuple[str, str]:
    ext = os.path.splitext(filename)[1].lower().strip(".")
    if ext in {"docx"}:
        text = _read_docx(content)
        file_type = "docx"
    elif ext in {"doc"}:
        text = _read_doc(content)
        file_type = "doc"
    elif ext in {"pdf"}:
        text = _read_pdf(content)
        file_type = "pdf"
    elif ext in {"csv"}:
        text = _read_csv(content)
        file_type = "csv"
    elif ext in {"xlsx", "xls"}:
        text = _read_xlsx(content)
        file_type = "xlsx"
    elif ext in {"txt"}:
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            text = content.decode("gbk", errors="ignore")
        file_type = "txt"
    else:
        raise UnsupportedFileTypeError(f"Unsupported file type: .{ext}")

    text = normalize_whitespace(text)
    text = trim_text(text, settings.MAX_TEXT_CHARS)
    return text, file_type
